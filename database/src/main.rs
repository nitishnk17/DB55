use anyhow::{Context, Result};
use clap::Parser;
use common::query::{Query, QueryOp};
use db_config::DbContext;
use std::io::{BufRead, BufReader, BufWriter, Write};

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};

mod buffer_pool;
mod cli;
mod cross;
mod disk_manager;
mod disk_run;
mod filter;
mod hash_join;
mod io_setup;
mod join;
mod operator;
mod project;
mod query_executor;
mod row;
mod sort;
mod sort_merge;
mod table_scanner;

#[derive(Default, Clone, Copy)]
struct QueryProfile {
    has_sort: bool,
    has_cross: bool,
}

fn profile_query(op: &QueryOp) -> QueryProfile {
    match op {
        QueryOp::Sort(data) => {
            let mut p = profile_query(&data.underlying);
            p.has_sort = true;
            p
        }
        QueryOp::Cross(data) => {
            let left = profile_query(&data.left);
            let right = profile_query(&data.right);
            QueryProfile {
                has_sort: left.has_sort || right.has_sort,
                has_cross: true,
            }
        }
        QueryOp::Filter(data) => profile_query(&data.underlying),
        QueryOp::Project(data) => profile_query(&data.underlying),
        QueryOp::Scan(_) => QueryProfile::default(),
    }
}

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Load database schema / statistics context
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    // Setup I/O handlers for disk and monitor
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, monitor_out) = setup_monitor_io();

    // Initialize DiskManager (queries block size automatically)
    let disk_manager = disk_manager::DiskManager::new(disk_in, disk_out)?;
    let block_size = disk_manager.block_size as usize;

    // Use buffered I/O for the monitor protocol. Result streaming can dominate
    // CPU time on output-heavy queries if every row becomes its own system call.
    let mut monitor_buf_reader = BufReader::new(monitor_in);
    let mut monitor_out = BufWriter::with_capacity(256 * 1024, monitor_out);
    let mut input_line = String::new();

    // ── Step 1: Read the query JSON from monitor ──────────────────────────
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(input_line.trim())
        .with_context(|| format!("Failed to parse query JSON: {}", input_line))?;

    // ── Step 2: Request memory limit from monitor ─────────────────────────
    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: usize = input_line
        .trim()
        .parse()
        .with_context(|| format!("Failed to parse memory limit: {}", input_line))?;
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;
    // ── Step 3: Partition memory with RLIMIT_AS-safe adaptive budgeting ─────
    //
    // The pool cache machinery (frames/page_table/lru_list) is no longer
    // backed by per-frame allocations (see `BufferPool::new`), so the
    // pool_pct line is now purely a *sizing knob* for batch heuristics in
    // table_scanner / join / cross — its actual RAM cost is negligible.
    // That headroom now flows into hash-join and sort budgets.
    let query_profile = profile_query(&query.root);
    let (pool_pct, sort_pct, overhead_mb, hash_cap_mb, hash_min_mb) = if memory_limit_mb <= 64 {
        if query_profile.has_cross && query_profile.has_sort {
            (14usize, 32usize, 14usize, 16usize, 4usize)
        } else if query_profile.has_cross {
            (14usize, 12usize, 14usize, 24usize, 5usize)
        } else if query_profile.has_sort {
            (14usize, 42usize, 14usize, 10usize, 3usize)
        } else {
            (16usize, 12usize, 14usize, 18usize, 4usize)
        }
    } else if query_profile.has_cross && query_profile.has_sort {
        (18usize, 36usize, 12usize, 40usize, 6usize)
    } else if query_profile.has_cross {
        (20usize, 16usize, 12usize, 56usize, 8usize)
    } else if query_profile.has_sort {
        (18usize, 46usize, 12usize, 20usize, 4usize)
    } else {
        (22usize, 14usize, 12usize, 40usize, 6usize)
    };

    let min_frames = 64usize;
    let max_frames = 4096usize;
    let target_pool_bytes = (memory_limit_bytes * pool_pct / 100).max(block_size * min_frames);
    let num_frames = (target_pool_bytes / block_size).clamp(min_frames, max_frames);
    let actual_pool_bytes = num_frames * block_size;

    let sort_memory_bytes = (memory_limit_bytes * sort_pct / 100).max(block_size * min_frames);

    // Reserve large fixed overhead for runtime + shared libs + stack.
    // Then cap hash-join budget aggressively to avoid allocator spikes.
    let overhead = overhead_mb * 1024 * 1024usize;
    let remaining = memory_limit_bytes
        .saturating_sub(actual_pool_bytes)
        .saturating_sub(sort_memory_bytes)
        .saturating_sub(overhead);
    let hash_join_budget = remaining
        .min(hash_cap_mb * 1024 * 1024)
        .max(hash_min_mb * 1024 * 1024);

    let mut buffer_pool = buffer_pool::BufferPool::new(num_frames, disk_manager);

    // ── Step 4: Build the operator tree and run it ───────────────────────
    let mut root_op = query_executor::build_operator(
        &query.root,
        &ctx,
        &mut buffer_pool,
        sort_memory_bytes,
        hash_join_budget,
    );

    // ── Step 5: Stream results back to monitor ────────────────────────────
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    while let Some(row) = root_op.next(&mut buffer_pool) {
        row.write_to(&mut monitor_out)?;
        monitor_out.write_all(b"\n")?;
    }

    // Signal end of results
    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
