use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

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
mod table_scanner;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Load database schema / statistics context
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    // Setup I/O handlers for disk and monitor
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Initialize DiskManager (queries block size automatically)
    let disk_manager = disk_manager::DiskManager::new(disk_in, disk_out)?;
    let block_size = disk_manager.block_size as usize;

    // Use buffered reader for monitor input
    let mut monitor_buf_reader = BufReader::new(monitor_in);
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
    let memory_limit_mb: usize = input_line.trim().parse()
        .with_context(|| format!("Failed to parse memory limit: {}", input_line))?;
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;
    // ── Step 3: Size the buffer pool conservatively ─────────────────────
    //
    // The assignment harness runs the database under a strict RLIMIT_AS budget on
    // Linux.  Using "most" of that budget for our own heap leaves too little room
    // for the Rust runtime, shared libraries, stacks, and temporary operator state.
    //
    // We therefore keep the buffer pool and sort working set intentionally modest:
    //   • ~10% of the limit for buffer frames
    //   • ~30% of the limit for sort run generation
    //   • the remaining ~60% left untouched as headroom for the process itself and
    //     other transient allocations during joins / sorting.
    let min_frames = 64usize;
    let max_frames = 2048usize;
    let target_pool_bytes = (memory_limit_bytes / 10).max(block_size * min_frames);
    let num_frames = (target_pool_bytes / block_size).clamp(min_frames, max_frames);

    // Keep sort buffers conservative as well so external sort kicks in earlier
    // instead of risking RLIMIT_AS failures on large queries.
    let sort_memory_bytes = (memory_limit_bytes * 3 / 10).max(block_size * min_frames);

    let mut buffer_pool = buffer_pool::BufferPool::new(num_frames, disk_manager);

    // ── Step 4: Build the operator tree and run it ───────────────────────
    let mut root_op = query_executor::build_operator(
        &query.root,
        &ctx,
        &mut buffer_pool,
        sort_memory_bytes,
    );

    // ── Step 5: Stream results back to monitor ────────────────────────────
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    while let Some(row) = root_op.next(&mut buffer_pool) {
        monitor_out.write_all(format!("{}\n", row).as_bytes())?;
    }

    // Signal end of results
    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
