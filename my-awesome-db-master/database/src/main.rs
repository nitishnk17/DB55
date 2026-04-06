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
    // Redirect stderr to /dev/null so that RLIMIT_FSIZE=0 (set by monitor)
    // doesn't kill this process via SIGXFSZ when debug prints go to a file.
    /*unsafe {
        let dev_null = libc::open(b"/dev/null\0".as_ptr() as *const libc::c_char, libc::O_WRONLY);
        if dev_null >= 0 {
            libc::dup2(dev_null, 2);
            libc::close(dev_null);
        }
    }*/

    let cli_options = CliOptions::parse();

    // Load database schema / statistics context
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    for table_spec in ctx.get_table_specs() {
        eprintln!("Table: {}", table_spec.name);
        eprintln!("File id: {}", table_spec.file_id);
        for column_spec in &table_spec.column_specs {
            eprintln!(
                "\tColumn: {} ({:?})",
                column_spec.column_name, column_spec.data_type
            );
        }
        eprintln!();
    }

    // Setup I/O handlers for disk and monitor
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Initialize DiskManager (queries block size automatically)
    let disk_manager = disk_manager::DiskManager::new(disk_in, disk_out)?;
    let block_size = disk_manager.block_size as usize;
    eprintln!("Block size: {} bytes", block_size);

    // Use buffered reader for monitor input
    let mut monitor_buf_reader = BufReader::new(monitor_in);
    let mut input_line = String::new();

    // ── Step 1: Read the query JSON from monitor ──────────────────────────
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(input_line.trim())
        .with_context(|| format!("Failed to parse query JSON: {}", input_line))?;
    eprintln!("Query received: {:#?}", query);

    // ── Step 2: Request memory limit from monitor ─────────────────────────
    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: usize = input_line.trim().parse()
        .with_context(|| format!("Failed to parse memory limit: {}", input_line))?;
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;
    eprintln!("Memory limit: {} MB ({} bytes)", memory_limit_mb, memory_limit_bytes);

    // ── Step 3: Size the buffer pool ─────────────────────────────────────
    //
    // RLIMIT_AS on Linux limits total virtual address space to memory_limit_bytes.
    // A Rust binary + glibc + libstd + stack already occupies ~25–35 MB of virtual
    // address space before we allocate any heap.  We must therefore NOT use all of
    // memory_limit_bytes for the buffer pool.
    //
    // Strategy: use at most 25 % of the limit for buffer frames, but clamp to a
    // sensible range so we always have at least 256 frames and never more than
    // 8 192 frames (32 MB at 4 KB/frame).
    //
    // The remaining ~75 % is available for:
    //   • sort / join working vectors  (we budget 50 % of the limit)
    //   • code + shared libraries + stack  (OS-level virtual overhead ~20-30 MB)
    let pool_bytes  = (memory_limit_bytes / 4).max(1 * 1024 * 1024); // at least 1 MB
    let num_frames  = (pool_bytes / block_size).clamp(256, 8192);

    // Sort operators get 50 % of total limit as their in-memory row budget.
    // This translates to a byte budget that estimate_memory_budget() converts to
    // a maximum row count.
    let sort_memory_bytes: usize = memory_limit_bytes / 2;

    eprintln!(
        "Buffer pool: {} frames × {} bytes = {} MB",
        num_frames,
        block_size,
        (num_frames * block_size) / (1024 * 1024)
    );
    eprintln!("Sort memory budget: {} MB", sort_memory_bytes / (1024 * 1024));

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

    while let Some(row) = root_op.next() {
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
