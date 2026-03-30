use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Read, Write};

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};

mod cli;
mod io_setup;
mod disk_manager;
mod row;
mod operator;
mod table_scanner;
mod query_executor;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Use the ctx to access the tables and stats
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;
    for table_spec in ctx.get_table_specs() {
        println!("Table: {}", table_spec.name);
        println!("File id: {}", table_spec.file_id);
        for column_spec in &table_spec.column_specs {
            println!(
                "\tColumn: {} ({:?})",
                column_spec.column_name, column_spec.data_type
            );
        }
        println!();
    }

    // Setup I/O handlers for disk and monitor
    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Initialize DiskManager (queries block size automatically)
    let mut disk_manager = disk_manager::DiskManager::new(disk_in, disk_out)?;
    println!("block size is {}", disk_manager.block_size);

    // Use buffered reader for monitor
    let mut monitor_buf_reader = BufReader::new(monitor_in);
    let mut input_line = String::new();

    // Read query from monitor
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line).unwrap();
    println!("Input query is: {:#?}", query);

    // Get memory limit from monitor
    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    println!("Memory limit is set to {} MB", memory_limit_mb);

    // Build operator tree from query AST
    use crate::operator::Operator;
    let mut root_op = query_executor::build_operator(&query.root, &ctx, &mut disk_manager);

    // Send results to monitor for validation
    monitor_out.write_all(b"validate\n")?;
    while let Some(row) = root_op.next() {
        monitor_out.write_all(format!("{}\n", row).as_bytes())?;
    }
    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
