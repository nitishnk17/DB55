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

    // --- Day 3 test: read first block of customer table via DiskManager ---
    let customer_start = disk_manager.get_file_start_block("customer")?;
    let customer_num_blocks = disk_manager.get_file_num_blocks("customer")?;
    println!(
        "Customer table: start_block={}, num_blocks={}",
        customer_start, customer_num_blocks
    );

    let block_data = disk_manager.read_blocks(customer_start, 1)?;
    println!(
        "First few bytes of customer block: {:?}",
        String::from_utf8_lossy(&block_data[..100])
    );

    // Day 4: Decode real rows from the 'customer' table
    let customer_spec = ctx.get_table_specs().iter()
        .find(|s| s.name == "customer")
        .context("Customer table spec not found")?;

    // Get row count from the block footer (last 2 bytes)
    let b_size = disk_manager.block_size as usize;
    let count_bytes: [u8; 2] = block_data[b_size-2..b_size].try_into()?;
    let row_count = u16::from_le_bytes(count_bytes);

    eprintln!("--- ROW DECODING CHECK (Customer Table) ---");
    eprintln!("Block contains {} rows.", row_count);

    let mut cursor = 0;
    for i in 0..row_count {
        let (decoded_row, bytes_used) = row::Row::decode(&block_data[cursor..], &customer_spec.column_specs)?;
        eprintln!("Row {}: {:?}", i, decoded_row.values);
        cursor += bytes_used;
    }

    // Get memory limit from monitor
    input_line.clear();
    monitor_out.write_all("get_memory_limit\n".as_bytes())?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    println!("Memory limit is set to {} MB", memory_limit_mb);

    // Send result of query to monitor for validation
    /*
    monitor_out.write_all("validate\n".as_bytes())?;
    monitor_out.write_all("1|hello|DBMS|\n".as_bytes())?;
    monitor_out.write_all("!\n".as_bytes())?;
    monitor_out.flush()?;
    */

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
