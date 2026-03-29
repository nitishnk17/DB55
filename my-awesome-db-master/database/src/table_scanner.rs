use std::io::{Read, Write};
use db_config::table::ColumnSpec;
use crate::disk_manager::DiskManager;
use crate::row::{Row, decode_block};
use crate::operator::Operator;

pub struct TableScanner {
    column_specs: Vec<ColumnSpec>,
    column_names: Vec<String>,
    all_rows: Vec<Row>,
    current_index: usize,
}

impl TableScanner {
    pub fn new(
        disk_manager: &mut DiskManager<impl Read, impl Write>,
        file_id: &str,
        column_specs: Vec<ColumnSpec>,
    ) -> Self {
        // 1. Query disk for start block and number of blocks
        let start_block = disk_manager.get_file_start_block(file_id).unwrap();
        let num_blocks = disk_manager.get_file_num_blocks(file_id).unwrap();

        // 2. Read ALL blocks at once
        let all_block_data = disk_manager.read_blocks(start_block, num_blocks).unwrap();
        let block_size = disk_manager.block_size as usize;

        // 3. Decode each block and collect all rows
        let mut all_rows = Vec::new();
        for i in 0..num_blocks as usize {
            let block_start = i * block_size;
            let block_end = block_start + block_size;
            let block_slice = &all_block_data[block_start..block_end];
            let rows = decode_block(block_slice, &column_specs);
            all_rows.extend(rows);
        }

        // 4. Extract column names for schema()
        let column_names = column_specs.iter()
            .map(|c| c.column_name.clone())
            .collect();

        TableScanner {
            column_specs,
            column_names,
            all_rows,
            current_index: 0,
        }
    }
}

impl Operator for TableScanner {
    fn next(&mut self) -> Option<Row> {
        if self.current_index < self.all_rows.len() {
            let row = self.all_rows[self.current_index].clone();
            self.current_index += 1;
            Some(row)
        } else {
            None
        }
    }

    fn schema(&self) -> Vec<String> {
        self.column_names.clone()
    }
}
