use std::io::{Read, Write};
use db_config::table::ColumnSpec;
use crate::buffer_pool::BufferPool;
use crate::row::{Row, decode_block};
use crate::operator::Operator;

pub struct TableScanner {
    column_names: Vec<String>,
    all_rows: Vec<Row>,
    current_index: usize,
}

impl TableScanner {
    pub fn new(
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        file_id: &str,
        column_specs: Vec<ColumnSpec>,
    ) -> Self {
        // 1. Query metadata through buffer pool
        let start_block = buffer_pool.get_file_start_block(file_id);
        let num_blocks = buffer_pool.get_file_num_blocks(file_id);
        let _block_size = buffer_pool.block_size();

        // 2. Fetch each block through the buffer pool and decode rows
        let mut all_rows = Vec::new();
        for i in 0..num_blocks {
            let block_data = buffer_pool.fetch_block(start_block + i);
            let rows = decode_block(&block_data, &column_specs);
            all_rows.extend(rows);
            // Unpin after decoding — we've copied the rows out
            buffer_pool.unpin(start_block + i);
        }

        // 3. Extract column names for schema()
        let column_names = column_specs
            .iter()
            .map(|c| c.column_name.clone())
            .collect();

        TableScanner {
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
