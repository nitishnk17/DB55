use std::io::{Read, Write};
use common::DataType;
use db_config::table::ColumnSpec;
use crate::buffer_pool::BufferPool;
use crate::row::{Row, decode_block};
use crate::operator::Operator;
use std::collections::VecDeque;

pub struct TableScanner {
    column_names: Vec<String>,
    column_types: Vec<DataType>,
    file_id: String,
    start_block: u64,
    num_blocks: u64,
    current_block: u64,
    batch_rows: VecDeque<Row>,
}

impl TableScanner {
    pub fn new(
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        file_id: &str,
        column_specs: &[ColumnSpec],
    ) -> Self {
        let start_block = buffer_pool.get_file_start_block(file_id);
        let num_blocks  = buffer_pool.get_file_num_blocks(file_id);

        let column_names = column_specs
            .iter()
            .map(|c| c.column_name.clone())
            .collect();
        let column_types = column_specs
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        TableScanner {
            column_names,
            column_types,
            file_id: file_id.to_string(),
            start_block,
            num_blocks,
            current_block: start_block,
            batch_rows: VecDeque::new(),
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for TableScanner {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            if let Some(r) = self.batch_rows.pop_front() {
                return Some(r);
            }

            if self.current_block >= self.start_block + self.num_blocks {
                return None;
            }

            // Adapt dynamically limit based on BufferPool frames but fallback reasonably
            // Using a max of either 20% of buffer pool or 256
            let available_frames = (pool.num_frames() / 5).max(32) as u64;
            let dynamic_batch_size = available_frames.clamp(32, 256);

            let count = dynamic_batch_size.min(self.start_block + self.num_blocks - self.current_block);
            let raw = pool.read_blocks_sequential(self.current_block, count);
            let block_size = pool.block_size();

            for i in 0..count as usize {
                let begin = i * block_size;
                let end   = begin + block_size;
                let block_data = &raw[begin..end];
                let rows = decode_block(block_data, &self.column_types);
                for r in rows {
                    self.batch_rows.push_back(r);
                }
            }
            self.current_block += count;
        }
    }

    fn schema(&self) -> Vec<String> {
        self.column_names.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.column_types.clone()
    }
}
