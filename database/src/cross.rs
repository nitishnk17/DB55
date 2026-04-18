use crate::operator::Operator;
use crate::row::Row;
use crate::disk_run::{rows_to_blocks, Run, RunReader};

use common::DataType;
use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;

pub struct CrossOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_run: Run,
    current_left_row: Option<Row>,
    right_reader: Option<RunReader>,
    right_types: Vec<DataType>,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> CrossOp<R, W> {
    pub fn new(mut left: Box<dyn Operator<R, W>>, mut right: Box<dyn Operator<R, W>>, pool: &mut BufferPool<R, W>) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());
        let right_types = right.data_types();

        // Materialize right child sequentially into a Run (out-of-core)
        let block_size = pool.block_size();
        let chunk_size = std::cmp::max((100 * block_size) / 256, 100);
        let mut block_ids = Vec::new();
        let mut total_right_rows = 0;

        loop {
            let mut right_chunk = Vec::new();
            for _ in 0..chunk_size {
                if let Some(row) = right.next(pool) {
                    right_chunk.push(row);
                } else {
                    break;
                }
            }
            if right_chunk.is_empty() {
                break;
            }
            total_right_rows += right_chunk.len();
            let blocks = rows_to_blocks(&right_chunk, block_size);
            let num_blocks = blocks.len() as u64;
            let start_block = pool.allocate_anon_blocks(num_blocks);
            for (i, block_data) in blocks.iter().enumerate() {
                let bid = start_block + i as u64;
                pool.write_block(bid, block_data);
                block_ids.push(bid);
            }
        }

        let right_run = Run {
            block_ids,
            num_rows: total_right_rows,
        };

        let current_left_row = left.next(pool);

        CrossOp {
            left,
            right_run,
            current_left_row,
            right_reader: None,
            right_types,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for CrossOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            let left_row = match &self.current_left_row {
                Some(row) => row,
                None => {
                    if !self.right_run.block_ids.is_empty() {
                        pool.free_run(&self.right_run);
                        self.right_run.block_ids.clear();
                    }
                    return None;
                }
            };

            if self.right_reader.is_none() {
                self.right_reader = Some(RunReader::new(&self.right_run, self.right_types.clone(), pool));
            }

            let reader = self.right_reader.as_mut().unwrap();

            if let Some(right_row) = reader.peek() {
                let mut combined = left_row.values.clone();
                combined.extend(right_row.values.clone());
                reader.advance(pool);
                return Some(Row { values: combined });
            }

            // Right run exhausted for this left row, advance left
            self.current_left_row = self.left.next(pool);
            self.right_reader = None;
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
