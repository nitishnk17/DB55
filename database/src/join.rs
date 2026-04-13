use std::io::{Read, Write};
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::disk_run::{rows_to_blocks, Run, RunReader};
use crate::operator::Operator;
use crate::row::Row;

pub struct JoinOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_run: Run,
    left_chunk: Vec<Row>,
    right_reader: Option<RunReader>,
    current_left_idx: usize,
    left_col_idx: usize,
    right_col_idx: usize,
    right_types: Vec<DataType>,
    max_chunk_rows: usize,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> JoinOp<R, W> {
    pub fn new(
        left: Box<dyn Operator<R, W>>,
        mut right: Box<dyn Operator<R, W>>,
        left_col_idx: usize,
        right_col_idx: usize,
        right_types: Vec<DataType>,
        buffer_pool: &mut BufferPool<R, W>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        // 1. Materialize right child sequentially into a Run by flushing in chunks to stay well within 64MB memory limit
        let block_size = buffer_pool.block_size();
        let chunk_size = std::cmp::max((100 * block_size) / 256, 100);
        let mut block_ids = Vec::new();
        let mut total_right_rows = 0;

        loop {
            let mut right_chunk = Vec::new();
            for _ in 0..chunk_size {
                if let Some(row) = right.next(buffer_pool) {
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
            let start_block = buffer_pool.allocate_anon_blocks(num_blocks);
            for (i, block_data) in blocks.iter().enumerate() {
                let bid = start_block + i as u64;
                buffer_pool.write_block(bid, block_data);
                block_ids.push(bid);
            }
        }

        let right_run = Run {
            block_ids,
            num_rows: total_right_rows,
        };

        let outer_memory_budget_blocks = 100;
        let max_chunk_rows = std::cmp::max((outer_memory_budget_blocks * block_size) / 256, 100);

        JoinOp {
            left,
            right_run,
            left_chunk: Vec::new(),
            right_reader: None,
            current_left_idx: 0,
            left_col_idx,
            right_col_idx,
            right_types,
            max_chunk_rows,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for JoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            // If left chunk is empty, populate it and reset right reader
            if self.left_chunk.is_empty() {
                for _ in 0..self.max_chunk_rows {
                    if let Some(row) = self.left.next(pool) {
                        self.left_chunk.push(row);
                    } else {
                        break;
                    }
                }
                if self.left_chunk.is_empty() {
                    return None; // Completely exhausted
                }
                self.right_reader = Some(RunReader::new(&self.right_run, self.right_types.clone(), pool));
                self.current_left_idx = 0;
            }

            // At this point right_reader is guaranteed to be Some
            let reader = self.right_reader.as_mut().unwrap();

            while let Some(right_row) = reader.peek() {
                // Try matching remaining rows in left_chunk with the current right_row
                while self.current_left_idx < self.left_chunk.len() {
                    let left_row = &self.left_chunk[self.current_left_idx];
                    self.current_left_idx += 1;

                    let left_val = &left_row.values[self.left_col_idx];
                    let right_val = &right_row.values[self.right_col_idx];

                    if left_val == right_val {
                        let mut combined = left_row.values.clone();
                        combined.extend(right_row.values.clone());
                        return Some(Row { values: combined });
                    }
                }
                // Exhausted left chunk for this right row, advance right row
                self.current_left_idx = 0;
                reader.advance(pool);
            }

            // Exhausted right run for the current left chunk. Clear chunk to read next one.
            self.left_chunk.clear();
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
