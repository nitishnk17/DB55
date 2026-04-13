use std::io::{Read, Write};
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::disk_run::{rows_to_blocks, Run, RunReader};
use crate::operator::Operator;
use crate::row::Row;

pub struct JoinOp<R: Read, W: Write> {
    joined_rows: Vec<Row>,
    current_index: usize,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    _marker: std::marker::PhantomData<(R, W)>,
}

impl<R: Read, W: Write> JoinOp<R, W> {
    pub fn new(
        mut left: Box<dyn Operator<R, W>>,
        mut right: Box<dyn Operator<R, W>>,
        left_col_idx: usize,
        right_col_idx: usize,
        right_types: Vec<DataType>,
        buffer_pool: &mut BufferPool<R, W>,
    ) -> Self {
        // Output schema
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        // 1. Materialize the right child entirely into anonymous blocks
        let mut right_rows = Vec::new();
        while let Some(row) = right.next(buffer_pool) {
            right_rows.push(row);
        }

        let block_size = buffer_pool.block_size();
        let blocks = rows_to_blocks(&right_rows, block_size);
        let num_blocks = blocks.len() as u64;
        let start_block = buffer_pool.allocate_anon_blocks(num_blocks);
        for (i, block_data) in blocks.iter().enumerate() {
            buffer_pool.write_block(start_block + i as u64, block_data);
        }

        let right_run = Run {
            start_block,
            num_blocks,
            num_rows: right_rows.len(),
        };

        // 2. Perform Block Nested Loop Join (BNLJ)
        let outer_memory_budget_blocks = 100;
        let max_chunk_rows = std::cmp::max((outer_memory_budget_blocks * block_size) / 256, 100);

        let mut joined_rows = Vec::new();

        loop {
            // Read `max_chunk_rows` from outer (left)
            let mut left_chunk = Vec::new();
            for _ in 0..max_chunk_rows {
                if let Some(row) = left.next(buffer_pool) {
                    left_chunk.push(row);
                } else {
                    break;
                }
            }

            if left_chunk.is_empty() {
                break; // No more outer rows
            }

            // Stream inner (right_run) and check against all rows in left_chunk
            let mut right_reader = RunReader::new(&right_run, right_types.clone(), buffer_pool);

            loop {
                if let Some(right_row) = right_reader.peek() {
                    // Check this right_row against all left rows in chunk
                    for left_row in &left_chunk {
                        let left_val = &left_row.values[left_col_idx];
                        let right_val = &right_row.values[right_col_idx];

                        if left_val == right_val {
                            let mut combined_values = left_row.values.clone();
                            combined_values.extend(right_row.values.clone());
                            joined_rows.push(Row {
                                values: combined_values,
                            });
                        }
                    }
                } else {
                    break; // Exhausted inner run
                }
                right_reader.advance(buffer_pool);
            }
        }

        JoinOp {
            joined_rows,
            current_index: 0,
            output_schema,
            output_types,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for JoinOp<R, W> {
    fn next(&mut self, _pool: &mut BufferPool<R, W>) -> Option<Row> {
        if self.current_index < self.joined_rows.len() {
            let row = self.joined_rows[self.current_index].clone();
            self.current_index += 1;
            Some(row)
        } else {
            None
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
