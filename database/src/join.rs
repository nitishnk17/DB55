use std::io::{Read, Write};
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::operator::Operator;
use crate::row::Row;
use crate::disk_run::{rows_to_blocks, Run, RunReader};

pub struct JoinOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_run: Run,
    left_chunk: Vec<Row>,
    current_left_idx: usize,
    right_reader: Option<RunReader>,
    left_col_idx: usize,
    right_col_idx: usize,
    right_types: Vec<DataType>,
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

        // Materialize right child sequentially into a Run (out-of-core)
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

        JoinOp {
            left,
            right_run,
            left_chunk: Vec::new(),
            current_left_idx: 0,
            right_reader: None,
            left_col_idx,
            right_col_idx,
            right_types,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for JoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            // Populate left chunk if it is empty
            if self.left_chunk.is_empty() {
                let mut chunk = Vec::new();
                for _ in 0..5000 {
                    if let Some(row) = self.left.next(pool) {
                        chunk.push(row);
                    } else {
                        break;
                    }
                }
                
                if chunk.is_empty() {
                    // Left operator is fully exhausted
                    if !self.right_run.block_ids.is_empty() {
                        pool.free_run(&self.right_run);
                        self.right_run.block_ids.clear();
                    }
                    return None; // Join is complete
                }

                self.left_chunk = chunk;
                self.current_left_idx = 0;
                self.right_reader = Some(RunReader::new(&self.right_run, self.right_types.clone(), pool));
            }

            // Read from right run and match against left chunk
            let reader = self.right_reader.as_mut().unwrap();

            if let Some(right_row) = reader.peek() {
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
            } else {
                // Right run reader exhausted, which means this left chunk has been fully processed against all right rows.
                self.left_chunk.clear();
                self.right_reader = None;
            }
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
