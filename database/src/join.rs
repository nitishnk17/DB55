use crate::buffer_pool::BufferPool;
use crate::disk_run::{Run, RunReader, rows_to_run_buffer};
use crate::operator::Operator;
use crate::row::Row;
use common::Data;
use common::DataType;
use std::io::{Read, Write};

pub struct JoinOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_run: Run,
    right_rows_mem: Vec<Row>,
    in_memory_mode: bool,
    current_left_row: Option<Row>,
    current_mem_right_idx: usize,
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
    #[inline]
    fn combine_rows(left_vals: &[Data], right_vals: &[Data]) -> Vec<Data> {
        let mut values = Vec::with_capacity(left_vals.len() + right_vals.len());
        values.extend(left_vals.iter().cloned());
        values.extend(right_vals.iter().cloned());
        values
    }

    fn estimate_row_size(types: &[DataType]) -> usize {
        let mut size = 24 + types.len() * 32;
        for dt in types {
            size += match dt {
                DataType::Int32 => 4,
                DataType::Int64 => 8,
                DataType::Float32 => 4,
                DataType::Float64 => 8,
                DataType::String => 74,
            };
        }
        size
    }

    pub fn new(
        mut left: Box<dyn Operator<R, W>>,
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
        let memory_budget_bytes =
            (buffer_pool.num_frames() * block_size).clamp(4 * 1024 * 1024, 16 * 1024 * 1024);
        let right_row_size = Self::estimate_row_size(&right_types);
        let mut right_rows_mem = Vec::with_capacity(1024);
        let mut bytes_used = 0usize;
        let mut in_memory_mode = true;

        while let Some(row) = right.next(buffer_pool) {
            if bytes_used + right_row_size > memory_budget_bytes {
                in_memory_mode = false;
                break;
            }
            bytes_used += right_row_size;
            right_rows_mem.push(row);
        }

        let chunk_size = std::cmp::max((100 * block_size) / 256, 100);
        let mut block_ids = Vec::new();
        let mut total_right_rows = 0;

        if !in_memory_mode {
            loop {
                let mut right_chunk = Vec::with_capacity(chunk_size);
                if !right_rows_mem.is_empty() {
                    right_chunk.append(&mut right_rows_mem);
                }
                while right_chunk.len() < chunk_size {
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
                let raw = rows_to_run_buffer(&right_chunk, block_size);
                let n = raw.len() / block_size;
                let new_ids = buffer_pool.write_raw_run_blocks(&raw, n);
                block_ids.extend(new_ids);
            }
        }

        let right_run = Run {
            block_ids,
            num_rows: total_right_rows,
        };
        let current_left_row = if in_memory_mode {
            left.next(buffer_pool)
        } else {
            None
        };

        JoinOp {
            left,
            right_run,
            right_rows_mem,
            in_memory_mode,
            current_left_row,
            current_mem_right_idx: 0,
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
            if self.in_memory_mode {
                let left_row = match &self.current_left_row {
                    Some(row) => row,
                    None => return None,
                };

                while self.current_mem_right_idx < self.right_rows_mem.len() {
                    let right_row = &self.right_rows_mem[self.current_mem_right_idx];
                    self.current_mem_right_idx += 1;

                    if left_row.values[self.left_col_idx] == right_row.values[self.right_col_idx] {
                        let combined = Self::combine_rows(&left_row.values, &right_row.values);
                        return Some(Row { values: combined });
                    }
                }

                self.current_left_row = self.left.next(pool);
                self.current_mem_right_idx = 0;
                continue;
            }

            // Populate left chunk if it is empty
            if self.left_chunk.is_empty() {
                let mut chunk = Vec::with_capacity(5000);
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
                self.right_reader = Some(RunReader::new(
                    &self.right_run,
                    self.right_types.clone(),
                    pool,
                ));
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
                        let combined = Self::combine_rows(&left_row.values, &right_row.values);
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
