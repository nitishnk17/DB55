use crate::disk_run::{Run, RunReader, rows_to_run_buffer};
use crate::operator::Operator;
use crate::row::Row;

use crate::buffer_pool::BufferPool;
use common::Data;
use common::DataType;
use std::io::{Read, Write};

pub struct CrossOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_run: Run,
    right_rows_mem: Vec<Row>,
    in_memory_mode: bool,
    current_left_row: Option<Row>,
    right_reader: Option<RunReader>,
    current_mem_right_idx: usize,
    left_chunk: Vec<Row>,
    chunk_left_idx: usize,
    right_types: Vec<DataType>,
    pool_ptr: *mut BufferPool<R, W>,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> CrossOp<R, W> {
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
        pool: &mut BufferPool<R, W>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());
        let right_types = right.data_types();

        let mut right_rows_mem = Vec::with_capacity(1024);
        let memory_budget_bytes =
            (pool.num_frames() * pool.block_size()).clamp(4 * 1024 * 1024, 16 * 1024 * 1024);
        let right_row_size = Self::estimate_row_size(&right_types);
        let mut bytes_used = 0usize;
        let mut exceeded = false;

        while let Some(row) = right.next(pool) {
            if bytes_used + right_row_size > memory_budget_bytes {
                exceeded = true;
                break;
            }
            bytes_used += right_row_size;
            right_rows_mem.push(row);
        }

        let in_memory_mode: bool;
        let mut right_run = Run {
            block_ids: vec![],
            num_rows: 0,
        };

        if !exceeded {
            in_memory_mode = true;
        } else {
            in_memory_mode = false;
            let block_size = pool.block_size();
            let chunk_size = std::cmp::max((100 * block_size) / 256, 100);
            let mut block_ids = Vec::new();
            let mut total_right_rows = 0;

            // Write the buffered prefix first, then stream the remainder.
            let raw = rows_to_run_buffer(&right_rows_mem, block_size);
            let n = raw.len() / block_size;
            let new_ids = pool.write_raw_run_blocks(&raw, n);
            total_right_rows += right_rows_mem.len();
            block_ids.extend(new_ids);
            right_rows_mem.clear(); // Free memory

            // Stream the rest
            loop {
                let mut right_chunk = Vec::with_capacity(chunk_size);
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
                let raw = rows_to_run_buffer(&right_chunk, block_size);
                let n = raw.len() / block_size;
                let new_ids = pool.write_raw_run_blocks(&raw, n);
                block_ids.extend(new_ids);
            }

            right_run = Run {
                block_ids,
                num_rows: total_right_rows,
            };
        }

        let current_left_row = left.next(pool);

        CrossOp {
            left,
            right_run,
            right_rows_mem,
            in_memory_mode,
            current_left_row,
            right_reader: None,
            current_mem_right_idx: 0,
            left_chunk: Vec::new(),
            chunk_left_idx: 0,
            right_types,
            pool_ptr: pool as *mut BufferPool<R, W>,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Drop for CrossOp<R, W> {
    fn drop(&mut self) {
        if self.pool_ptr.is_null() {
            return;
        }
        // SAFETY: pool_ptr is captured from the live buffer pool during operator
        // construction and remains valid until operator teardown.
        let pool = unsafe { &mut *self.pool_ptr };
        if !self.right_run.block_ids.is_empty() {
            pool.free_run(&self.right_run);
            self.right_run.block_ids.clear();
        }
        if let Some(reader) = &mut self.right_reader {
            if !reader.run.block_ids.is_empty() {
                pool.free_run(&reader.run);
                reader.run.block_ids.clear();
            }
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for CrossOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            if self.in_memory_mode {
                let left_row = match &self.current_left_row {
                    Some(row) => row,
                    None => return None,
                };

                if self.current_mem_right_idx < self.right_rows_mem.len() {
                    let right_row = &self.right_rows_mem[self.current_mem_right_idx];
                    self.current_mem_right_idx += 1;

                    let combined = Self::combine_rows(&left_row.values, &right_row.values);
                    return Some(Row { values: combined });
                } else {
                    self.current_left_row = self.left.next(pool);
                    self.current_mem_right_idx = 0;
                }
            } else {
                // Disk Mode (Chunked BNLJ)
                if self.left_chunk.is_empty() {
                    self.left_chunk.reserve(5000);
                    for _ in 0..5000 {
                        if let Some(row) = self.left.next(pool) {
                            self.left_chunk.push(row);
                        } else {
                            break;
                        }
                    }
                    if self.left_chunk.is_empty() {
                        if !self.right_run.block_ids.is_empty() {
                            pool.free_run(&self.right_run);
                            self.right_run.block_ids.clear();
                        }
                        return None;
                    }
                }

                if self.right_reader.is_none() {
                    self.right_reader = Some(RunReader::new(
                        &self.right_run,
                        self.right_types.clone(),
                        pool,
                    ));
                }

                let reader = self.right_reader.as_mut().unwrap();

                if let Some(right_row) = reader.peek() {
                    let left_row = &self.left_chunk[self.chunk_left_idx];
                    let combined = Self::combine_rows(&left_row.values, &right_row.values);

                    self.chunk_left_idx += 1;

                    if self.chunk_left_idx == self.left_chunk.len() {
                        self.chunk_left_idx = 0;
                        reader.advance(pool);
                    }
                    return Some(Row { values: combined });
                }

                // Exhausted right run. Reset for next chunk!
                self.right_reader = None;
                self.left_chunk.clear();
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
