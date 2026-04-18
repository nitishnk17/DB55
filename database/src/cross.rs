use crate::operator::Operator;
use crate::row::Row;
use crate::disk_run::{rows_to_blocks, Run, RunReader};

use common::DataType;
use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;

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

        let mut right_rows_mem = Vec::new();
        let limit = 10_000;
        let mut exceeded = false;

        while let Some(row) = right.next(pool) {
            right_rows_mem.push(row);
            if right_rows_mem.len() >= limit {
                exceeded = true;
                break;
            }
        }

        let in_memory_mode: bool;
        let mut right_run = Run { block_ids: vec![], num_rows: 0 };

        if !exceeded {
            in_memory_mode = true;
        } else {
            in_memory_mode = false;
            let block_size = pool.block_size();
            let chunk_size = std::cmp::max((100 * block_size) / 256, 100);
            let mut block_ids = Vec::new();
            let mut total_right_rows = 0;

            // Write initial 10,000 rows
            let blocks = rows_to_blocks(&right_rows_mem, block_size);
            let num_blocks = blocks.len() as u64;
            let start_block = pool.allocate_anon_blocks(num_blocks);
            for (i, block_data) in blocks.iter().enumerate() {
                let bid = start_block + i as u64;
                pool.write_block(bid, block_data);
                block_ids.push(bid);
            }
            total_right_rows += right_rows_mem.len();
            right_rows_mem.clear(); // Free memory

            // Stream the rest
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
            output_schema,
            output_types,
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
                    
                    let mut combined = left_row.values.clone();
                    combined.extend(right_row.values.clone());
                    return Some(Row { values: combined });
                } else {
                    self.current_left_row = self.left.next(pool);
                    self.current_mem_right_idx = 0;
                }
            } else {
                // Disk Mode (Chunked BNLJ)
                if self.left_chunk.is_empty() {
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
                    self.right_reader = Some(RunReader::new(&self.right_run, self.right_types.clone(), pool));
                }

                let reader = self.right_reader.as_mut().unwrap();

                if let Some(right_row) = reader.peek() {
                    let left_row = &self.left_chunk[self.chunk_left_idx];
                    let mut combined = left_row.values.clone();
                    combined.extend(right_row.values.clone());
                    
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
