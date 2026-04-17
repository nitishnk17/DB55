use crate::operator::Operator;
use crate::row::Row;
use crate::disk_run::{rows_to_blocks, Run, RunReader};

use common::DataType;
use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;
use common::query::SortSpec;

pub struct CrossOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_run: Option<Run>,
    right_in_memory: Vec<Row>,
    current_left_row: Option<Row>,
    right_reader: Option<RunReader>,
    current_right_idx: usize,
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

        // Try to buffer right child in memory first
        let mut right_in_memory = Vec::new();
        let mut total_bytes = 0;
        let mem_budget = 20 * 1024 * 1024; // 20 MB budget for cross join right side
        let mut overflowed = false;

        while let Some(row) = right.next(pool) {
            let row_bytes = 24 + row.values.iter().map(|v| match v {
                common::Data::String(s) => 24 + s.capacity(),
                _ => 8,
            }).sum::<usize>();
            
            total_bytes += row_bytes;
            right_in_memory.push(row);

            if total_bytes > mem_budget {
                overflowed = true;
                break;
            }
        }

        let right_run = if overflowed {
            eprintln!("CrossJoin: right side exceeded memory budget, falling back to disk materialization");
            // Materialize the rest to disk
            let block_size = pool.block_size();
            let mut block_ids = Vec::new();
            let mut total_right_rows = right_in_memory.len();

            // Write what we already have in memory to disk
            let blocks = rows_to_blocks(&right_in_memory, block_size);
            let num_blocks = blocks.len() as u64;
            let start_block = pool.allocate_anon_blocks(num_blocks);
            for (i, block_data) in blocks.iter().enumerate() {
                pool.write_block(start_block + i as u64, block_data);
                block_ids.push(start_block + i as u64);
            }
            right_in_memory.clear();

            // Continue reading from right and writing to disk
            loop {
                let mut chunk = Vec::new();
                for _ in 0..1000 {
                    if let Some(row) = right.next(pool) {
                        chunk.push(row);
                    } else {
                        break;
                    }
                }
                if chunk.is_empty() { break; }
                total_right_rows += chunk.len();
                let blocks = rows_to_blocks(&chunk, block_size);
                let start = pool.allocate_anon_blocks(blocks.len() as u64);
                for (i, blk) in blocks.iter().enumerate() {
                    pool.write_block(start + i as u64, blk);
                    block_ids.push(start + i as u64);
                }
            }
            Some(Run { block_ids, num_rows: total_right_rows })
        } else {
            eprintln!("CrossJoin: right side fits in memory ({} rows)", right_in_memory.len());
            None
        };

        let current_left_row = left.next(pool);

        CrossOp {
            left,
            right_run,
            right_in_memory,
            current_left_row,
            right_reader: None,
            current_right_idx: 0,
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
                None => return None,
            };

            // Case A: Right side is in memory
            if self.right_run.is_none() {
                if self.current_right_idx < self.right_in_memory.len() {
                    let right_row = &self.right_in_memory[self.current_right_idx];
                    self.current_right_idx += 1;
                    let mut combined = left_row.values.clone();
                    combined.extend(right_row.values.clone());
                    return Some(Row { values: combined });
                }
                // Right side exhausted for this left row, advance left
                self.current_left_row = self.left.next(pool);
                self.current_right_idx = 0;
                continue;
            }

            // Case B: Right side is on disk
            if self.right_reader.is_none() {
                self.right_reader = Some(RunReader::new(self.right_run.as_ref().unwrap(), self.right_types.clone(), pool));
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

    fn order(&self) -> Option<Vec<SortSpec>> {
        self.left.order()
    }
}
