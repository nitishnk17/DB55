use std::io::{Read, Write};
use common::{Data, DataType};
use db_config::table::ColumnSpec;
use db_config::statistics::ColumnStat;
use common::query::{Predicate, ComparisionOperator, ComparisionValue};
use crate::buffer_pool::BufferPool;
use crate::row::{Row, decode_block};
use crate::operator::Operator;
use std::collections::VecDeque;

pub struct TableScanner {
    column_names: Vec<String>,
    column_types: Vec<DataType>,
    all_types: Vec<DataType>,
    needed_column_indices: Vec<usize>,
    scan_start_block: u64,
    scan_end_block: u64,
    current_block: u64,
    batch_rows: VecDeque<Row>,
}

impl TableScanner {
    pub fn new(
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        file_id: &str,
        column_specs: &[ColumnSpec],
        needed_column_indices: Vec<usize>,
        pushed_predicate: Option<Predicate>,
    ) -> Self {
        let mut scan_start_block = buffer_pool.get_file_start_block(file_id);
        let mut scan_end_block  = scan_start_block + buffer_pool.get_file_num_blocks(file_id);

        let mut column_names: Vec<String> = Vec::new();
        let mut column_types: Vec<DataType> = Vec::new();

        for &idx in &needed_column_indices {
            column_names.push(column_specs[idx].column_name.clone());
            column_types.push(column_specs[idx].data_type.clone());
        }

        let all_types: Vec<DataType> = column_specs
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        // ── Binary Search Optimization ─────────────────────────────────────
        if let Some(pred) = pushed_predicate {
            if let Some(idx) = column_specs.iter().position(|c| c.column_name == pred.column_name) {
                let spec = &column_specs[idx];
                let is_ordered = spec.stats.as_ref().map_or(false, |stats| {
                    stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered))
                });

                if is_ordered {
                    eprintln!("TableScanner: Using binary search on '{}' for {:?}", pred.column_name, pred.operator);
                    
                    let cmp = |row_val: &Data| -> std::cmp::Ordering {
                        match (&pred.value, row_val) {
                            (ComparisionValue::I32(c), Data::Int32(r)) => r.cmp(c),
                            (ComparisionValue::I64(c), Data::Int64(r)) => r.cmp(c),
                            (ComparisionValue::F32(c), Data::Float32(r)) => r.total_cmp(c),
                            (ComparisionValue::F64(c), Data::Float64(r)) => r.total_cmp(c),
                            (ComparisionValue::String(c), Data::String(r)) => r.cmp(c),
                            _ => std::cmp::Ordering::Equal,
                        }
                    };

                    // For EQ, GT, GTE: find lower bound block
                    if matches!(pred.operator, ComparisionOperator::EQ | ComparisionOperator::GT | ComparisionOperator::GTE) {
                        let mut low = scan_start_block;
                        let mut high = scan_end_block;
                        while low < high {
                            let mid = low + (high - low) / 2;
                            let raw = buffer_pool.read_blocks_sequential(mid, 1);
                            let rows = decode_block(&raw, &all_types, &[idx]);
                            
                            if let Some(last_row) = rows.last() {
                                if cmp(&last_row.values[0]) == std::cmp::Ordering::Less {
                                    low = mid + 1;
                                } else {
                                    high = mid;
                                }
                            } else {
                                break;
                            }
                        }
                        scan_start_block = low;
                    }

                    // For EQ, LT, LTE: find upper bound block
                    if matches!(pred.operator, ComparisionOperator::EQ | ComparisionOperator::LT | ComparisionOperator::LTE) {
                        let mut low = scan_start_block;
                        let mut high = scan_end_block;
                        while low < high {
                            let mid = low + (high - low) / 2;
                            let raw = buffer_pool.read_blocks_sequential(mid, 1);
                            let rows = decode_block(&raw, &all_types, &[idx]);
                            
                            if let Some(first_row) = rows.first() {
                                if cmp(&first_row.values[0]) == std::cmp::Ordering::Greater {
                                    high = mid;
                                } else {
                                    low = mid + 1;
                                }
                            } else {
                                break;
                            }
                        }
                        scan_end_block = low; // exclusive end block
                    }
                }
            }
        }

        let current_block = scan_start_block;

        TableScanner {
            column_names,
            column_types,
            all_types,
            needed_column_indices,
            scan_start_block,
            scan_end_block,
            current_block,
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

            if self.current_block >= self.scan_end_block {
                return None;
            }

            // Adapt dynamically limit based on BufferPool frames but fallback reasonably
            // Using a max of either 20% of buffer pool or 256
            let available_frames = (pool.num_frames() / 5).max(32) as u64;
            let dynamic_batch_size = available_frames.clamp(32, 256);

            let count = dynamic_batch_size.min(self.scan_end_block - self.current_block);
            let raw = pool.read_blocks_sequential(self.current_block, count);
            let block_size = pool.block_size();

            for i in 0..count as usize {
                let begin = i * block_size;
                let end   = begin + block_size;
                let block_data = &raw[begin..end];
                let rows = decode_block(block_data, &self.all_types, &self.needed_column_indices);
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
