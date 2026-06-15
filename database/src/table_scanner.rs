use crate::buffer_pool::BufferPool;
use crate::hash_join::JoinFilter;
use crate::operator::Operator;
use crate::row::{Row, build_needed_mask, decode_block_with_mask, decode_block_with_mask_into};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::{Data, DataType};
use db_config::statistics::ColumnStat;
use db_config::table::ColumnSpec;
use std::io::{Read, Write};

pub struct TableScanner {
    column_names: Vec<String>,
    column_types: Vec<DataType>,
    all_types: Vec<DataType>,
    needed_mask: Vec<bool>,
    scan_end_block: u64,
    current_block: u64,
    batch_rows: std::vec::IntoIter<Row>,
    scan_predicates: Vec<ScanPredicate>,
    pub dynamic_filter: Option<(usize, std::sync::Arc<std::sync::Mutex<Option<JoinFilter>>>)>,
}

struct ScanPredicate {
    col_idx: usize,
    operator: ComparisionOperator,
    value: Data,
}

impl TableScanner {
    pub fn new(
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        file_id: &str,
        column_specs: &[ColumnSpec],
        needed_column_indices: Vec<usize>,
        pushed_predicates: Vec<Predicate>,
        dynamic_filter: Option<(String, std::sync::Arc<std::sync::Mutex<Option<JoinFilter>>>)>,
    ) -> Self {
        let mut scan_start_block = buffer_pool.get_file_start_block(file_id);
        let mut scan_end_block = scan_start_block + buffer_pool.get_file_num_blocks(file_id);

        let mut column_names: Vec<String> = Vec::new();
        let mut column_types: Vec<DataType> = Vec::new();

        for &idx in &needed_column_indices {
            column_names.push(column_specs[idx].column_name.clone());
            column_types.push(column_specs[idx].data_type.clone());
        }

        let all_types: Vec<DataType> = column_specs.iter().map(|c| c.data_type.clone()).collect();
        let needed_mask = build_needed_mask(all_types.len(), &needed_column_indices);

        // ── Binary Search Optimization ─────────────────────────────────────
        for pred in &pushed_predicates {
            if let Some(idx) = column_specs
                .iter()
                .position(|c| c.column_name == pred.column_name)
            {
                let spec = &column_specs[idx];

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

                // O(1) Range Pruning
                let mut impossible = false;
                if let Some(stats) = &spec.stats {
                    for stat in stats {
                        if let ColumnStat::RangeStat(range) = stat {
                            let order_lower = cmp(&range.lower_bound);
                            let order_upper = cmp(&range.upper_bound);

                            match &pred.operator {
                                ComparisionOperator::GT | ComparisionOperator::GTE => {
                                    if order_upper == std::cmp::Ordering::Less
                                        || (matches!(&pred.operator, ComparisionOperator::GT)
                                            && order_upper == std::cmp::Ordering::Equal)
                                    {
                                        impossible = true;
                                    }
                                }
                                ComparisionOperator::LT | ComparisionOperator::LTE => {
                                    if order_lower == std::cmp::Ordering::Greater
                                        || (matches!(&pred.operator, ComparisionOperator::LT)
                                            && order_lower == std::cmp::Ordering::Equal)
                                    {
                                        impossible = true;
                                    }
                                }
                                ComparisionOperator::EQ => {
                                    if order_lower == std::cmp::Ordering::Greater
                                        || order_upper == std::cmp::Ordering::Less
                                    {
                                        impossible = true;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }

                if impossible {
                    scan_end_block = scan_start_block;
                    break;
                } else {
                    let is_ordered = spec.stats.as_ref().map_or(false, |stats| {
                        stats
                            .iter()
                            .any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered))
                    });

                    if is_ordered {
                        let single_needed = build_needed_mask(all_types.len(), &[idx]);
                        // For EQ, GT, GTE: find lower bound block
                        if matches!(
                            &pred.operator,
                            ComparisionOperator::EQ
                                | ComparisionOperator::GT
                                | ComparisionOperator::GTE
                        ) {
                            let mut low = scan_start_block;
                            let mut high = scan_end_block;
                            while low < high {
                                let mid = low + (high - low) / 2;
                                let raw = buffer_pool.read_blocks_sequential(mid, 1);
                                let rows = decode_block_with_mask(&raw, &all_types, &single_needed);

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
                            scan_start_block = scan_start_block.max(low);
                        }

                        // For EQ, LT, LTE: find upper bound block
                        if matches!(
                            &pred.operator,
                            ComparisionOperator::EQ
                                | ComparisionOperator::LT
                                | ComparisionOperator::LTE
                        ) {
                            let mut low = scan_start_block;
                            let mut high = scan_end_block;
                            while low < high {
                                let mid = low + (high - low) / 2;
                                let raw = buffer_pool.read_blocks_sequential(mid, 1);
                                let rows = decode_block_with_mask(&raw, &all_types, &single_needed);

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
                            scan_end_block = scan_end_block.min(low); // exclusive end block
                        }
                    }
                }
            }
        }

        let current_block = scan_start_block;
        let scan_predicates: Vec<ScanPredicate> = pushed_predicates
            .iter()
            .filter_map(|pred| {
                let full_idx = column_specs
                    .iter()
                    .position(|c| c.column_name == pred.column_name)?;
                let projected_idx = needed_column_indices
                    .iter()
                    .position(|&needed_idx| needed_idx == full_idx)?;
                let value = match &pred.value {
                    ComparisionValue::I32(v) => Data::Int32(*v),
                    ComparisionValue::I64(v) => Data::Int64(*v),
                    ComparisionValue::F32(v) => Data::Float32(*v),
                    ComparisionValue::F64(v) => Data::Float64(*v),
                    ComparisionValue::String(v) => Data::String(v.clone()),
                    ComparisionValue::Column(_) => return None,
                };
                Some(ScanPredicate {
                    col_idx: projected_idx,
                    operator: match &pred.operator {
                        ComparisionOperator::EQ => ComparisionOperator::EQ,
                        ComparisionOperator::NE => ComparisionOperator::NE,
                        ComparisionOperator::GT => ComparisionOperator::GT,
                        ComparisionOperator::GTE => ComparisionOperator::GTE,
                        ComparisionOperator::LT => ComparisionOperator::LT,
                        ComparisionOperator::LTE => ComparisionOperator::LTE,
                    },
                    value,
                })
            })
            .collect();

        let dynamic_filter_idx = dynamic_filter.and_then(|(col_name, ptr)| {
            column_specs
                .iter()
                .position(|c| c.column_name == col_name)
                .and_then(|full_idx| {
                    needed_column_indices
                        .iter()
                        .position(|&needed_idx| needed_idx == full_idx)
                        .map(|projected_idx| (projected_idx, ptr))
                })
        });

        TableScanner {
            column_names,
            column_types,
            all_types,
            needed_mask,
            scan_end_block,
            current_block,
            batch_rows: Vec::new().into_iter(),
            scan_predicates,
            dynamic_filter: dynamic_filter_idx,
        }
    }

    #[inline]
    fn row_passes_scan_predicates(&self, row: &Row) -> bool {
        self.scan_predicates.iter().all(|pred| {
            let left = &row.values[pred.col_idx];
            let right = &pred.value;
            match &pred.operator {
                ComparisionOperator::EQ => left == right,
                ComparisionOperator::NE => left != right,
                ComparisionOperator::GT => {
                    left.partial_cmp(right) == Some(std::cmp::Ordering::Greater)
                }
                ComparisionOperator::GTE => {
                    matches!(
                        left.partial_cmp(right),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    )
                }
                ComparisionOperator::LT => {
                    left.partial_cmp(right) == Some(std::cmp::Ordering::Less)
                }
                ComparisionOperator::LTE => {
                    matches!(
                        left.partial_cmp(right),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    )
                }
            }
        })
    }
}

impl<R: Read, W: Write> Operator<R, W> for TableScanner {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            if let Some(r) = self.batch_rows.next() {
                return Some(r);
            }

            if self.current_block >= self.scan_end_block {
                return None;
            }

            // Target ~4 MB per disk command to amortize the pipe round-trip.
            // The pool no longer pre-allocates frame data, so larger sequential
            // reads are cheap (one transient `Vec<u8>` per call, freed at end
            // of the loop iteration).
            let block_size_u64 = pool.block_size() as u64;
            let target_bytes: u64 = 4 * 1024 * 1024;
            let dynamic_batch_size = (target_bytes / block_size_u64.max(1)).clamp(64, 4096);

            let count = dynamic_batch_size.min(self.scan_end_block - self.current_block);
            let raw = pool.read_blocks_sequential(self.current_block, count);
            let block_size = pool.block_size();
            let mut next_batch_rows = Vec::with_capacity((count as usize) * 16);
            let mut block_rows = Vec::new();

            if self.scan_predicates.is_empty() && self.dynamic_filter.is_none() {
                for i in 0..count as usize {
                    let begin = i * block_size;
                    let end = begin + block_size;
                    let block_data = &raw[begin..end];
                    decode_block_with_mask_into(
                        block_data,
                        &self.all_types,
                        &self.needed_mask,
                        &mut next_batch_rows,
                    );
                }
            } else if let Some((col_idx, ref bf_mutex)) = self.dynamic_filter {
                // Acquire once per batch instead of once per block.
                if let Ok(bf_guard) = bf_mutex.try_lock() {
                    if let Some(ref filter) = *bf_guard {
                        for i in 0..count as usize {
                            let begin = i * block_size;
                            let end = begin + block_size;
                            let block_data = &raw[begin..end];
                            block_rows.clear();
                            decode_block_with_mask_into(
                                block_data,
                                &self.all_types,
                                &self.needed_mask,
                                &mut block_rows,
                            );
                            for r in block_rows.drain(..) {
                                if !self.row_passes_scan_predicates(&r) {
                                    continue;
                                }
                                let h = crate::hash_join::hash_data(&r.values[col_idx]);
                                if filter.might_contain_hash(h) {
                                    next_batch_rows.push(r);
                                }
                            }
                        }
                    } else {
                        for i in 0..count as usize {
                            let begin = i * block_size;
                            let end = begin + block_size;
                            let block_data = &raw[begin..end];
                            block_rows.clear();
                            decode_block_with_mask_into(
                                block_data,
                                &self.all_types,
                                &self.needed_mask,
                                &mut block_rows,
                            );
                            for r in block_rows.drain(..) {
                                if self.row_passes_scan_predicates(&r) {
                                    next_batch_rows.push(r);
                                }
                            }
                        }
                    }
                } else {
                    // If lock is contended, don't block the scan path.
                    for i in 0..count as usize {
                        let begin = i * block_size;
                        let end = begin + block_size;
                        let block_data = &raw[begin..end];
                        block_rows.clear();
                        decode_block_with_mask_into(
                            block_data,
                            &self.all_types,
                            &self.needed_mask,
                            &mut block_rows,
                        );
                        for r in block_rows.drain(..) {
                            if self.row_passes_scan_predicates(&r) {
                                next_batch_rows.push(r);
                            }
                        }
                    }
                }
            } else {
                for i in 0..count as usize {
                    let begin = i * block_size;
                    let end = begin + block_size;
                    let block_data = &raw[begin..end];
                    block_rows.clear();
                    decode_block_with_mask_into(
                        block_data,
                        &self.all_types,
                        &self.needed_mask,
                        &mut block_rows,
                    );
                    for r in block_rows.drain(..) {
                        if self.row_passes_scan_predicates(&r) {
                            next_batch_rows.push(r);
                        }
                    }
                }
            }
            self.current_block += count;
            self.batch_rows = next_batch_rows.into_iter();
        }
    }

    fn schema(&self) -> Vec<String> {
        self.column_names.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.column_types.clone()
    }
}
