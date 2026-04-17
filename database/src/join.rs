use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};

use common::{Data, DataType};

use crate::buffer_pool::BufferPool;
use crate::operator::Operator;
use crate::row::Row;
use common::query::SortSpec;

// ─── Hash Helper ──────────────────────────────────────────────────────────

fn hash_data(val: &Data) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match val {
        Data::Int32(v) => v.hash(&mut hasher),
        Data::Int64(v) => v.hash(&mut hasher),
        Data::Float32(v) => v.to_bits().hash(&mut hasher),
        Data::Float64(v) => v.to_bits().hash(&mut hasher),
        Data::String(v) => v.hash(&mut hasher),
    }
    hasher.finish()
}

// ─── In-Memory Hash Join ──────────────────────────────────────────────────

/// In-Memory Hash Join operator.
///
/// Builds a hash map of the entire right relation in memory.
/// Streams the left relation and probes the hash map.
/// This operator should be used only when the right relation is small enough to fit in RAM.
pub struct JoinOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    hash_table: HashMap<u64, Vec<Row>>,
    left_col_idx: usize,
    right_col_idx: usize,

    current_probe_row: Option<Row>,
    current_matches: Vec<Row>,
    current_match_idx: usize,

    output_schema: Vec<String>,
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> JoinOp<R, W> {
    pub fn new(
        left: Box<dyn Operator<R, W>>,
        mut right: Box<dyn Operator<R, W>>,
        left_col_idx: usize,
        right_col_idx: usize,
        _right_types: Vec<DataType>,
        buffer_pool: &mut BufferPool<R, W>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        // Build phase: Load the entire right side into memory
        let mut hash_table: HashMap<u64, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next(buffer_pool) {
            let h = hash_data(&row.values[right_col_idx]);
            hash_table.entry(h).or_default().push(row);
        }

        JoinOp {
            left,
            hash_table,
            left_col_idx,
            right_col_idx,
            current_probe_row: None,
            current_matches: Vec::new(),
            current_match_idx: 0,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for JoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            // 1. Yield matched build rows one by one
            if self.current_match_idx < self.current_matches.len() {
                let right_row = &self.current_matches[self.current_match_idx];
                self.current_match_idx += 1;
                let left_row = self.current_probe_row.as_ref().expect("probe row must exist");

                let mut combined = left_row.values.clone();
                combined.extend(right_row.values.clone());
                return Some(Row { values: combined });
            }

            // 2. Scan probe (left) side for next matching row
            if let Some(left_row) = self.left.next(pool) {
                self.current_matches.clear();
                self.current_match_idx = 0;

                let h = hash_data(&left_row.values[self.left_col_idx]);
                if let Some(candidates) = self.hash_table.get(&h) {
                    for right_row in candidates {
                        // Equality check after hash match
                        if left_row.values[self.left_col_idx] == right_row.values[self.right_col_idx] {
                            self.current_matches.push(right_row.clone());
                        }
                    }
                }
                
                if !self.current_matches.is_empty() {
                    self.current_probe_row = Some(left_row);
                    continue; // Yield the matches in the next loop iteration
                }
            } else {
                return None; // Left exhausted
            }
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
