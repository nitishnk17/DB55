use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};

use common::Data;
use db_config::table::ColumnSpec;

use crate::buffer_pool::BufferPool;
use crate::disk_run::{rows_to_blocks, Run, RunReader};
use crate::operator::Operator;
use crate::row::Row;

// ─── Grace Hash Join ─────────────────────────────────────────────────────

/// Grace Hash Join operator.
///
/// Phase 1 (Partition): Both relations are hashed on the join key and
///     partitioned into N buckets, each written to anonymous disk blocks.
/// Phase 2 (Build & Probe): For each partition pair, the smaller side is
///     loaded into an in-memory hash table and the larger side is scanned
///     to probe for matches.
///
/// This is superior to BNLJ for large tables because the partition step
/// ensures each in-memory hash table is small (≈ total_rows / N).
pub struct HashJoinOp {
    /// All joined result rows (materialized during construction).
    joined_rows: Vec<Row>,
    /// Current position in joined_rows for the iterator.
    current_index: usize,
    /// Output schema = left_schema ++ right_schema.
    output_schema: Vec<String>,
}

// ─── Hashing Helper ──────────────────────────────────────────────────────

/// Compute a deterministic hash for a Data value.
/// Used to assign rows to partition buckets.
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

// ─── Partition Phase ─────────────────────────────────────────────────────

/// Reads all rows from `input`, hashes each on `join_col_idx`, and writes
/// them into `num_partitions` disk-backed partitions (as `Run`s).
///
/// Returns a Vec<Option<Run>> of length `num_partitions`.
/// A None entry means that partition received zero rows.
fn partition_input(
    input: &mut Box<dyn Operator>,
    join_col_idx: usize,
    num_partitions: usize,
    _column_specs: &[ColumnSpec],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
) -> Vec<Option<Run>> {
    // Accumulate rows per bucket in memory, then flush to disk.
    let mut buckets: Vec<Vec<Row>> = (0..num_partitions).map(|_| Vec::new()).collect();

    while let Some(row) = input.next() {
        let h = hash_data(&row.values[join_col_idx]);
        let bucket_id = (h as usize) % num_partitions;
        buckets[bucket_id].push(row);
    }

    let block_size = buffer_pool.block_size();

    buckets
        .into_iter()
        .map(|bucket_rows| {
            if bucket_rows.is_empty() {
                return None;
            }
            let num_rows = bucket_rows.len();
            let blocks = rows_to_blocks(&bucket_rows, block_size);
            let num_blocks = blocks.len() as u64;
            let start_block = buffer_pool.allocate_anon_blocks(num_blocks);
            for (i, block_data) in blocks.iter().enumerate() {
                buffer_pool.write_block(start_block + i as u64, block_data);
            }
            Some(Run {
                start_block,
                num_blocks,
                num_rows,
            })
        })
        .collect()
}

// ─── Build & Probe Phase ─────────────────────────────────────────────────

/// For a single partition pair, load the build side into a hash map and
/// probe with the other side. Returns all matching joined rows.
fn build_and_probe(
    build_run: &Run,
    probe_run: &Run,
    build_col_idx: usize,
    probe_col_idx: usize,
    build_specs: &[ColumnSpec],
    probe_specs: &[ColumnSpec],
    build_is_left: bool,
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
) -> Vec<Row> {
    // BUILD: load entire build partition into an in-memory hash map.
    // Key = hash(join_val), Value = Vec<Row> (to handle collisions).
    //
    // Pre-size the map with the known row count so the HashMap never needs to
    // rehash during the build phase.  `with_capacity(n)` guarantees at least n
    // buckets, eliminating O(log n) rehash copies for large partitions.
    let mut hash_table: HashMap<u64, Vec<Row>> = HashMap::with_capacity(build_run.num_rows);

    let mut build_reader = RunReader::new(build_run, build_specs.to_vec(), buffer_pool);
    loop {
        if let Some(row) = build_reader.peek() {
            let h = hash_data(&row.values[build_col_idx]);
            hash_table.entry(h).or_default().push(row.clone());
        } else {
            break;
        }
        build_reader.advance(buffer_pool);
    }

    // PROBE: scan probe partition, look up matches in hash table.
    // Pre-allocate to avoid repeated reallocations; min(build, probe) is a
    // safe lower bound for an equi-join result size.
    let mut results = Vec::with_capacity(build_run.num_rows.min(probe_run.num_rows));

    let mut probe_reader = RunReader::new(probe_run, probe_specs.to_vec(), buffer_pool);
    loop {
        if let Some(probe_row) = probe_reader.peek() {
            let h = hash_data(&probe_row.values[probe_col_idx]);
            if let Some(candidates) = hash_table.get(&h) {
                for build_row in candidates {
                    // Exact value comparison (not just hash) to handle collisions
                    if build_row.values[build_col_idx] == probe_row.values[probe_col_idx] {
                        // Combine: always left ++ right regardless of which was build vs probe
                        let combined = if build_is_left {
                            let mut v = build_row.values.clone();
                            v.extend(probe_row.values.clone());
                            v
                        } else {
                            let mut v = probe_row.values.clone();
                            v.extend(build_row.values.clone());
                            v
                        };
                        results.push(Row { values: combined });
                    }
                }
            }
        } else {
            break;
        }
        probe_reader.advance(buffer_pool);
    }

    results
}

// ─── HashJoinOp Implementation ───────────────────────────────────────────

impl HashJoinOp {
    pub fn new(
        mut left: Box<dyn Operator>,
        mut right: Box<dyn Operator>,
        left_col_idx: usize,
        right_col_idx: usize,
        left_column_specs: Vec<ColumnSpec>,
        right_column_specs: Vec<ColumnSpec>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        // Output schema = left columns followed by right columns
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        // Number of partitions — 64 is a reasonable default.
        // For very large tables this keeps each partition small enough
        // to fit an in-memory hash table within the 64 MB budget.
        let num_partitions = 64;

        eprintln!(
            "HashJoin: partitioning into {} buckets (left_col={}, right_col={})",
            num_partitions, left_col_idx, right_col_idx
        );

        // Phase 1: Partition both inputs
        let partitions_left = partition_input(
            &mut left,
            left_col_idx,
            num_partitions,
            &left_column_specs,
            buffer_pool,
        );
        let partitions_right = partition_input(
            &mut right,
            right_col_idx,
            num_partitions,
            &right_column_specs,
            buffer_pool,
        );

        // Phase 2: Build & Probe for each partition pair
        let mut joined_rows = Vec::new();

        for i in 0..num_partitions {
            let (left_part, right_part) = match (&partitions_left[i], &partitions_right[i]) {
                (Some(l), Some(r)) => (l, r),
                _ => continue, // Skip empty partition pairs
            };

            // Choose the smaller partition as the build side
            let (build_is_left, build_run, probe_run, build_specs, probe_specs, build_idx, probe_idx) =
                if left_part.num_rows <= right_part.num_rows {
                    (true, left_part, right_part, &left_column_specs, &right_column_specs, left_col_idx, right_col_idx)
                } else {
                    (false, right_part, left_part, &right_column_specs, &left_column_specs, right_col_idx, left_col_idx)
                };

            let partition_results = build_and_probe(
                build_run,
                probe_run,
                build_idx,
                probe_idx,
                build_specs,
                probe_specs,
                build_is_left,
                buffer_pool,
            );

            joined_rows.extend(partition_results);
        }

        eprintln!("HashJoin: produced {} result rows", joined_rows.len());

        HashJoinOp {
            joined_rows,
            current_index: 0,
            output_schema,
        }
    }
}

// ─── Operator trait ──────────────────────────────────────────────────────

impl Operator for HashJoinOp {
    fn next(&mut self) -> Option<Row> {
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
}
