use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};

use common::{Data, DataType};

use crate::buffer_pool::BufferPool;
use crate::disk_run::{rows_to_blocks, Run, RunReader};
use crate::operator::Operator;
use crate::row::Row;

// ─── Grace Hash Join ─────────────────────────────────────────────────────

/// Grace Hash Join operator.
///
/// Phase 1 (Partition): Both relations are hashed on the join key and
///     partitioned into N buckets, written incrementally to anonymous disk blocks.
/// Phase 2 (Build & Probe): Lazily loads the smaller partition into an in-memory 
///     hash table and scans the larger partition to yield joined rows.
pub struct HashJoinOp<R: Read, W: Write> {
    partitions_left: Vec<Option<Run>>,
    partitions_right: Vec<Option<Run>>,
    left_types: Vec<DataType>,
    right_types: Vec<DataType>,
    left_col_idx: usize,
    right_col_idx: usize,

    current_partition: usize,
    build_is_left: bool,
    hash_table: HashMap<u64, Vec<Row>>,
    probe_reader: Option<RunReader>,

    current_probe_row: Option<Row>,
    current_matches: Vec<Row>,
    current_match_idx: usize,

    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    _marker: std::marker::PhantomData<(R, W)>,
}

// ─── Hashing Helper ──────────────────────────────────────────────────────

struct FnvHasher(u64);

impl Default for FnvHasher {
    fn default() -> Self {
        FnvHasher(0xcbf29ce484222325)
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

fn hash_data(val: &Data) -> u64 {
    let mut hasher = FnvHasher::default();
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

fn partition_input<R: Read, W: Write>(
    input: &mut Box<dyn Operator<R, W>>,
    join_col_idx: usize,
    num_partitions: usize,
    buffer_pool: &mut BufferPool<R, W>,
) -> Vec<Option<Run>> {
    let mut buckets: Vec<Vec<Row>> = (0..num_partitions).map(|_| Vec::new()).collect();
    let mut block_ids: Vec<Vec<u64>> = (0..num_partitions).map(|_| Vec::new()).collect();
    let mut total_rows: Vec<usize> = vec![0; num_partitions];

    let block_size = buffer_pool.block_size();
    // We have a 64 MB memory budget. Reserving ~15MB for 64 partition buffers
    // means each bucket can hold ~240KB before flushing.
    // 4000 rows per flush creates much larger contiguous disk allocation chunks
    // and drastically reduces seek times from fragmentation.
    let rows_per_flush = 4000;

    while let Some(row) = input.next(buffer_pool) {
        let h = hash_data(&row.values[join_col_idx]);
        let bucket_id = (h as usize) % num_partitions;
        buckets[bucket_id].push(row);
        total_rows[bucket_id] += 1;

        if buckets[bucket_id].len() >= rows_per_flush {
            let blocks = rows_to_blocks(&buckets[bucket_id], block_size);
            let start_block = buffer_pool.allocate_anon_blocks(blocks.len() as u64);
            for (i, block_data) in blocks.iter().enumerate() {
                let bid = start_block + i as u64;
                buffer_pool.write_block(bid, block_data);
                block_ids[bucket_id].push(bid);
            }
            buckets[bucket_id].clear();
        }
    }

    for bucket_id in 0..num_partitions {
        if !buckets[bucket_id].is_empty() {
            let blocks = rows_to_blocks(&buckets[bucket_id], block_size);
            let start_block = buffer_pool.allocate_anon_blocks(blocks.len() as u64);
            for (i, block_data) in blocks.iter().enumerate() {
                let bid = start_block + i as u64;
                buffer_pool.write_block(bid, block_data);
                block_ids[bucket_id].push(bid);
            }
        }
    }

    block_ids
        .into_iter()
        .zip(total_rows.into_iter())
        .map(|(b_ids, count)| {
            if count == 0 {
                None
            } else {
                Some(Run {
                    block_ids: b_ids,
                    num_rows: count,
                })
            }
        })
        .collect()
}

// ─── HashJoinOp Implementation ───────────────────────────────────────────

impl<R: Read, W: Write> HashJoinOp<R, W> {
    pub fn new(
        mut left: Box<dyn Operator<R, W>>,
        mut right: Box<dyn Operator<R, W>>,
        left_col_idx: usize,
        right_col_idx: usize,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        buffer_pool: &mut BufferPool<R, W>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        let num_partitions = 64;

        eprintln!(
            "HashJoin: partitioning into {} buckets (left_col={}, right_col={})",
            num_partitions, left_col_idx, right_col_idx
        );

        let partitions_left = partition_input(&mut left, left_col_idx, num_partitions, buffer_pool);
        let partitions_right = partition_input(&mut right, right_col_idx, num_partitions, buffer_pool);

        HashJoinOp {
            partitions_left,
            partitions_right,
            left_types,
            right_types,
            left_col_idx,
            right_col_idx,

            current_partition: 0,
            build_is_left: true,
            hash_table: HashMap::new(),
            probe_reader: None,

            current_probe_row: None,
            current_matches: Vec::new(),
            current_match_idx: 0,

            output_schema,
            output_types,
            _marker: std::marker::PhantomData,
        }
    }
}

// ─── Operator trait ──────────────────────────────────────────────────────

impl<R: Read, W: Write> Operator<R, W> for HashJoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            // 1. Yield matched build rows one by one
            if self.current_match_idx < self.current_matches.len() {
                let build_row = &self.current_matches[self.current_match_idx];
                self.current_match_idx += 1;
                let probe_row = self.current_probe_row.as_ref().unwrap();

                let combined = if self.build_is_left {
                    let mut v = build_row.values.clone();
                    v.extend(probe_row.values.clone());
                    v
                } else {
                    let mut v = probe_row.values.clone();
                    v.extend(build_row.values.clone());
                    v
                };
                return Some(Row { values: combined });
            }

            // 2. Scan probe partition for next matching row
            if let Some(reader) = &mut self.probe_reader {
                if let Some(probe_row) = reader.peek() {
                    let probe_row_clone = probe_row.clone();
                    self.current_probe_row = Some(probe_row_clone.clone());
                    self.current_matches.clear();
                    self.current_match_idx = 0;

                    let probe_idx = if self.build_is_left { self.right_col_idx } else { self.left_col_idx };
                    let build_idx = if self.build_is_left { self.left_col_idx } else { self.right_col_idx };

                    let h = hash_data(&probe_row_clone.values[probe_idx]);
                    if let Some(candidates) = self.hash_table.get(&h) {
                        for build_row in candidates {
                            if build_row.values[build_idx] == probe_row_clone.values[probe_idx] {
                                self.current_matches.push(build_row.clone());
                            }
                        }
                    }
                    reader.advance(pool);
                    continue; // Loop back around to yield matches
                } else {
                    // Exhausted probe reader
                    if let Some(reader) = self.probe_reader.take() {
                        pool.free_run(&reader.run);
                    }
                }
            }

            // 3. Move to next partition pair
            if self.current_partition >= self.partitions_left.len() {
                return None; // Fully exhausted all partitions
            }

            let p_idx = self.current_partition;
            self.current_partition += 1;

            if let (Some(left_part), Some(right_part)) = (&self.partitions_left[p_idx], &self.partitions_right[p_idx]) {
                self.hash_table.clear();
                
                let (build_is_left, build_run, probe_run, build_types, probe_types, build_col_idx) =
                    if left_part.num_rows <= right_part.num_rows {
                        (true, left_part, right_part, self.left_types.clone(), self.right_types.clone(), self.left_col_idx)
                    } else {
                        (false, right_part, left_part, self.right_types.clone(), self.left_types.clone(), self.right_col_idx)
                    };
                self.build_is_left = build_is_left;

                // Load build side into memory hash table
                let mut build_reader = RunReader::new(build_run, build_types, pool);
                while let Some(row) = build_reader.peek() {
                    let h = hash_data(&row.values[build_col_idx]);
                    self.hash_table.entry(h).or_default().push(row.clone());
                    build_reader.advance(pool);
                }
                pool.free_run(build_run);

                // Prepare probe reader
                self.probe_reader = Some(RunReader::new(probe_run, probe_types, pool));
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
