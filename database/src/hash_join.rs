use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{Read, Write};

use common::{Data, DataType};

use crate::buffer_pool::BufferPool;
use crate::disk_run::{Run, RunReader, encoded_row_size, write_row_into};
use crate::operator::Operator;
use crate::row::Row;

const DEFAULT_JOIN_FILTER_BITS: usize = 1 << 23;
const PROBE_SKIP_FILTER_BITS: usize = 1 << 21;

/// Chained-hash build side used by [`HashJoinOp`].
///
/// Stores all build rows in one flat `Vec<Row>` plus a parallel `Vec<i32>`
/// of "next-in-chain" indices (`-1` terminates).  The head map gives O(1)
/// access to the first row of each chain.  Compared to `HashMap<u64, Vec<Row>>`
/// this eliminates the per-bucket `Vec<Row>` heap allocation (one per unique
/// key — millions for a PK-style join) and removes the per-yield HashMap
/// lookup during probing.
struct BuildSide {
    rows: Vec<Row>,
    next: Vec<i32>,
    head: HashMap<u64, i32, BuildHasherDefault<U64IdentityHasher>>,
}

impl BuildSide {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            next: Vec::new(),
            head: HashMap::with_hasher(BuildHasherDefault::default()),
        }
    }

    fn with_capacity(n: usize) -> Self {
        Self {
            rows: Vec::with_capacity(n),
            next: Vec::with_capacity(n),
            head: HashMap::with_capacity_and_hasher(n.max(64), BuildHasherDefault::default()),
        }
    }

    #[inline]
    fn insert(&mut self, hash: u64, row: Row) {
        let new_idx = self.rows.len() as i32;
        let prev = self.head.insert(hash, new_idx).unwrap_or(-1);
        self.rows.push(row);
        self.next.push(prev);
    }

    #[inline]
    fn first(&self, hash: u64) -> i32 {
        self.head.get(&hash).copied().unwrap_or(-1)
    }

    #[inline]
    fn next_index(&self, idx: i32) -> i32 {
        self.next[idx as usize]
    }

    #[inline]
    fn row(&self, idx: i32) -> &Row {
        &self.rows[idx as usize]
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.next.clear();
        self.head.clear();
    }

    fn reserve(&mut self, additional: usize) {
        self.rows.reserve(additional);
        self.next.reserve(additional);
        self.head.reserve(additional);
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Default)]
struct U64IdentityHasher(u64);

impl Hasher for U64IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // Fallback path (not expected for u64 keys).
        let mut acc = 0u64;
        for &b in bytes {
            acc = acc.wrapping_mul(131).wrapping_add(b as u64);
        }
        self.0 = acc;
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

pub struct JoinFilter {
    bits: Vec<u64>,
    mask: usize,
}

impl JoinFilter {
    pub fn new(bit_count: usize) -> Self {
        assert!(bit_count.is_power_of_two());
        JoinFilter {
            bits: vec![0; bit_count / 64],
            mask: bit_count - 1,
        }
    }

    pub fn insert_hash(&mut self, hash: u64) {
        let (idx1, idx2, idx3) = self.bit_positions(hash);
        self.bits[idx1 / 64] |= 1u64 << (idx1 % 64);
        self.bits[idx2 / 64] |= 1u64 << (idx2 % 64);
        self.bits[idx3 / 64] |= 1u64 << (idx3 % 64);
    }

    #[inline]
    pub fn might_contain_hash(&self, hash: u64) -> bool {
        let (idx1, idx2, idx3) = self.bit_positions(hash);
        (self.bits[idx1 / 64] & (1u64 << (idx1 % 64))) != 0
            && (self.bits[idx2 / 64] & (1u64 << (idx2 % 64))) != 0
            && (self.bits[idx3 / 64] & (1u64 << (idx3 % 64))) != 0
    }

    #[inline]
    fn bit_positions(&self, hash: u64) -> (usize, usize, usize) {
        let h1 = hash;
        let h2 = hash.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
        let h3 = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9).rotate_left(31);
        (
            (h1 as usize) & self.mask,
            (h2 as usize) & self.mask,
            (h3 as usize) & self.mask,
        )
    }
}

// ─── Grace Hash Join ─────────────────────────────────────────────────────

/// Symmetric Hybrid Hash Join operator.
pub struct HashJoinOp<R: Read, W: Write> {
    partitions_left: Vec<Option<Run>>,
    partitions_right: Vec<Option<Run>>,
    left_types: Vec<DataType>,
    right_types: Vec<DataType>,
    left_col_idx: usize,
    right_col_idx: usize,

    current_partition: usize,
    build_is_left: bool,
    build_side: BuildSide,
    probe_reader: Option<RunReader>,

    current_probe_row: Option<Row>,
    current_match_indices: Vec<i32>,
    current_match_idx: usize,
    current_probe_filter: Option<JoinFilter>,
    build_key_idx: usize,
    probe_key_idx: usize,

    in_memory_mode: bool,
    probe_child: Option<Box<dyn Operator<R, W>>>,
    probe_buffered: Vec<Row>,
    pool_ptr: *mut BufferPool<R, W>,

    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    _marker: std::marker::PhantomData<(R, W)>,
}

// ─── Estimator Helper ────────────────────────────────────────────────────

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

// ─── Hashing Helper ──────────────────────────────────────────────────────

#[inline]
fn mix64(mut z: u64) -> u64 {
    z ^= z >> 30;
    z = z.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z ^= z >> 27;
    z = z.wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[inline]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[inline]
pub fn hash_data(val: &Data) -> u64 {
    match val {
        Data::Int32(v) => mix64((*v as u32 as u64) ^ 0x1111_1111_1111_1111),
        Data::Int64(v) => mix64((*v as u64) ^ 0x2222_2222_2222_2222),
        Data::Float32(v) => mix64((v.to_bits() as u64) ^ 0x3333_3333_3333_3333),
        Data::Float64(v) => mix64(v.to_bits() ^ 0x4444_4444_4444_4444),
        Data::String(v) => mix64(fnv1a64(v.as_bytes()) ^ 0x5555_5555_5555_5555),
    }
}

#[inline]
fn combine_rows(left_vals: &[Data], right_vals: &[Data]) -> Vec<Data> {
    let mut values = Vec::with_capacity(left_vals.len() + right_vals.len());
    values.extend(left_vals.iter().cloned());
    values.extend(right_vals.iter().cloned());
    values
}

/// Build a combined row when the probe-side row is owned (last match for that
/// probe row).  Saves cloning the probe row's `Vec<Data>` and its inner values.
#[inline]
fn combine_rows_probe_owned(
    build_vals: &[Data],
    probe_owned: Vec<Data>,
    build_is_left: bool,
) -> Vec<Data> {
    if build_is_left {
        let mut values = Vec::with_capacity(build_vals.len() + probe_owned.len());
        values.extend(build_vals.iter().cloned());
        values.extend(probe_owned);
        values
    } else {
        let mut values = probe_owned;
        values.reserve(build_vals.len());
        values.extend(build_vals.iter().cloned());
        values
    }
}

// ─── Partition Phase ─────────────────────────────────────────────────────

/// Per-partition encoder.  Buffers encoded row bytes directly instead of
/// `Vec<Row>` (which carries Data-enum + Vec metadata overhead, ~7× bigger
/// than the on-disk encoded form).  This lets us flush far fewer, much
/// larger byte runs to the disk simulator.
struct PartitionWriter {
    /// The block currently being filled (length = block_size, last 2 bytes
    /// reserved for the row-count tag).
    current_block: Vec<u8>,
    current_offset: usize,
    current_row_count: u16,
    /// Completed-block bytes awaiting a single contiguous flush.
    pending: Vec<u8>,
    pending_block_count: usize,
    /// Block IDs that have already been written to disk for this partition.
    block_ids: Vec<u64>,
    total_rows: usize,
}

impl PartitionWriter {
    fn new(block_size: usize) -> Self {
        PartitionWriter {
            current_block: vec![0u8; block_size],
            current_offset: 0,
            current_row_count: 0,
            pending: Vec::new(),
            pending_block_count: 0,
            block_ids: Vec::new(),
            total_rows: 0,
        }
    }

    #[inline]
    fn finalize_current_block(&mut self, block_size: usize) {
        let count_bytes = self.current_row_count.to_le_bytes();
        let tail = block_size - 2;
        self.current_block[tail..tail + 2].copy_from_slice(&count_bytes);
        self.pending.extend_from_slice(&self.current_block);
        self.pending_block_count += 1;
        // Reset logical contents only. Decoders read exactly `row_count` rows
        // and ignore padding before the count tag, so clearing the full block
        // only burns CPU during large spill-heavy joins.
        self.current_offset = 0;
        self.current_row_count = 0;
    }

    fn flush_pending<R: Read, W: Write>(&mut self, buffer_pool: &mut BufferPool<R, W>) {
        if self.pending_block_count == 0 {
            return;
        }
        let new_ids = buffer_pool.write_raw_run_blocks(&self.pending, self.pending_block_count);
        self.block_ids.extend(new_ids);
        self.pending.clear();
        self.pending_block_count = 0;
    }

    fn finish<R: Read, W: Write>(&mut self, block_size: usize, buffer_pool: &mut BufferPool<R, W>) {
        if self.current_row_count > 0 {
            self.finalize_current_block(block_size);
        }
        self.flush_pending(buffer_pool);
    }
}

fn partition_input<R: Read, W: Write>(
    input: &mut Box<dyn Operator<R, W>>,
    join_col_idx: usize,
    num_partitions: usize,
    buffer_pool: &mut BufferPool<R, W>,
    initial_rows: Vec<Row>,
    _estimated_row_size: usize,
    memory_budget: usize,
    mut join_filter: Option<&mut JoinFilter>,
) -> Vec<Option<Run>> {
    let block_size = buffer_pool.block_size();
    let usable_space = block_size - 2;
    let partition_mask = if num_partitions.is_power_of_two() {
        Some(num_partitions - 1)
    } else {
        None
    };

    // Per-partition pending byte threshold.  Total resident ≈
    // num_partitions * per_partition_flush_bytes, kept below memory_budget
    // (with headroom for the per-partition `current_block` and bookkeeping).
    let per_partition_flush_bytes =
        (memory_budget / (num_partitions + 4).max(1)).clamp(64 * 1024, 1 * 1024 * 1024);

    let mut writers: Vec<PartitionWriter> = (0..num_partitions)
        .map(|_| PartitionWriter::new(block_size))
        .collect();

    let mut initial_iter = initial_rows.into_iter();

    loop {
        let row_opt = if let Some(r) = initial_iter.next() {
            Some(r)
        } else {
            input.next(buffer_pool)
        };

        let row = match row_opt {
            Some(r) => r,
            None => break,
        };

        let h = hash_data(&row.values[join_col_idx]);
        if let Some(filter) = join_filter.as_deref_mut() {
            filter.insert_hash(h);
        }
        let bucket_id = match partition_mask {
            Some(mask) => (h as usize) & mask,
            None => (h as usize) % num_partitions,
        };

        let row_size = encoded_row_size(&row);
        debug_assert!(
            row_size <= usable_space,
            "Row of {} bytes exceeds block usable space {}",
            row_size,
            usable_space
        );
        let writer = &mut writers[bucket_id];

        if writer.current_offset + row_size > usable_space {
            // Roll over to the next block.
            writer.finalize_current_block(block_size);
            // Flush this partition if its pending buffer has hit the threshold.
            if writer.pending.len() >= per_partition_flush_bytes {
                writer.flush_pending(buffer_pool);
            }
        }

        write_row_into(
            &row,
            &mut writer.current_block[writer.current_offset..writer.current_offset + row_size],
        );
        writer.current_offset += row_size;
        writer.current_row_count += 1;
        writer.total_rows += 1;
    }

    let mut runs: Vec<Option<Run>> = Vec::with_capacity(num_partitions);
    for mut writer in writers.into_iter() {
        writer.finish(block_size, buffer_pool);
        if writer.total_rows == 0 {
            runs.push(None);
        } else {
            runs.push(Some(Run {
                block_ids: writer.block_ids,
                num_rows: writer.total_rows,
            }));
        }
    }
    runs
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
        memory_budget: usize,
        dynamic_filter_initializer: Option<std::sync::Arc<std::sync::Mutex<Option<JoinFilter>>>>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        // Symmetric Hybrid Hash logic
        let mut left_buffered = Vec::with_capacity(2048);
        let mut right_buffered = Vec::with_capacity(2048);
        let mut total_size = 0;
        // Use the caller-supplied budget instead of a hardcoded limit.
        // This budget is shared with the sort operator so peak memory
        // stays within RLIMIT_AS (buffer_pool + sort/hash budget + headroom ≤ 64 MB).
        let limit = memory_budget;
        let left_row_size = estimate_row_size(&left_types);
        let right_row_size = estimate_row_size(&right_types);

        let mut left_exhausted = false;
        let mut right_exhausted = false;
        let mut exceeded = false;
        let avg_row_size = ((left_row_size + right_row_size) / 2).max(1);
        let chunk_size = (memory_budget / (avg_row_size * 8)).clamp(256, 4096);

        loop {
            // Read next chunk from left
            for _ in 0..chunk_size {
                if let Some(row) = left.next(buffer_pool) {
                    left_buffered.push(row);
                    total_size += left_row_size;
                } else {
                    left_exhausted = true;
                    break;
                }
            }
            if left_exhausted || total_size > limit {
                if total_size > limit {
                    exceeded = true;
                }
                break;
            }

            // Read next chunk from right
            for _ in 0..chunk_size {
                if let Some(row) = right.next(buffer_pool) {
                    right_buffered.push(row);
                    total_size += right_row_size;
                } else {
                    right_exhausted = true;
                    break;
                }
            }
            if right_exhausted || total_size > limit {
                if total_size > limit {
                    exceeded = true;
                }
                break;
            }
        }

        if !exceeded {
            let build_is_left = if left_exhausted && right_exhausted {
                left_buffered.len() <= right_buffered.len()
            } else {
                left_exhausted
            };
            let (build_buf, mut probe_buf, probe_child, build_idx) = if build_is_left {
                (left_buffered, right_buffered, right, left_col_idx)
            } else {
                (right_buffered, left_buffered, left, right_col_idx)
            };

            let mut build_side = BuildSide::with_capacity(build_buf.len().max(64));
            let mut join_filter = JoinFilter::new(DEFAULT_JOIN_FILTER_BITS);
            let mut probe_skip_filter = JoinFilter::new(PROBE_SKIP_FILTER_BITS);
            for row in build_buf {
                let h = hash_data(&row.values[build_idx]);
                build_side.insert(h, row);
                join_filter.insert_hash(h);
                probe_skip_filter.insert_hash(h);
            }

            if let Some(ref lock) = dynamic_filter_initializer {
                if let Ok(mut guard) = lock.lock() {
                    *guard = Some(join_filter);
                }
            }

            // Reversing the buffer allows O(1) pops that fetch elements in correct chronological order
            probe_buf.reverse();

            return HashJoinOp {
                partitions_left: Vec::new(),
                partitions_right: Vec::new(),
                left_types,
                right_types,
                left_col_idx,
                right_col_idx,
                current_partition: 0,
                build_is_left,
                build_side,
                probe_reader: None,
                current_probe_row: None,
                current_match_indices: Vec::new(),
                current_match_idx: 0,
                current_probe_filter: Some(probe_skip_filter),
                build_key_idx: build_idx,
                probe_key_idx: if build_is_left {
                    right_col_idx
                } else {
                    left_col_idx
                },
                in_memory_mode: true,
                probe_child: Some(probe_child),
                probe_buffered: probe_buf,
                pool_ptr: buffer_pool as *mut BufferPool<R, W>,
                output_schema,
                output_types,
                _marker: std::marker::PhantomData,
            };
        }

        // Partition count proportional to budgeted memory: more partitions for smaller
        // memory budgets keeps per-partition build tables small and predictable.
        let num_partitions = if memory_budget <= 4 * 1024 * 1024 {
            128
        } else {
            64
        };
        let mut spill_filter = dynamic_filter_initializer
            .as_ref()
            .map(|_| JoinFilter::new(DEFAULT_JOIN_FILTER_BITS));

        let partitions_left = partition_input(
            &mut left,
            left_col_idx,
            num_partitions,
            buffer_pool,
            left_buffered,
            left_row_size,
            memory_budget,
            spill_filter.as_mut(),
        );
        if let (Some(lock), Some(filter)) = (dynamic_filter_initializer.as_ref(), spill_filter) {
            if let Ok(mut guard) = lock.lock() {
                *guard = Some(filter);
            }
        }
        let partitions_right = partition_input(
            &mut right,
            right_col_idx,
            num_partitions,
            buffer_pool,
            right_buffered,
            right_row_size,
            memory_budget,
            None,
        );

        HashJoinOp {
            partitions_left,
            partitions_right,
            left_types,
            right_types,
            left_col_idx,
            right_col_idx,
            current_partition: 0,
            build_is_left: true, // For disk mode, partition by partition matching handles optimization natively
            build_side: BuildSide::new(),
            probe_reader: None,
            current_probe_row: None,
            current_match_indices: Vec::new(),
            current_match_idx: 0,
            current_probe_filter: None,
            build_key_idx: left_col_idx,
            probe_key_idx: right_col_idx,
            in_memory_mode: false,
            probe_child: None,
            probe_buffered: Vec::new(),
            pool_ptr: buffer_pool as *mut BufferPool<R, W>,
            output_schema,
            output_types,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R: Read, W: Write> Drop for HashJoinOp<R, W> {
    fn drop(&mut self) {
        if self.pool_ptr.is_null() {
            return;
        }
        // SAFETY: pool_ptr is captured from the active buffer pool during
        // construction and operators are dropped before pool teardown.
        let pool = unsafe { &mut *self.pool_ptr };

        for part in &mut self.partitions_left {
            if let Some(run) = part {
                if !run.block_ids.is_empty() {
                    pool.free_run(run);
                    run.block_ids.clear();
                }
            }
        }
        for part in &mut self.partitions_right {
            if let Some(run) = part {
                if !run.block_ids.is_empty() {
                    pool.free_run(run);
                    run.block_ids.clear();
                }
            }
        }
        if let Some(reader) = &mut self.probe_reader {
            if !reader.run.block_ids.is_empty() {
                pool.free_run(&reader.run);
                reader.run.block_ids.clear();
            }
        }
    }
}

// ─── Operator trait ──────────────────────────────────────────────────────

impl<R: Read, W: Write> Operator<R, W> for HashJoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            // 1. Yield matched build rows one by one (no per-yield HashMap lookup;
            //    indices point straight into the flat `BuildSide::rows` Vec).
            if self.current_match_idx < self.current_match_indices.len() {
                let build_idx = self.current_match_indices[self.current_match_idx];
                let build_row = self.build_side.row(build_idx);
                self.current_match_idx += 1;
                let is_last = self.current_match_idx == self.current_match_indices.len();

                let combined = if is_last {
                    // Last match for this probe row — move its values out instead of cloning.
                    let probe_owned = self.current_probe_row.take().unwrap().values;
                    combine_rows_probe_owned(&build_row.values, probe_owned, self.build_is_left)
                } else {
                    let probe_row = self.current_probe_row.as_ref().unwrap();
                    if self.build_is_left {
                        combine_rows(&build_row.values, &probe_row.values)
                    } else {
                        combine_rows(&probe_row.values, &build_row.values)
                    }
                };
                return Some(Row { values: combined });
            }

            // 1.5. In-Memory Mode Probe
            if self.in_memory_mode {
                let next_probe_row = if let Some(r) = self.probe_buffered.pop() {
                    Some(r)
                } else {
                    let child = self.probe_child.as_mut().unwrap();
                    child.next(pool)
                };

                if let Some(probe_row) = next_probe_row {
                    self.current_match_indices.clear();
                    self.current_match_idx = 0;

                    let probe_val = &probe_row.values[self.probe_key_idx];
                    let h = hash_data(probe_val);
                    if let Some(filter) = &self.current_probe_filter {
                        if !filter.might_contain_hash(h) {
                            self.current_probe_row = Some(probe_row);
                            continue;
                        }
                    }
                    let mut idx = self.build_side.first(h);
                    while idx >= 0 {
                        let build_row = self.build_side.row(idx);
                        if &build_row.values[self.build_key_idx] == probe_val {
                            self.current_match_indices.push(idx);
                        }
                        idx = self.build_side.next_index(idx);
                    }
                    if self.current_match_indices.is_empty() {
                        continue;
                    }
                    self.current_probe_row = Some(probe_row);
                    continue;
                } else {
                    return None;
                }
            }

            // 2. Scan probe partition for next matching row (Disk Mode)
            if let Some(reader) = &mut self.probe_reader {
                if let Some(probe_row) = reader.next_owned(pool) {
                    self.current_match_indices.clear();
                    self.current_match_idx = 0;

                    let probe_val = &probe_row.values[self.probe_key_idx];
                    let h = hash_data(probe_val);
                    if let Some(filter) = &self.current_probe_filter {
                        if !filter.might_contain_hash(h) {
                            self.current_probe_row = Some(probe_row);
                            continue;
                        }
                    }
                    let mut idx = self.build_side.first(h);
                    while idx >= 0 {
                        let build_row = self.build_side.row(idx);
                        if &build_row.values[self.build_key_idx] == probe_val {
                            self.current_match_indices.push(idx);
                        }
                        idx = self.build_side.next_index(idx);
                    }
                    if self.current_match_indices.is_empty() {
                        continue;
                    }
                    self.current_probe_row = Some(probe_row);
                    continue;
                } else {
                    if let Some(reader) = self.probe_reader.take() {
                        pool.free_run(&reader.run);
                    }
                }
            }

            // 3. Move to next partition pair (Disk Mode)
            if self.current_partition >= self.partitions_left.len() {
                return None;
            }

            let p_idx = self.current_partition;
            self.current_partition += 1;

            match (&self.partitions_left[p_idx], &self.partitions_right[p_idx]) {
                (Some(left_part), Some(right_part)) => {
                    self.build_side.clear();
                    let mut probe_skip_filter = JoinFilter::new(PROBE_SKIP_FILTER_BITS);

                    let (
                        build_is_left,
                        build_run,
                        probe_run,
                        build_types,
                        probe_types,
                        build_col_idx,
                    ) = if left_part.num_rows <= right_part.num_rows {
                        (
                            true,
                            left_part,
                            right_part,
                            self.left_types.clone(),
                            self.right_types.clone(),
                            self.left_col_idx,
                        )
                    } else {
                        (
                            false,
                            right_part,
                            left_part,
                            self.right_types.clone(),
                            self.left_types.clone(),
                            self.right_col_idx,
                        )
                    };
                    self.build_is_left = build_is_left;
                    self.build_key_idx = build_col_idx;
                    self.probe_key_idx = if build_is_left {
                        self.right_col_idx
                    } else {
                        self.left_col_idx
                    };
                    let needed_cap = build_run.num_rows.saturating_mul(100).div_ceil(70);
                    if self.build_side.len() < needed_cap {
                        self.build_side.reserve(needed_cap - self.build_side.len());
                    }

                    let mut build_reader = RunReader::new(build_run, build_types, pool);
                    while let Some(row) = build_reader.next_owned(pool) {
                        let h = hash_data(&row.values[build_col_idx]);
                        probe_skip_filter.insert_hash(h);
                        self.build_side.insert(h, row);
                    }
                    pool.free_run(build_run);
                    self.current_probe_filter = Some(probe_skip_filter);

                    self.probe_reader = Some(RunReader::new(probe_run, probe_types, pool));
                }
                (Some(part), None) | (None, Some(part)) => {
                    pool.free_run(part);
                    self.current_probe_filter = None;
                }
                (None, None) => {}
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
