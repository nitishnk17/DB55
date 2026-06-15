use crate::buffer_pool::BufferPool;
use crate::row::{Row, build_needed_mask, decode_block_with_mask_into};
use common::{Data, DataType};
use std::io::{Read, Write};

// ─── Run Management ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Run {
    pub block_ids: Vec<u64>,
    pub num_rows: usize,
}

/// Convert rows into block-formatted byte buffers ready for disk writes.
///
/// Each block has `block_size - 2` usable bytes for row data, with the last
/// 2 bytes storing the row count (u16 LE).  A row must fit within a single
/// block.
pub fn rows_to_blocks(rows: &[Row], block_size: usize) -> Vec<Vec<u8>> {
    let raw = rows_to_run_buffer(rows, block_size);
    let num_blocks = raw.len() / block_size;
    let mut out = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        out.push(raw[i * block_size..(i + 1) * block_size].to_vec());
    }
    out
}

/// Pack `rows` directly into one contiguous run buffer, saving the per-block
/// `Vec<u8>` allocations that `rows_to_blocks` would otherwise create.
/// Returns `Vec<u8>` of length `num_blocks * block_size`.
pub fn rows_to_run_buffer(rows: &[Row], block_size: usize) -> Vec<u8> {
    let usable_space = block_size - 2;
    if rows.is_empty() {
        return Vec::new();
    }

    // Estimate block count to size the destination Vec once.
    let mut est_blocks = 1usize;
    let mut est_offset = 0usize;
    let mut row_sizes = Vec::with_capacity(rows.len());
    for row in rows {
        let rs = encoded_row_size(row);
        row_sizes.push(rs);
        if est_offset + rs > usable_space {
            est_blocks += 1;
            est_offset = 0;
        }
        est_offset += rs;
    }

    let mut buffer = vec![0u8; est_blocks * block_size];
    let mut block_idx = 0usize;
    let mut offset = 0usize;
    let mut row_count: u16 = 0;

    for (row, &row_size) in rows.iter().zip(row_sizes.iter()) {
        assert!(
            row_size <= usable_space,
            "Row of {} bytes exceeds block usable space of {} bytes (block_size={})",
            row_size,
            usable_space,
            block_size
        );

        if offset + row_size > usable_space {
            // Finalize current block.
            let block_start = block_idx * block_size;
            buffer[block_start + block_size - 2..block_start + block_size]
                .copy_from_slice(&row_count.to_le_bytes());
            block_idx += 1;
            offset = 0;
            row_count = 0;

            // Grow buffer if our estimate was tight (rare — should not happen).
            if (block_idx + 1) * block_size > buffer.len() {
                buffer.resize((block_idx + 1) * block_size, 0u8);
            }
        }

        let dst_start = block_idx * block_size + offset;
        write_row_into(row, &mut buffer[dst_start..dst_start + row_size]);
        offset += row_size;
        row_count += 1;
    }

    // Finalize the last block.
    let block_start = block_idx * block_size;
    buffer[block_start + block_size - 2..block_start + block_size]
        .copy_from_slice(&row_count.to_le_bytes());
    let total_blocks = block_idx + 1;

    if total_blocks * block_size != buffer.len() {
        buffer.truncate(total_blocks * block_size);
    }

    buffer
}

pub(crate) fn encoded_row_size(row: &Row) -> usize {
    row.values
        .iter()
        .map(|value| match value {
            Data::Int32(_) => 4,
            Data::Int64(_) => 8,
            Data::Float32(_) => 4,
            Data::Float64(_) => 8,
            Data::String(v) => v.len() + 1,
        })
        .sum()
}

pub(crate) fn write_row_into(row: &Row, dst: &mut [u8]) {
    let mut offset = 0usize;
    for value in &row.values {
        match value {
            Data::Int32(v) => {
                dst[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
                offset += 4;
            }
            Data::Int64(v) => {
                dst[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
                offset += 8;
            }
            Data::Float32(v) => {
                dst[offset..offset + 4].copy_from_slice(&v.to_le_bytes());
                offset += 4;
            }
            Data::Float64(v) => {
                dst[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
                offset += 8;
            }
            Data::String(v) => {
                let bytes = v.as_bytes();
                dst[offset..offset + bytes.len()].copy_from_slice(bytes);
                dst[offset + bytes.len()] = 0;
                offset += bytes.len() + 1;
            }
        }
    }
}

// ─── Run Reader ──────────────────────────────────────────────────────────

pub struct RunReader {
    pub run: Run,
    pub current_block_idx: usize,
    pub current_row_idx: usize,
    pub current_block_rows: Vec<Row>,
    pub types: Vec<DataType>,
    needed_mask: Vec<bool>,
    block_size: usize,
    pub exhausted: bool,
    prefetch_blocks: usize,
    prefetched_start_idx: usize,
    prefetched_count: usize,
    prefetched_raw: Vec<u8>,
}

impl RunReader {
    pub fn new(
        run: &Run,
        types: Vec<DataType>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        // Default prefetch — the pool no longer owns per-frame data so the
        // transient Vec<u8> for prefetched bytes is the only RAM cost here.
        Self::new_with_prefetch(run, types, buffer_pool, 128)
    }

    pub fn new_with_prefetch(
        run: &Run,
        types: Vec<DataType>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        prefetch_blocks: usize,
    ) -> Self {
        let needed_mask = build_needed_mask(types.len(), &(0..types.len()).collect::<Vec<usize>>());
        let mut reader = RunReader {
            run: run.clone(),
            current_block_idx: 0,
            current_row_idx: 0,
            current_block_rows: Vec::new(),
            types,
            needed_mask,
            block_size: buffer_pool.block_size(),
            exhausted: run.num_rows == 0,
            prefetch_blocks: prefetch_blocks.max(1),
            prefetched_start_idx: 0,
            prefetched_count: 0,
            prefetched_raw: Vec::new(),
        };

        if !reader.exhausted {
            reader.load_prefetch_batch(0, buffer_pool);
            let end = reader.block_size;
            if end <= reader.prefetched_raw.len() {
                let block_data = &reader.prefetched_raw[..end];
                decode_block_with_mask_into(
                    block_data,
                    &reader.types,
                    &reader.needed_mask,
                    &mut reader.current_block_rows,
                );
            }
        }

        reader
    }

    pub fn peek(&self) -> Option<&Row> {
        if self.exhausted {
            return None;
        }
        self.current_block_rows.get(self.current_row_idx)
    }

    pub fn next_owned(
        &mut self,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Option<Row> {
        if self.exhausted || self.current_row_idx >= self.current_block_rows.len() {
            return None;
        }
        let row = std::mem::replace(
            &mut self.current_block_rows[self.current_row_idx],
            Row { values: Vec::new() },
        );
        self.advance(buffer_pool);
        Some(row)
    }

    pub fn advance(&mut self, buffer_pool: &mut BufferPool<impl Read, impl Write>) {
        self.current_row_idx += 1;
        if self.current_row_idx >= self.current_block_rows.len() {
            self.current_block_idx += 1;
            if self.current_block_idx >= self.run.block_ids.len() {
                self.exhausted = true;
                return;
            }
            if self.current_block_idx < self.prefetched_start_idx
                || self.current_block_idx >= self.prefetched_start_idx + self.prefetched_count
            {
                self.load_prefetch_batch(self.current_block_idx, buffer_pool);
            }
            let prefetch_idx = self.current_block_idx - self.prefetched_start_idx;
            let begin = prefetch_idx * self.block_size;
            let end = begin + self.block_size;
            if end <= self.prefetched_raw.len() {
                self.current_block_rows.clear();
                let block_data = &self.prefetched_raw[begin..end];
                decode_block_with_mask_into(
                    block_data,
                    &self.types,
                    &self.needed_mask,
                    &mut self.current_block_rows,
                );
            } else {
                self.current_block_rows.clear();
                self.exhausted = true;
                return;
            }
            self.current_row_idx = 0;
        }
    }

    fn load_prefetch_batch(
        &mut self,
        start_idx: usize,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) {
        self.prefetched_start_idx = start_idx;
        self.prefetched_count = 0;
        self.prefetched_raw.clear();

        if start_idx >= self.run.block_ids.len() {
            return;
        }

        let start_block_id = self.run.block_ids[start_idx];
        let mut count = 1usize;
        while start_idx + count < self.run.block_ids.len() && count < self.prefetch_blocks {
            let prev = self.run.block_ids[start_idx + count - 1];
            let next = self.run.block_ids[start_idx + count];
            if next != prev + 1 {
                break;
            }
            count += 1;
        }

        self.prefetched_raw = buffer_pool.read_blocks_sequential(start_block_id, count as u64);
        self.prefetched_count = count;
    }

    fn prefetched_block_slice(&self, prefetch_idx: usize) -> Option<&[u8]> {
        if prefetch_idx >= self.prefetched_count {
            return None;
        }
        let begin = prefetch_idx * self.block_size;
        let end = begin + self.block_size;
        self.prefetched_raw.get(begin..end)
    }
}
