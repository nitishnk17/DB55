use std::collections::VecDeque;
use std::io::{Read, Write};
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::row::{decode_block, encode_row, Row};

// ─── Run Management ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Run {
    pub block_ids: Vec<u64>,
    pub num_rows: usize,
}

/// Convert rows into block-formatted byte buffers ready for disk writes.
pub fn rows_to_blocks(rows: &[Row], block_size: usize) -> Vec<Vec<u8>> {
    let usable_space = block_size - 2;
    let mut blocks = Vec::new();
    let mut current_block = vec![0u8; block_size];
    let mut offset = 0;
    let mut row_count: u16 = 0;

    for row in rows {
        let encoded = encode_row(row);

        assert!(
            encoded.len() <= usable_space,
            "Row of {} bytes exceeds block usable space of {} bytes (block_size={}). \
             This typically means a join produced a row wider than one block.",
            encoded.len(), usable_space, block_size
        );

        if offset + encoded.len() > usable_space {
            if row_count > 0 {
                current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
                blocks.push(current_block);
            }
            current_block = vec![0u8; block_size];
            offset = 0;
            row_count = 0;
        }

        current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count += 1;
    }

    if row_count > 0 {
        current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
        blocks.push(current_block);
    }

    blocks
}

// ─── Run Reader ──────────────────────────────────────────────────────────

/// Number of blocks to read per I/O call in RunReader.
///
/// Batching dramatically reduces disk calls during K-way merges and hash-join
/// partition reads. With blocks typically being 4 KB, 64 blocks = 256 KB per
/// read. In a 128-way merge, this cuts individual disk operations from O(total_rows)
/// to O(total_rows / rows_per_block / 64) — roughly a 64× reduction in I/O calls.
const RUN_BATCH_BLOCKS: usize = 64;

pub struct RunReader {
    pub run: Run,
    block_cursor: usize,        // index into run.block_ids for the next batch to read
    row_buffer: VecDeque<Row>,  // decoded rows ready to hand out via peek/advance
    pub types: Vec<DataType>,
}

impl RunReader {
    pub fn new(
        run: &Run,
        types: Vec<DataType>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        let mut reader = RunReader {
            run: run.clone(),
            block_cursor: 0,
            row_buffer: VecDeque::new(),
            types,
        };
        // Pre-load the first batch so peek() works immediately.
        if !run.block_ids.is_empty() {
            reader.fill_buffer(buffer_pool);
        }
        reader
    }

    /// Read the next batch of up to RUN_BATCH_BLOCKS blocks into row_buffer.
    ///
    /// When the blocks are contiguous (the common case — the linear allocator
    /// always produces contiguous IDs within a single run), a single
    /// read_blocks_sequential call is issued. Otherwise we fall back to
    /// individual reads.
    fn fill_buffer(&mut self, buffer_pool: &mut BufferPool<impl Read, impl Write>) {
        let remaining = self.run.block_ids.len() - self.block_cursor;
        if remaining == 0 {
            return;
        }
        let batch = remaining.min(RUN_BATCH_BLOCKS);
        let block_size = buffer_pool.block_size();

        let start_id = self.run.block_ids[self.block_cursor];
        // The linear allocator always hands out consecutive IDs, so this check
        // passes virtually 100% of the time.
        let contiguous = (0..batch).all(|i| {
            self.run.block_ids[self.block_cursor + i] == start_id + i as u64
        });

        if contiguous {
            // One I/O call for the whole batch.
            let data = buffer_pool.read_blocks_sequential(start_id, batch as u64);
            for i in 0..batch {
                let begin = i * block_size;
                let end = begin + block_size;
                let rows = decode_block(&data[begin..end], &self.types);
                self.row_buffer.extend(rows);
            }
        } else {
            // Rare non-contiguous case: fall back to per-block reads.
            for i in 0..batch {
                let bid = self.run.block_ids[self.block_cursor + i];
                let data = buffer_pool.read_blocks_sequential(bid, 1);
                let rows = decode_block(&data, &self.types);
                self.row_buffer.extend(rows);
            }
        }
        self.block_cursor += batch;
    }

    pub fn peek(&self) -> Option<&Row> {
        self.row_buffer.front()
    }

    pub fn advance(&mut self, buffer_pool: &mut BufferPool<impl Read, impl Write>) {
        self.row_buffer.pop_front();
        // Refill when the buffer drains and there are still blocks to read.
        if self.row_buffer.is_empty() && self.block_cursor < self.run.block_ids.len() {
            self.fill_buffer(buffer_pool);
        }
    }
}
