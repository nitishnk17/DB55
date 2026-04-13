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
///
/// Each block has `block_size - 2` usable bytes for row data, with the last
/// 2 bytes storing the row count (u16 LE).  A row must fit within a single
/// block.  If a row is too large, this function panics with a descriptive
/// message (this should never happen for well-formed table data or join
/// results within the block-size guarantees of the assignment).
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
            // Finalize current block (only if it has rows — guards against empty flush)
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

pub struct RunReader {
    pub run: Run,
    pub current_block_idx: usize,
    pub current_row_idx: usize,
    pub current_block_rows: Vec<Row>,
    pub types: Vec<DataType>,
    pub exhausted: bool,
}

impl RunReader {
    pub fn new(
        run: &Run,
        types: Vec<DataType>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        let block_data = if run.block_ids.is_empty() {
            vec![]
        } else {
            let b = buffer_pool.fetch_block(run.block_ids[0]);
            buffer_pool.unpin(run.block_ids[0]);
            b
        };
        let rows = if run.block_ids.is_empty() { vec![] } else { decode_block(&block_data, &types) };

        RunReader {
            run: run.clone(),
            current_block_idx: 0,
            current_row_idx: 0,
            current_block_rows: rows,
            types,
            exhausted: run.num_rows == 0,
        }
    }

    pub fn peek(&self) -> Option<&Row> {
        if self.exhausted {
            return None;
        }
        self.current_block_rows.get(self.current_row_idx)
    }

    pub fn advance(&mut self, buffer_pool: &mut BufferPool<impl Read, impl Write>) {
        self.current_row_idx += 1;
        if self.current_row_idx >= self.current_block_rows.len() {
            self.current_block_idx += 1;
            if self.current_block_idx >= self.run.block_ids.len() {
                self.exhausted = true;
                return;
            }
            let block_id = self.run.block_ids[self.current_block_idx];
            let block_data = buffer_pool.fetch_block(block_id);
            buffer_pool.unpin(block_id);
            self.current_block_rows = decode_block(&block_data, &self.types);
            self.current_row_idx = 0;
        }
    }
}
