use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;
use crate::row::{decode_block, encode_row, Row};
use db_config::table::ColumnSpec;

// ─── Run Management ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Run {
    pub start_block: u64,
    pub num_blocks: u64,
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

        if offset + encoded.len() > usable_space {
            // Finalize current block
            current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
            blocks.push(current_block);
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
    pub start_block: u64,
    pub num_blocks: u64,
    pub current_block_idx: u64,
    pub current_row_idx: usize,
    pub current_block_rows: Vec<Row>,
    pub schema: Vec<ColumnSpec>,
    pub exhausted: bool,
}

impl RunReader {
    pub fn new(
        run: &Run,
        schema: Vec<ColumnSpec>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        let block_data = buffer_pool.fetch_block(run.start_block);
        buffer_pool.unpin(run.start_block);
        let rows = decode_block(&block_data, &schema);

        RunReader {
            start_block: run.start_block,
            num_blocks: run.num_blocks,
            current_block_idx: 0,
            current_row_idx: 0,
            current_block_rows: rows,
            schema,
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
            if self.current_block_idx >= self.num_blocks {
                self.exhausted = true;
                return;
            }
            let block_id = self.start_block + self.current_block_idx;
            let block_data = buffer_pool.fetch_block(block_id);
            buffer_pool.unpin(block_id);
            self.current_block_rows = decode_block(&block_data, &self.schema);
            self.current_row_idx = 0;
        }
    }
}
