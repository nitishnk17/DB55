use std::io::{Read, Write};
use db_config::table::ColumnSpec;
use crate::buffer_pool::BufferPool;
use crate::row::{Row, decode_block};
use crate::operator::Operator;

/// Number of blocks to read in a single disk call during a sequential scan.
///
/// Larger batches mean fewer round-trips to the disk simulator (each of which
/// incurs seek + rotational-latency overhead), but more peak memory usage.
/// 128 blocks × 4 KB = 512 KB per batch — reduces round-trips by 4× vs 32.
const SCAN_BATCH_SIZE: u64 = 128;

pub struct TableScanner {
    column_names: Vec<String>,
    all_rows: Vec<Row>,
    current_index: usize,
}

impl TableScanner {
    pub fn new(
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        file_id: &str,
        column_specs: Vec<ColumnSpec>,
    ) -> Self {
        // Query file metadata
        let start_block = buffer_pool.get_file_start_block(file_id);
        let num_blocks  = buffer_pool.get_file_num_blocks(file_id);
        let block_size  = buffer_pool.block_size();

        eprintln!(
            "TableScanner: '{}' → start_block={}, num_blocks={}",
            file_id, start_block, num_blocks
        );

        // ── Batch sequential reads ────────────────────────────────────────
        //
        // Instead of fetching one block at a time through the LRU cache (which
        // would cause sequential flooding for large tables), we issue multi-block
        // read commands directly.  This:
        //   1. Reduces the number of disk protocol round-trips from N to N/BATCH.
        //   2. Avoids polluting the LRU cache with pages that won't be revisited.
        //
        // The raw bytes are decoded into Row objects immediately; the byte buffer
        // is dropped after each batch so memory usage stays at O(batch) not O(N).
        //
        // Pre-allocate the row vector with an estimated capacity to avoid
        // repeated reallocation as we push rows.  We estimate ~60 bytes per
        // encoded row (conservative for TPC-H tables with several string cols).
        let estimated_rows = ((num_blocks as usize) * block_size / 60).max(64);
        let mut all_rows: Vec<Row> = Vec::with_capacity(estimated_rows);
        let mut block = start_block;

        while block < start_block + num_blocks {
            let count = SCAN_BATCH_SIZE.min(start_block + num_blocks - block);

            // One disk call for `count` blocks
            let raw = buffer_pool.read_blocks_sequential(block, count);

            // Decode each block from the raw byte slice
            for i in 0..count as usize {
                let begin = i * block_size;
                let end   = begin + block_size;
                let block_data = &raw[begin..end];
                let rows = decode_block(block_data, &column_specs);
                all_rows.extend(rows);
            }

            block += count;
        }

        eprintln!("TableScanner: '{}' → decoded {} rows", file_id, all_rows.len());

        // Extract column names for the schema
        let column_names = column_specs
            .iter()
            .map(|c| c.column_name.clone())
            .collect();

        TableScanner {
            column_names,
            all_rows,
            current_index: 0,
        }
    }
}

impl Operator for TableScanner {
    fn next(&mut self) -> Option<Row> {
        if self.current_index < self.all_rows.len() {
            let row = self.all_rows[self.current_index].clone();
            self.current_index += 1;
            Some(row)
        } else {
            None
        }
    }

    fn schema(&self) -> Vec<String> {
        self.column_names.clone()
    }
}
