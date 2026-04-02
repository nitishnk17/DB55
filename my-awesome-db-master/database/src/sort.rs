use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{Read, Write};
use common::query::SortSpec;
use common::DataType;
use db_config::table::ColumnSpec;
use crate::buffer_pool::BufferPool;
use crate::disk_run::{rows_to_blocks, Run, RunReader};
use crate::operator::Operator;
use crate::row::{encode_row, Row};

// ─── SortOp ─────────────────────────────────────────────────────────────

pub struct SortOp {
    sorted_rows: Vec<Row>,
    current_index: usize,
    output_schema: Vec<String>,
}

impl Operator for SortOp {
    fn next(&mut self) -> Option<Row> {
        if self.current_index < self.sorted_rows.len() {
            let row = self.sorted_rows[self.current_index].clone();
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

impl SortOp {
    pub fn new(
        mut child: Box<dyn Operator>,
        sort_specs: Vec<SortSpec>,
        column_specs: Vec<ColumnSpec>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        let output_schema = child.schema();

        // Pre-compute sort key indices
        let col_index_map: HashMap<String, usize> = output_schema
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        let sort_keys: Vec<(usize, bool)> = sort_specs
            .iter()
            .map(|spec| {
                let idx = col_index_map[&spec.column_name];
                (idx, spec.ascending)
            })
            .collect();

        // Estimate memory budget
        let block_size = buffer_pool.block_size();
        let memory_budget_rows = estimate_memory_budget(block_size, &column_specs);

        // Materialize rows, switching to external sort if too many
        let mut all_rows = Vec::new();
        let mut exceeded = false;
        while let Some(row) = child.next() {
            all_rows.push(row);
            if all_rows.len() > memory_budget_rows {
                exceeded = true;
                break;
            }
        }

        if !exceeded {
            // All rows fit in memory — simple in-memory sort
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
            return SortOp {
                sorted_rows: all_rows,
                current_index: 0,
                output_schema,
            };
        }

        // External sort path: too many rows for memory
        eprintln!(
            "External sort: exceeded {} row budget, switching to disk-based sort",
            memory_budget_rows
        );

        let mut runs = Vec::new();

        // Process the rows we already have as the first (partial) run,
        // then continue reading from child for subsequent runs
        loop {
            // Sort current buffer
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));

            // Serialize and write to anonymous blocks
            let blocks = rows_to_blocks(&all_rows, block_size);
            let num_blocks = blocks.len() as u64;
            let start_block = buffer_pool.allocate_anon_blocks(num_blocks);
            for (i, block_data) in blocks.iter().enumerate() {
                buffer_pool.write_block(start_block + i as u64, block_data);
            }
            runs.push(Run {
                start_block,
                num_blocks,
                num_rows: all_rows.len(),
            });

            // Read next batch
            all_rows.clear();
            for _ in 0..memory_budget_rows {
                match child.next() {
                    Some(row) => all_rows.push(row),
                    None => break,
                }
            }
            if all_rows.is_empty() {
                break;
            }
        }

        // Multi-pass K-way merge controller
        let sorted_rows = merge_all_runs(
            runs,
            &sort_keys,
            &column_specs,
            buffer_pool,
            block_size,
        );

        SortOp {
            sorted_rows,
            current_index: 0,
            output_schema,
        }
    }
}

// ─── Row Comparison ──────────────────────────────────────────────────────

fn compare_rows(a: &Row, b: &Row, sort_keys: &[(usize, bool)]) -> Ordering {
    for &(col_idx, ascending) in sort_keys {
        let val_a = &a.values[col_idx];
        let val_b = &b.values[col_idx];
        let cmp = val_a.partial_cmp(val_b).unwrap_or(Ordering::Equal);
        match cmp {
            Ordering::Equal => continue,
            other => {
                return if ascending { other } else { other.reverse() };
            }
        }
    }
    Ordering::Equal
}

// ─── Memory Budget ───────────────────────────────────────────────────────

fn estimate_memory_budget(block_size: usize, column_specs: &[ColumnSpec]) -> usize {
    let fixed_size: usize = column_specs
        .iter()
        .map(|c| match c.data_type {
            DataType::Int32 => 4,
            DataType::Int64 => 8,
            DataType::Float32 => 4,
            DataType::Float64 => 8,
            DataType::String => 50,
        })
        .sum();

    let row_overhead = 24 + column_specs.len() * 32;
    let effective_row_size = fixed_size + row_overhead;

    // Use ~70% of available memory for sort buffer
    let available_memory = block_size * 1000;
    std::cmp::max(available_memory / effective_row_size, 100)
}

// Components moved to disk_run.rs

// ─── Heap Entry (min-heap via reversed Ord) ──────────────────────────────

struct HeapEntry {
    row: Row,
    run_index: usize,
    sort_keys: Vec<(usize, bool)>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        compare_rows(&self.row, &other.row, &self.sort_keys) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // REVERSE for min-heap: BinaryHeap is max-heap by default
        compare_rows(&other.row, &self.row, &self.sort_keys)
    }
}

// ─── K-Way Merge ─────────────────────────────────────────────────────────

fn merge_all_runs(
    mut runs: Vec<Run>,
    sort_keys: &[(usize, bool)],
    column_specs: &[ColumnSpec],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
    block_size: usize,
) -> Vec<Row> {
    // 128 is a conservative fanout for typical systems
    // In our tests, memory easily fits this without buffer pool contention
    let max_fanout = 128;

    while runs.len() > max_fanout {
        let mut next_pass_runs = Vec::new();

        for chunk in runs.chunks(max_fanout) {
            if chunk.len() == 1 {
                // Just carry the remaining run over if alone
                next_pass_runs.push(chunk[0].clone());
            } else {
                let merged_run = merge_k_runs_to_disk(
                    chunk,
                    sort_keys,
                    column_specs,
                    buffer_pool,
                    block_size,
                );
                next_pass_runs.push(merged_run);
            }
        }
        runs = next_pass_runs;
    }

    merge_k_runs_to_vec(&runs, sort_keys, column_specs, buffer_pool)
}

fn merge_k_runs_to_disk(
    runs: &[Run],
    sort_keys: &[(usize, bool)],
    column_specs: &[ColumnSpec],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
    block_size: usize,
) -> Run {
    let mut readers: Vec<RunReader> = runs
        .iter()
        .map(|run| RunReader::new(run, column_specs.to_vec(), buffer_pool))
        .collect();

    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.peek() {
            heap.push(HeapEntry {
                row: row.clone(),
                run_index: i,
                sort_keys: sort_keys.to_vec(),
            });
        }
    }

    let mut current_block = vec![0u8; block_size];
    let usable_space = block_size - 2;
    let mut offset = 0;
    let mut row_count_in_blk: u16 = 0;

    let start_block = buffer_pool.allocate_anon_blocks(0); // Peeking the next block ID
    let mut num_blocks = 0;
    let mut total_rows = 0;

    while let Some(entry) = heap.pop() {
        // Encode and write row buffer to disk
        let encoded = encode_row(&entry.row);
        if offset + encoded.len() > usable_space {
            current_block[block_size - 2..].copy_from_slice(&row_count_in_blk.to_le_bytes());
            
            let blk = buffer_pool.allocate_anon_blocks(1);
            buffer_pool.write_block(blk, &current_block);
            num_blocks += 1;

            current_block = vec![0u8; block_size];
            offset = 0;
            row_count_in_blk = 0;
        }

        current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count_in_blk += 1;
        total_rows += 1;

        // Advance reader
        let reader = &mut readers[entry.run_index];
        reader.advance(buffer_pool);

        if let Some(next_row) = reader.peek() {
            heap.push(HeapEntry {
                row: next_row.clone(),
                run_index: entry.run_index,
                sort_keys: sort_keys.to_vec(),
            });
        }
    }

    if row_count_in_blk > 0 {
        current_block[block_size - 2..].copy_from_slice(&row_count_in_blk.to_le_bytes());
        let blk = buffer_pool.allocate_anon_blocks(1);
        buffer_pool.write_block(blk, &current_block);
        num_blocks += 1;
    }

    Run {
        start_block,
        num_blocks,
        num_rows: total_rows,
    }
}

fn merge_k_runs_to_vec(
    runs: &[Run],
    sort_keys: &[(usize, bool)],
    column_specs: &[ColumnSpec],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
) -> Vec<Row> {
    let mut readers: Vec<RunReader> = runs
        .iter()
        .map(|run| RunReader::new(run, column_specs.to_vec(), buffer_pool))
        .collect();

    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.peek() {
            heap.push(HeapEntry {
                row: row.clone(),
                run_index: i,
                sort_keys: sort_keys.to_vec(),
            });
        }
    }

    let total_rows: usize = runs.iter().map(|r| r.num_rows).sum();
    let mut result = Vec::with_capacity(total_rows);

    while let Some(entry) = heap.pop() {
        result.push(entry.row);

        let reader = &mut readers[entry.run_index];
        reader.advance(buffer_pool);

        if let Some(next_row) = reader.peek() {
            heap.push(HeapEntry {
                row: next_row.clone(),
                run_index: entry.run_index,
                sort_keys: sort_keys.to_vec(),
            });
        }
    }

    result
}
