use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{Read, Write};
use std::sync::Arc;
use common::query::SortSpec;
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::disk_run::{rows_to_blocks, Run, RunReader};
use crate::operator::Operator;
use crate::row::{encode_row, Row};

// ─── SortOp ─────────────────────────────────────────────────────────────────

/// SortOp uses either in-memory sorted rows (small data) or a disk-backed
/// RunReader (large data after external merge sort) to stream results.
pub struct SortOp<R: Read, W: Write> {
    /// In-memory path: all rows sorted and stored here.
    sorted_rows: Vec<Row>,
    current_index: usize,
    /// External sort path: final merged run on disk, read via RunReader.
    final_run_reader: Option<RunReader>,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    _marker: std::marker::PhantomData<(R, W)>,
}

impl<R: Read, W: Write> Operator<R, W> for SortOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        // External sort path: stream from the final merged run on disk
        if let Some(reader) = &mut self.final_run_reader {
            if let Some(row) = reader.peek() {
                let row = row.clone();
                reader.advance(pool);
                return Some(row);
            }
            return None;
        }
        // In-memory path
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

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}

impl<R: Read, W: Write> SortOp<R, W> {
    /// Create a new SortOp.
    ///
    /// `sort_memory_bytes` is the total byte budget available for holding rows
    /// in memory during the run-generation phase.  Pass roughly 50 % of the
    /// process memory limit so we leave room for the buffer pool and other
    /// operators.
    pub fn new(
        mut child: Box<dyn Operator<R, W>>,
        sort_specs: &[SortSpec],
        data_types: Vec<DataType>,
        buffer_pool: &mut BufferPool<R, W>,
        sort_memory_bytes: usize,
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

        // Calculate how many rows fit in the memory budget
        let memory_budget_rows = estimate_memory_budget(sort_memory_bytes, &data_types);
        let block_size = buffer_pool.block_size();

        eprintln!(
            "Sort: memory budget = {} MB → {} rows per run",
            sort_memory_bytes / (1024 * 1024),
            memory_budget_rows
        );

        // Materialize rows, switching to external sort when budget is exceeded
        let mut all_rows = Vec::new();
        let mut exceeded = false;
        while let Some(row) = child.next(buffer_pool) {
            all_rows.push(row);
            if all_rows.len() > memory_budget_rows {
                exceeded = true;
                break;
            }
        }

        if !exceeded {
            // All rows fit in memory — simple in-memory sort
            eprintln!("Sort: {} rows fit in memory, using in-memory sort", all_rows.len());
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
            return SortOp {
                sorted_rows: all_rows,
                current_index: 0,
                final_run_reader: None,
                output_schema,
                output_types: data_types,
                _marker: std::marker::PhantomData,
            };
        }

        // External sort path
        eprintln!(
            "Sort: exceeded {} row budget, switching to external merge sort",
            memory_budget_rows
        );

        let mut runs: Vec<Run> = Vec::new();

        // Process the rows we already have as the first run, then continue
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
                block_ids: (start_block..start_block+num_blocks).collect(),
                num_rows: all_rows.len(),
            });
            eprintln!("Sort: wrote run {} ({} rows, {} blocks starting at block {})",
                runs.len(), all_rows.len(), num_blocks, start_block);

            // Read the next batch from the child
            all_rows.clear();
            for _ in 0..memory_budget_rows {
                match child.next(buffer_pool) {
                    Some(row) => all_rows.push(row),
                    None => break,
                }
            }
            if all_rows.is_empty() {
                break;
            }
        }

        eprintln!("Sort: {} runs generated, starting merge", runs.len());

        // Multi-pass K-way merge — merge all runs down to a single sorted run
        // on disk, then stream results via RunReader to avoid loading all rows
        // into memory (which would blow the 64 MB RLIMIT_AS budget).
        let final_run = merge_all_runs_to_disk(
            runs,
            &sort_keys,
            &data_types,
            buffer_pool,
            block_size,
        );

        eprintln!("Sort: merge complete, {} rows in final run on disk", final_run.num_rows);

        let reader = RunReader::new(&final_run, data_types.clone(), buffer_pool);
        SortOp {
            sorted_rows: Vec::new(),
            current_index: 0,
            final_run_reader: Some(reader),
            output_schema,
            output_types: data_types,
            _marker: std::marker::PhantomData,
        }
    }
}

// ─── Row Comparison ──────────────────────────────────────────────────────────

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

// ─── Memory Budget ───────────────────────────────────────────────────────────

/// Estimate how many rows fit in `sort_memory_bytes`.
///
/// Accounts for the in-memory Rust representation of a Row (Vec<Data>) rather
/// than the on-disk binary encoding.  The per-row overhead is larger in memory
/// because each Data::String owns a heap-allocated String, and the Vec itself
/// adds indirection.
fn estimate_memory_budget(sort_memory_bytes: usize, data_types: &[DataType]) -> usize {
    // Estimated bytes for each column's Data variant on the heap
    let data_size: usize = data_types
        .iter()
        .map(|dt| match dt {
            DataType::Int32   => 4,
            DataType::Int64   => 8,
            DataType::Float32 => 4,
            DataType::Float64 => 8,
            // String: 24-byte String header + ~50 bytes average payload
            DataType::String  => 74,
        })
        .sum();

    // Vec<Data> overhead: 24 bytes for Vec struct itself
    // Each Data enum variant: 32 bytes (enum discriminant + largest variant)
    let row_overhead = 24 + data_types.len() * 32;
    let effective_row_size = data_size + row_overhead;

    // Use the supplied memory budget (at least 100 rows to avoid degenerate behaviour)
    let budget = sort_memory_bytes.max(1024 * 1024); // floor at 1 MB
    (budget / effective_row_size).max(100)
}

// ─── Heap Entry (min-heap via reversed Ord) ──────────────────────────────────

/// A single entry in the K-way merge priority queue.
///
/// `sort_keys` is an `Arc` (reference-counted pointer) so that all entries in
/// the heap share the *same* allocation.  The previous approach cloned the
/// `Vec<(usize,bool)>` for every `heap.push()`, which caused O(k × log k)
/// heap allocations per merged row — a significant overhead for large merges.
struct HeapEntry {
    row: Row,
    run_index: usize,
    sort_keys: Arc<Vec<(usize, bool)>>,
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
        // REVERSED so that BinaryHeap (max-heap) behaves as a min-heap
        compare_rows(&other.row, &self.row, &self.sort_keys)
    }
}

// ─── K-Way Merge Controller ──────────────────────────────────────────────────

/// Merge all runs down to a single sorted Run on disk.
/// Uses multi-pass K-way merge with max_fanout runs per pass.
fn merge_all_runs_to_disk(
    mut runs: Vec<Run>,
    sort_keys: &[(usize, bool)],
    data_types: &[DataType],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
    block_size: usize,
) -> Run {
    let max_fanout = 128;

    // If there's only one run, it's already sorted — return it directly.
    if runs.len() == 1 {
        return runs.into_iter().next().unwrap();
    }

    // Merge passes until we have a single run
    while runs.len() > 1 {
        eprintln!("Sort: merge pass ({} runs → chunks of {})", runs.len(), max_fanout);
        let mut next_pass_runs = Vec::new();

        for chunk in runs.chunks(max_fanout) {
            if chunk.len() == 1 {
                next_pass_runs.push(chunk[0].clone());
            } else {
                let merged_run = merge_k_runs_to_disk(
                    chunk,
                    sort_keys,
                    data_types,
                    buffer_pool,
                    block_size,
                );
                next_pass_runs.push(merged_run);
            }
        }
        runs = next_pass_runs;
    }

    runs.into_iter().next().unwrap()
}

fn merge_k_runs_to_disk(
    runs: &[Run],
    sort_keys: &[(usize, bool)],
    data_types: &[DataType],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
    block_size: usize,
) -> Run {
    let mut readers: Vec<RunReader> = runs
        .iter()
        .map(|run| RunReader::new(run, data_types.to_vec(), buffer_pool))
        .collect();

    // Wrap sort_keys in Arc so all HeapEntries share one allocation.
    let arc_keys = Arc::new(sort_keys.to_vec());

    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.peek() {
            heap.push(HeapEntry {
                row: row.clone(),
                run_index: i,
                sort_keys: Arc::clone(&arc_keys),
            });
        }
    }

    let usable_space = block_size - 2;
    let mut current_block = vec![0u8; block_size];
    let mut offset = 0usize;
    let mut row_count_in_blk: u16 = 0;

    let mut out_block_ids = Vec::new();
    let mut total_rows: usize = 0;

    while let Some(entry) = heap.pop() {
        let encoded = encode_row(&entry.row);

        // Flush full block to disk
        if offset + encoded.len() > usable_space {
            if row_count_in_blk > 0 {
                current_block[block_size - 2..].copy_from_slice(&row_count_in_blk.to_le_bytes());
                let blk = buffer_pool.allocate_anon_blocks(1);
                buffer_pool.write_block(blk, &current_block);
                out_block_ids.push(blk);
            }
            current_block = vec![0u8; block_size];
            offset = 0;
            row_count_in_blk = 0;
        }

        current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count_in_blk += 1;
        total_rows += 1;

        // Advance the run that contributed this row
        let reader = &mut readers[entry.run_index];
        let run_index = entry.run_index;
        reader.advance(buffer_pool);
        if let Some(next_row) = reader.peek() {
            heap.push(HeapEntry {
                row: next_row.clone(),
                run_index,
                sort_keys: Arc::clone(&arc_keys),
            });
        }
    }

    // Flush remaining partial block
    if row_count_in_blk > 0 {
        current_block[block_size - 2..].copy_from_slice(&row_count_in_blk.to_le_bytes());
        let blk = buffer_pool.allocate_anon_blocks(1);
        buffer_pool.write_block(blk, &current_block);
        out_block_ids.push(blk);
    }

    Run {
        block_ids: out_block_ids,
        num_rows: total_rows,
    }
}

// merge_k_runs_to_vec removed — the final merge now always writes to disk
// and results are streamed via RunReader to avoid materializing all rows in memory.
