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
/// RunReader/StreamingMerge (large data after external merge sort) to stream results.
pub struct SortOp<R: Read, W: Write> {
    /// In-memory path: all rows sorted and stored here.
    sorted_rows: Vec<Row>,
    current_index: usize,
    
    /// External sort path: Final merged run on disk OR active merge heap.
    final_run_reader: Option<RunReader>,
    streaming_merge: Option<StreamingMerge>,

    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    
    // Cached sort specs for order() method
    sort_specs_cache: Vec<SortSpec>,

    _marker: std::marker::PhantomData<(R, W)>,
}

/// Helper state for streaming the final K-way merge pass without writing it to disk.
struct StreamingMerge {
    heap: BinaryHeap<HeapEntry>,
    readers: Vec<RunReader>,
    sort_keys: Arc<Vec<(usize, bool)>>,
}

impl<R: Read, W: Write> Operator<R, W> for SortOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        // 1. Streaming Merge Path: Yield directly from heap (saves an entire disk write/read pass)
        if let Some(sm) = &mut self.streaming_merge {
            if let Some(entry) = sm.heap.pop() {
                let row = entry.row;
                let run_idx = entry.run_index;
                let reader = &mut sm.readers[run_idx];
                reader.advance(pool);
                if let Some(next_row) = reader.peek() {
                    sm.heap.push(HeapEntry {
                        row: next_row.clone(),
                        run_index: run_idx,
                        sort_keys: Arc::clone(&sm.sort_keys),
                    });
                }
                return Some(row);
            }
            return None;
        }

        // 2. External sort path: stream from a single final merged run on disk
        if let Some(reader) = &mut self.final_run_reader {
            if let Some(row) = reader.peek() {
                let row = row.clone();
                reader.advance(pool);
                return Some(row);
            }
            return None;
        }

        // 3. In-memory path
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

    fn order(&self) -> Option<Vec<SortSpec>> {
        Some(self.sort_specs_cache.clone())
    }
}

impl<R: Read, W: Write> SortOp<R, W> {
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

        let sort_keys = Arc::new(sort_specs
            .iter()
            .map(|spec| {
                let idx = col_index_map[&spec.column_name];
                (idx, spec.ascending)
            })
            .collect::<Vec<(usize, bool)>>());

        let block_size = buffer_pool.block_size();

        // ─── Phase 1: In-Memory Ingestion ─────────────────────────────────────
        // Track actual in-memory bytes aggressively.
        let mut all_rows = Vec::new();
        let mut exceeded = false;
        let mut current_bytes = 0;
        // Allow up to 15% overflow as a safety valve to avoid disk I/O for borderline queries.
        // The row-size estimate (24 + 8×numeric + (24+cap)×string) is conservative enough
        // that 1.15× rarely triggers OOM while catching more in-memory cases.
        let max_bytes = (sort_memory_bytes as f64 * 1.15) as usize;

        while let Some(row) = child.next(buffer_pool) {
            let row_bytes = 24 + row.values.iter().map(|v| match v {
                common::Data::String(s) => 24 + s.capacity(),
                _ => 8,
            }).sum::<usize>();
            
            current_bytes += row_bytes;
            all_rows.push(row);

            if current_bytes > max_bytes {
                exceeded = true;
                break;
            }
        }

        if !exceeded {
            eprintln!("Sort: In-Memory ({} rows, {} MB)", all_rows.len(), current_bytes / (1024*1024));
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
            return SortOp {
                sorted_rows: all_rows,
                current_index: 0,
                final_run_reader: None,
                streaming_merge: None,
                output_schema,
                output_types: data_types,
                sort_specs_cache: sort_specs.to_vec(),
                _marker: std::marker::PhantomData,
            };
        }

        // ─── Phase 2: Run Generation ──────────────────────────────────────────
        eprintln!("Sort: Spilling to External Merge Sort (Budget {} MB)", sort_memory_bytes / (1024*1024));
        let mut runs: Vec<Run> = Vec::new();

        loop {
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
            let blocks = rows_to_blocks(&all_rows, block_size);
            let num_blocks = blocks.len() as u64;
            let start_block = buffer_pool.allocate_anon_blocks(num_blocks);
            // Batch write: flatten all blocks into one contiguous buffer → one disk call per run.
            let all_data: Vec<u8> = blocks.into_iter().flatten().collect();
            buffer_pool.write_blocks_batch(start_block, &all_data);
            runs.push(Run {
                block_ids: (start_block..start_block+num_blocks).collect(),
                num_rows: all_rows.len(),
            });

            all_rows.clear();
            current_bytes = 0;
            while let Some(row) = child.next(buffer_pool) {
                let row_bytes = 24 + row.values.iter().map(|v| match v {
                    common::Data::String(s) => 24 + s.capacity(),
                    _ => 8,
                }).sum::<usize>();
                current_bytes += row_bytes;
                all_rows.push(row);
                if current_bytes > max_bytes { break; }
            }
            if all_rows.is_empty() { break; }
        }

        // ─── Phase 3: Multi-Pass Merge ────────────────────────────────────────
        let max_fanout = 128;
        
        // Merge passes until we can perform the final pass as a stream
        while runs.len() > max_fanout {
            eprintln!("Sort: Intermediate Merge Pass ({} runs)", runs.len());
            let mut next_pass_runs = Vec::new();
            for chunk in runs.chunks(max_fanout) {
                if chunk.len() == 1 {
                    next_pass_runs.push(chunk[0].clone());
                } else {
                    next_pass_runs.push(merge_k_runs_to_disk(chunk, &sort_keys, &data_types, buffer_pool, block_size));
                }
            }
            runs = next_pass_runs;
        }

        // Final streaming merge pass (0 disk writes for the entire table)
        if runs.len() > 1 {
            eprintln!("Sort: Final Pass (Streaming Merge of {} runs)", runs.len());
            let readers: Vec<RunReader> = runs.iter()
                .map(|run| RunReader::new(run, data_types.to_vec(), buffer_pool))
                .collect();
            let mut heap = BinaryHeap::new();
            for (i, reader) in readers.iter().enumerate() {
                if let Some(row) = reader.peek() {
                    heap.push(HeapEntry {
                        row: row.clone(),
                        run_index: i,
                        sort_keys: Arc::clone(&sort_keys),
                    });
                }
            }
            return SortOp {
                sorted_rows: Vec::new(),
                current_index: 0,
                final_run_reader: None,
                streaming_merge: Some(StreamingMerge { heap, readers, sort_keys }),
                output_schema,
                output_types: data_types,
                sort_specs_cache: sort_specs.to_vec(),
                _marker: std::marker::PhantomData,
            };
        } else {
            // Only one run left: stream it normally
            let reader = RunReader::new(&runs[0], data_types, buffer_pool);
            return SortOp {
                sorted_rows: Vec::new(),
                current_index: 0,
                final_run_reader: Some(reader),
                streaming_merge: None,
                output_schema,
                output_types: vec![], // Not used in this path
                sort_specs_cache: sort_specs.to_vec(),
                _marker: std::marker::PhantomData,
            };
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn compare_rows(a: &Row, b: &Row, sort_keys: &[(usize, bool)]) -> Ordering {
    for &(col_idx, ascending) in sort_keys {
        let val_a = &a.values[col_idx];
        let val_b = &b.values[col_idx];
        let cmp = val_a.partial_cmp(val_b).unwrap_or(Ordering::Equal);
        match cmp {
            Ordering::Equal => continue,
            other => return if ascending { other } else { other.reverse() },
        }
    }
    Ordering::Equal
}

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
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_rows(&other.row, &self.row, &self.sort_keys) // Min-heap
    }
}

fn merge_k_runs_to_disk(
    runs: &[Run],
    sort_keys: &Arc<Vec<(usize, bool)>>,
    data_types: &[DataType],
    pool: &mut BufferPool<impl Read, impl Write>,
    block_size: usize,
) -> Run {
    let mut readers: Vec<RunReader> = runs.iter()
        .map(|run| RunReader::new(run, data_types.to_vec(), pool))
        .collect();
    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.peek() {
            heap.push(HeapEntry { row: row.clone(), run_index: i, sort_keys: Arc::clone(sort_keys) });
        }
    }

    // Buffer output blocks and write in batches of WRITE_BATCH to reduce total_writes
    // from O(num_output_blocks) to O(num_output_blocks / WRITE_BATCH).
    const WRITE_BATCH: usize = 64;
    let mut pending_blocks: Vec<Vec<u8>> = Vec::with_capacity(WRITE_BATCH);
    let mut all_block_ids: Vec<u64> = Vec::new();

    let mut current_block = vec![0u8; block_size];
    let mut offset = 0usize;
    let mut row_count: u16 = 0;
    let mut total_rows: usize = 0;

    // Inline flush helper: write pending_blocks in one disk call.
    macro_rules! flush_pending {
        () => {
            if !pending_blocks.is_empty() {
                let n = pending_blocks.len() as u64;
                let start = pool.allocate_anon_blocks(n);
                let data: Vec<u8> = pending_blocks.drain(..).flatten().collect();
                pool.write_blocks_batch(start, &data);
                all_block_ids.extend(start..start + n);
            }
        };
    }

    while let Some(entry) = heap.pop() {
        let encoded = encode_row(&entry.row);
        if offset + encoded.len() > block_size - 2 {
            current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
            pending_blocks.push(current_block);
            current_block = vec![0u8; block_size];
            offset = 0;
            row_count = 0;

            if pending_blocks.len() >= WRITE_BATCH {
                flush_pending!();
            }
        }
        current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count += 1;
        total_rows += 1;

        let r_idx = entry.run_index;
        readers[r_idx].advance(pool);
        if let Some(next) = readers[r_idx].peek() {
            heap.push(HeapEntry { row: next.clone(), run_index: r_idx, sort_keys: Arc::clone(sort_keys) });
        }
    }

    if row_count > 0 {
        current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
        pending_blocks.push(current_block);
    }
    flush_pending!();

    Run { block_ids: all_block_ids, num_rows: total_rows }
}
