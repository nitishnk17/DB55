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

pub struct SortOp<R: Read, W: Write> {
    /// In-memory path: all rows sorted and stored here.
    sorted_rows: Vec<Row>,
    current_index: usize,
    in_memory_mode: bool,

    /// External sort path: run readers acting as merged inputs.
    run_readers: Vec<RunReader>,
    merge_heap: BinaryHeap<HeapEntry>,
    arc_keys: Arc<Vec<(usize, bool)>>,

    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    _marker: std::marker::PhantomData<(R, W)>,
}

impl<R: Read, W: Write> Operator<R, W> for SortOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        if self.in_memory_mode {
            if self.current_index < self.sorted_rows.len() {
                let row = self.sorted_rows[self.current_index].clone();
                self.current_index += 1;
                return Some(row);
            } else {
                return None;
            }
        }

        // External sort pipelined path
        if let Some(entry) = self.merge_heap.pop() {
            let row = entry.row.clone();
            let run_idx = entry.run_index;

            // Advance the reader
            let reader = &mut self.run_readers[run_idx];
            reader.advance(pool);
            if let Some(next_row) = reader.peek() {
                self.merge_heap.push(HeapEntry {
                    row: next_row.clone(),
                    run_index: run_idx,
                    sort_keys: Arc::clone(&self.arc_keys),
                });
            } else {
                // Reader exhausted. Free the run disk blocks immediately!
                if !reader.run.block_ids.is_empty() {
                    pool.free_run(&reader.run);
                    reader.run.block_ids.clear();
                }
            }
            return Some(row);
        }

        None
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
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
        let arc_keys = Arc::new(sort_keys.clone());

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
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
            return SortOp {
                sorted_rows: all_rows,
                current_index: 0,
                in_memory_mode: true,
                run_readers: Vec::new(),
                merge_heap: BinaryHeap::new(),
                arc_keys,
                output_schema,
                output_types: data_types,
                _marker: std::marker::PhantomData,
            };
        }

        // External sort path
        let mut runs: Vec<Run> = Vec::new();

        loop {
            all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));

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

        // --- Pipelined merge initialization ---
        let run_readers: Vec<RunReader> = runs
            .into_iter()
            .map(|run| RunReader::new(&run, data_types.clone(), buffer_pool))
            .collect();

        let mut merge_heap = BinaryHeap::new();
        for (i, reader) in run_readers.iter().enumerate() {
            if let Some(row) = reader.peek() {
                merge_heap.push(HeapEntry {
                    row: row.clone(),
                    run_index: i,
                    sort_keys: Arc::clone(&arc_keys),
                });
            }
        }

        SortOp {
            sorted_rows: Vec::new(),
            current_index: 0,
            in_memory_mode: false,
            run_readers,
            merge_heap,
            arc_keys,
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

fn estimate_memory_budget(sort_memory_bytes: usize, data_types: &[DataType]) -> usize {
    let data_size: usize = data_types
        .iter()
        .map(|dt| match dt {
            DataType::Int32   => 4,
            DataType::Int64   => 8,
            DataType::Float32 => 4,
            DataType::Float64 => 8,
            DataType::String  => 74,
        })
        .sum();

    let row_overhead = 24 + data_types.len() * 32;
    let effective_row_size = data_size + row_overhead;

    let budget = sort_memory_bytes.max(1024 * 1024);
    (budget / effective_row_size).max(100)
}

// ─── Heap Entry (min-heap via reversed Ord) ──────────────────────────────────

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
