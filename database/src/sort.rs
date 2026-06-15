use crate::buffer_pool::BufferPool;
use crate::disk_run::{Run, RunReader, rows_to_run_buffer};
use crate::operator::Operator;
use crate::row::Row;
use common::query::SortSpec;
use common::{Data, DataType};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{Read, Write};
use std::sync::Arc;

// ─── SortOp ─────────────────────────────────────────────────────────────────

pub struct SortOp<R: Read, W: Write> {
    /// In-memory path: all rows sorted and stored here.
    sorted_rows: Vec<Row>,
    current_index: usize,
    in_memory_mode: bool,

    /// External sort path: run readers acting as merged inputs.
    run_readers: Vec<RunReader>,
    merge_heap: BinaryHeap<HeapEntry>,
    arc_keys: Arc<Vec<SortKey>>,
    cache_sort_keys: bool,
    pool_ptr: *mut BufferPool<R, W>,

    output_schema: Vec<String>,
    output_types: Vec<DataType>,
    _marker: std::marker::PhantomData<(R, W)>,
}

impl<R: Read, W: Write> Operator<R, W> for SortOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        if self.in_memory_mode {
            if self.current_index < self.sorted_rows.len() {
                let row = std::mem::replace(
                    &mut self.sorted_rows[self.current_index],
                    Row { values: Vec::new() },
                );
                self.current_index += 1;
                return Some(row);
            } else {
                return None;
            }
        }

        // External sort pipelined path
        if let Some(entry) = self.merge_heap.pop() {
            let row = entry.row;
            let run_idx = entry.run_index;

            // Advance the reader
            let reader = &mut self.run_readers[run_idx];
            if let Some(next_row) = reader.next_owned(pool) {
                self.merge_heap.push(make_heap_entry(
                    next_row,
                    run_idx,
                    Arc::clone(&self.arc_keys),
                    self.cache_sort_keys,
                ));
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

        let sort_keys: Vec<SortKey> = sort_specs
            .iter()
            .map(|spec| {
                let idx = col_index_map[&spec.column_name];
                let dt = data_types[idx].clone();
                let cmp_fn = dispatch_cmp(&dt);
                SortKey {
                    col_idx: idx,
                    ascending: spec.ascending,
                    data_type: dt,
                    cmp_fn,
                }
            })
            .collect();

        let sort_budget_bytes = sort_memory_bytes.max(1024 * 1024);
        let block_size = buffer_pool.block_size();
        let arc_keys = Arc::new(sort_keys.clone());
        let cache_sort_keys = should_cache_sort_keys(&sort_keys);

        let mut all_rows = Vec::new();
        let mut buffered_bytes = 0usize;
        let mut overflow_row: Option<Row> = None;
        while let Some(row) = child.next(buffer_pool) {
            let row_bytes = estimate_row_memory_bytes(&row);
            if !all_rows.is_empty() && buffered_bytes + row_bytes > sort_budget_bytes {
                overflow_row = Some(row);
                break;
            }
            buffered_bytes += row_bytes;
            all_rows.push(row);
        }

        if overflow_row.is_none() {
            sort_rows(&mut all_rows, &sort_keys, cache_sort_keys);
            return SortOp {
                sorted_rows: all_rows,
                current_index: 0,
                in_memory_mode: true,
                run_readers: Vec::new(),
                merge_heap: BinaryHeap::new(),
                arc_keys,
                cache_sort_keys,
                pool_ptr: buffer_pool as *mut BufferPool<R, W>,
                output_schema,
                output_types: data_types,
                _marker: std::marker::PhantomData,
            };
        }

        // External sort path
        let mut runs: Vec<Run> = Vec::new();

        loop {
            sort_rows(&mut all_rows, &sort_keys, cache_sort_keys);

            let raw = rows_to_run_buffer(&all_rows, block_size);
            let num_rows = all_rows.len();
            let num_blocks = raw.len() / block_size;
            let block_ids = buffer_pool.write_raw_run_blocks(&raw, num_blocks);
            runs.push(Run {
                block_ids,
                num_rows,
            });

            all_rows.clear();
            buffered_bytes = 0;
            if let Some(row) = overflow_row.take() {
                buffered_bytes = estimate_row_memory_bytes(&row);
                all_rows.push(row);
            }

            while let Some(row) = child.next(buffer_pool) {
                let row_bytes = estimate_row_memory_bytes(&row);
                if !all_rows.is_empty() && buffered_bytes + row_bytes > sort_budget_bytes {
                    overflow_row = Some(row);
                    break;
                }
                buffered_bytes += row_bytes;
                all_rows.push(row);
            }
            if all_rows.is_empty() {
                break;
            }
        }

        // --- Pipelined merge initialization ---
        let num_runs = runs.len().max(1);
        // Keep aggregate prefetch buffers bounded during large merges.
        // Reserve only a quarter of the sort budget for active reader prefetch
        // space so heap entries and other merge state still fit comfortably.
        let merge_prefetch_budget = (sort_budget_bytes / 4).max(block_size * 4);
        let prefetch_blocks_per_reader =
            (merge_prefetch_budget / num_runs / block_size).clamp(8, 128);
        let mut run_readers: Vec<RunReader> = runs
            .into_iter()
            .map(|run| {
                RunReader::new_with_prefetch(
                    &run,
                    data_types.clone(),
                    buffer_pool,
                    prefetch_blocks_per_reader,
                )
            })
            .collect();

        let mut merge_heap = BinaryHeap::new();
        for (i, reader) in run_readers.iter_mut().enumerate() {
            if let Some(row) = reader.next_owned(buffer_pool) {
                merge_heap.push(make_heap_entry(
                    row,
                    i,
                    Arc::clone(&arc_keys),
                    cache_sort_keys,
                ));
            }
        }

        SortOp {
            sorted_rows: Vec::new(),
            current_index: 0,
            in_memory_mode: false,
            run_readers,
            merge_heap,
            arc_keys,
            cache_sort_keys,
            pool_ptr: buffer_pool as *mut BufferPool<R, W>,
            output_schema,
            output_types: data_types,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R: Read, W: Write> Drop for SortOp<R, W> {
    fn drop(&mut self) {
        if self.pool_ptr.is_null() {
            return;
        }
        // SAFETY: pool_ptr is captured from the live buffer_pool during operator
        // construction, and operators are dropped before that pool in db_main.
        let pool = unsafe { &mut *self.pool_ptr };
        for reader in &mut self.run_readers {
            if !reader.run.block_ids.is_empty() {
                pool.free_run(&reader.run);
                reader.run.block_ids.clear();
            }
        }
    }
}

// ─── Row Comparison ──────────────────────────────────────────────────────────

type CmpFn = fn(&Data, &Data) -> Ordering;

#[inline]
fn cmp_int32(a: &Data, b: &Data) -> Ordering {
    if let (Data::Int32(x), Data::Int32(y)) = (a, b) {
        x.cmp(y)
    } else {
        Ordering::Equal
    }
}
#[inline]
fn cmp_int64(a: &Data, b: &Data) -> Ordering {
    if let (Data::Int64(x), Data::Int64(y)) = (a, b) {
        x.cmp(y)
    } else {
        Ordering::Equal
    }
}
#[inline]
fn cmp_f32(a: &Data, b: &Data) -> Ordering {
    if let (Data::Float32(x), Data::Float32(y)) = (a, b) {
        x.total_cmp(y)
    } else {
        Ordering::Equal
    }
}
#[inline]
fn cmp_f64(a: &Data, b: &Data) -> Ordering {
    if let (Data::Float64(x), Data::Float64(y)) = (a, b) {
        x.total_cmp(y)
    } else {
        Ordering::Equal
    }
}
#[inline]
fn cmp_string(a: &Data, b: &Data) -> Ordering {
    if let (Data::String(x), Data::String(y)) = (a, b) {
        x.cmp(y)
    } else {
        Ordering::Equal
    }
}

#[inline]
fn dispatch_cmp(dt: &DataType) -> CmpFn {
    match dt {
        DataType::Int32 => cmp_int32,
        DataType::Int64 => cmp_int64,
        DataType::Float32 => cmp_f32,
        DataType::Float64 => cmp_f64,
        DataType::String => cmp_string,
    }
}

fn compare_rows(a: &Row, b: &Row, sort_keys: &[SortKey]) -> Ordering {
    for key in sort_keys {
        let val_a = &a.values[key.col_idx];
        let val_b = &b.values[key.col_idx];
        let cmp = (key.cmp_fn)(val_a, val_b);
        match cmp {
            Ordering::Equal => continue,
            other => {
                return if key.ascending {
                    other
                } else {
                    other.reverse()
                };
            }
        }
    }
    Ordering::Equal
}

fn sort_rows(rows: &mut Vec<Row>, sort_keys: &[SortKey], cache_sort_keys: bool) {
    // Fast path: single non-string key.  Extract typed keys into a
    // contiguous `(key, row_index)` Vec, sort that, then permute the rows by
    // index.  Skips the per-comparison `cmp_fn` function-pointer dispatch and
    // the `Data` enum match that the generic path pays.
    if sort_keys.len() == 1 {
        let key = &sort_keys[0];
        match key.data_type {
            DataType::Int32 => {
                sort_rows_single_int(rows, key.col_idx, key.ascending, |d| match d {
                    Data::Int32(v) => *v as i64,
                    _ => 0,
                });
                return;
            }
            DataType::Int64 => {
                sort_rows_single_int(rows, key.col_idx, key.ascending, |d| match d {
                    Data::Int64(v) => *v,
                    _ => 0,
                });
                return;
            }
            DataType::Float32 => {
                sort_rows_single_float(rows, key.col_idx, key.ascending, |d| match d {
                    Data::Float32(v) => *v as f64,
                    _ => 0.0,
                });
                return;
            }
            DataType::Float64 => {
                sort_rows_single_float(rows, key.col_idx, key.ascending, |d| match d {
                    Data::Float64(v) => *v,
                    _ => 0.0,
                });
                return;
            }
            DataType::String => {
                // fall through to generic path
            }
        }
    }

    if !cache_sort_keys {
        rows.sort_unstable_by(|a, b| compare_rows(a, b, sort_keys));
        return;
    }

    let mut decorated: Vec<DecoratedRow> = rows
        .drain(..)
        .map(|row| {
            let key_values = extract_key_values(&row, sort_keys);
            DecoratedRow { row, key_values }
        })
        .collect();

    decorated.sort_unstable_by(|a, b| compare_key_values(&a.key_values, &b.key_values, sort_keys));

    rows.reserve(decorated.len());
    for decorated_row in decorated {
        rows.push(decorated_row.row);
    }
}

fn sort_rows_single_int<F: Fn(&Data) -> i64>(
    rows: &mut Vec<Row>,
    col_idx: usize,
    ascending: bool,
    extract: F,
) {
    let n = rows.len();
    let mut pairs: Vec<(i64, u32)> = Vec::with_capacity(n);
    for (i, r) in rows.iter().enumerate() {
        pairs.push((extract(&r.values[col_idx]), i as u32));
    }
    if ascending {
        // Default Ord on (i64, u32) is lexicographic — ties broken by original index.
        pairs.sort_unstable();
    } else {
        pairs.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    }
    permute_rows_by_pairs(rows, &pairs);
}

fn sort_rows_single_float<F: Fn(&Data) -> f64>(
    rows: &mut Vec<Row>,
    col_idx: usize,
    ascending: bool,
    extract: F,
) {
    let n = rows.len();
    let mut pairs: Vec<(f64, u32)> = Vec::with_capacity(n);
    for (i, r) in rows.iter().enumerate() {
        pairs.push((extract(&r.values[col_idx]), i as u32));
    }
    if ascending {
        pairs.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    } else {
        pairs.sort_unstable_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    }
    permute_rows_by_pairs(rows, &pairs);
}

fn permute_rows_by_pairs<K>(rows: &mut Vec<Row>, pairs: &[(K, u32)]) {
    let mut sorted: Vec<Row> = Vec::with_capacity(pairs.len());
    for &(_, idx) in pairs {
        sorted.push(std::mem::replace(
            &mut rows[idx as usize],
            Row { values: Vec::new() },
        ));
    }
    *rows = sorted;
}

#[derive(Clone)]
struct SortKey {
    col_idx: usize,
    ascending: bool,
    data_type: DataType,
    cmp_fn: CmpFn,
}

// ─── Memory Budget ───────────────────────────────────────────────────────────

fn estimate_row_memory_bytes(row: &Row) -> usize {
    let payload: usize = row
        .values
        .iter()
        .map(|v| match v {
            Data::Int32(_) => 4,
            Data::Int64(_) => 8,
            Data::Float32(_) => 4,
            Data::Float64(_) => 8,
            Data::String(s) => s.len() + 1,
        })
        .sum();
    24 + row.values.len() * 32 + payload
}

fn should_cache_sort_keys(sort_keys: &[SortKey]) -> bool {
    !sort_keys.is_empty()
        && sort_keys
            .iter()
            .all(|k| !matches!(k.data_type, DataType::String))
}

// ─── Heap Entry (min-heap via reversed Ord) ──────────────────────────────────

struct HeapEntry {
    row: Row,
    run_index: usize,
    sort_keys: Arc<Vec<SortKey>>,
    key_values: Option<Vec<Data>>,
}

struct DecoratedRow {
    row: Row,
    key_values: Vec<Data>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        match (&self.key_values, &other.key_values) {
            (Some(a), Some(b)) => compare_key_values(a, b, &self.sort_keys) == Ordering::Equal,
            _ => compare_rows(&self.row, &other.row, &self.sort_keys) == Ordering::Equal,
        }
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
        match (&self.key_values, &other.key_values) {
            (Some(a), Some(b)) => compare_key_values(b, a, &self.sort_keys),
            _ => compare_rows(&other.row, &self.row, &self.sort_keys),
        }
    }
}

fn make_heap_entry(
    row: Row,
    run_index: usize,
    sort_keys: Arc<Vec<SortKey>>,
    cache_sort_keys: bool,
) -> HeapEntry {
    let key_values = if cache_sort_keys {
        Some(extract_key_values(&row, &sort_keys))
    } else {
        None
    };
    HeapEntry {
        row,
        run_index,
        sort_keys,
        key_values,
    }
}

fn extract_key_values(row: &Row, sort_keys: &[SortKey]) -> Vec<Data> {
    let mut keys = Vec::with_capacity(sort_keys.len());
    for key in sort_keys {
        keys.push(row.values[key.col_idx].clone());
    }
    keys
}

fn compare_key_values(a: &[Data], b: &[Data], sort_keys: &[SortKey]) -> Ordering {
    for (i, key) in sort_keys.iter().enumerate() {
        let cmp = (key.cmp_fn)(&a[i], &b[i]);
        match cmp {
            Ordering::Equal => continue,
            other => {
                return if key.ascending {
                    other
                } else {
                    other.reverse()
                };
            }
        }
    }
    Ordering::Equal
}
