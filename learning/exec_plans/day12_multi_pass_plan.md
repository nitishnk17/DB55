# Day 12 Implementation Plan: Multi-Pass Merge for External Sort

This plan addresses the remaining task from the `report.md` for Day 12: **Implement multi-pass merge (if needed) - When runs > B-1, merge in multiple passes**. 

While our current implementation of `SortOp` handles external sorting by creating sorted runs and merging them in a single pass using a k-way min-heap, it implicitly assumes that the number of runs ($N$) is less than or equal to the number of available buffer pages for merging ($B-1$, reserving 1 page for output). 

If we have strict memory constraints and $N > B-1$, a single-pass merge will fail because we cannot hold one block from every run in memory simultaneously. To make the external sort robust and scale to datasets far exceeding memory, we must implement multi-pass merging.

## Current State Analysis

1.  **Run Creation:** Works correctly. Creates $N$ sorted runs on anonymous blocks.
2.  **Merging:** Uses `merge_runs` which initializes a `RunReader` for *every* run simultaneously. Each `RunReader` explicitly fetches and unpins blocks from the `BufferPool`.
3.  **The Flaw:** If $N > B-1$, opening $N$ `RunReader`s will thrash the buffer pool or fail if we run out of frames, as we try to keep at least 1 block per run active in the merge step. 

## Proposed Architecture for Multi-Pass Merge

We will refactor `merge_runs` to check if a multi-pass approach is necessary based on a computed `merge_fanout` ($B-1$). If the number of runs exceeds this fanout, we iteratively merge chunks of runs into new, larger runs until only one run remains.

### Configuration / Heuristic
*   **Merge Fanout ($K$):** The maximum number of runs we can safely merge simultaneously.
    *   `K = available_memory_for_sort / block_size - 1` (reserving 1 block for the output buffer, though our output writes directly to an intermediate target right now, we still need to cap $K$).
    *   To be conservative and prevent buffer pool exhaustion, we will compute $K$ based on `memory_budget` or explicitly pass an estimated frame limit to `merge_runs`.

### Implementation Steps

---

### Step 1: Update `BufferPool` to allow freeing anonymous blocks
During intermediate merge passes, we read from old runs and write to new runs. To avoid leaking anonymous blocks and blowing up disk size, we need a way to tell `DiskManager` / `BufferPool` to free blocks once a run is fully merged, or we manage our own logical free list in `AnonBlockAllocator`.
However, looking at the assignment specs, SQLite/simulator doesn't require explicit freeing; we can just keep allocating forward using `AnonBlockAllocator::allocate(num_blocks)` as long as we don't exceed `RLIMIT_FSIZE`. 
*Decision: Keep allocation simple. No explicit free needed unless `disk_size` limit is breached, which is unlikely for intermediate passes.*

---

### Step 2: Refactor `merge_runs` into a core `merge_k_runs` function
Extract the current logic of `merge_runs` that takes a slice of runs `&[Run]` and produces a stream of rows. We will modify it to write its output to new anonymous blocks, creating a *new* single `Run` instead of returning `Vec<Row>`.

#### [MODIFY] `sort.rs`
1.  **Rename/Refactor `merge_runs`:**
    Create a helper `fn merge_k_runs(runs: &[Run], sort_keys: &[(usize, bool)], column_specs: &[ColumnSpec], buffer_pool: &mut BufferPool<...>, allocator: &mut AnonBlockAllocator, block_size: usize) -> Run`
    *   This function does the k-way heap merge.
    *   Instead of pushing to a `Vec<Row>`, it uses a buffer. When the buffer hits `block_size`, it writes the block to disk using the `allocator`.
    *   Returns a new `Run` representing the merged output.

2.  **Create a New `merge_all_runs` Controller:**
    ```rust
    fn merge_all_runs(
        mut runs: Vec<Run>,
        sort_keys: &[(usize, bool)],
        column_specs: &[ColumnSpec],
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        allocator: &mut AnonBlockAllocator,
        block_size: usize,
        max_fanout: usize,
    ) -> Run {
        while runs.len() > 1 {
            let mut next_pass_runs = Vec::new();
            // Process chunks of size `max_fanout`
            for chunk in runs.chunks(max_fanout) {
                if chunk.len() == 1 {
                    // Just carry the run over to the next pass
                    next_pass_runs.push(chunk[0].clone()); // assuming Run derives Clone
                } else {
                    let merged_run = merge_k_runs(chunk, sort_keys, column_specs, buffer_pool, allocator, block_size);
                    next_pass_runs.push(merged_run);
                }
            }
            runs = next_pass_runs;
        }
        runs.pop().unwrap()
    }
    ```

---

### Step 3: Implement `RunScanner` for Final Output Generation
The `SortOp::new` function currently expects a `Vec<Row>` to be built. If the final result size is huge, building a `Vec<Row>` in memory defeats the purpose of external sort.
**Current state:** `merge_runs` returns `Vec<Row>`, which we store in `SortOp.sorted_rows` and iterate over in `next()`.
**Required state:** `merge_all_runs` leaves the final sorted data on disk as a single `Run`. `SortOp` should hold a `RunReader` to this final run and pull rows one by one in `Operator::next()`.

#### [MODIFY] `sort.rs`
1.  **Update `SortOp` Struct:**
    ```rust
    pub struct SortOp {
        // Mode 1: In-memory
        sorted_rows: Option<Vec<Row>>,
        current_index: usize,
        
        // Mode 2: External Sort
        final_run_reader: Option<RunReader>,
        
        output_schema: Vec<String>,
        buffer_pool: *mut BufferPool<...>, // Need raw ptr or lifetime mgmt, or we pass state differently. 
    }
    ```
    *Wait, `Operator::next(&mut self)` does not take `buffer_pool`*. In our design, `Operator` doesn't know about `BufferPool`. How did `TableScanner` do it? `TableScanner` holds a raw pointer or similar?
    *Looking at `TableScanner` (from Day 1):* the query executor likely passed `&mut BufferPool` into `.new()`. 
    Actually, let's look at `TableScanner` definition. We might not need to change the buffering structure yet if the assignment only requires multi-pass logic but we can still return a materialized `Vec<Row>` if the strict constraint is only on the sorting phase buffering, not final output.
    
    *Correction:* To fully complete Day 12's "When runs > B-1, merge in multiple passes", we *must* implement the intermediate pass logic (`merge_all_runs`). However, returning a `Vec<Row>` at the very end is what the current Volcano iterator `SortOp` expects and changing the generic `Operator` trait to pass `&mut BufferPool` to `next()` would break the entire execution engine.
    Thus, our plan is:
    1.  Perform multi-pass merge on disk recursively until 1 run is left.
    2.  For the *last* run, read it entirely into memory to satisfy `SortOp`'s current iterator model (or read it chunk by chunk if memory allows, but `Vec<Row>` is easiest given the trait bounds).
    
    *Wait, if we pull the final run into memory, we violate memory budget again!*
    Let's check how `TableScanner` handles `next()`. Does it hold a pointer to the buffer pool? We need to use `view_file` on `TableScanner` to decide how `SortOp` should stream data in `next()`.

---

### Phase 1 Tasks Summary
1.  Add `Clone` to `Run`.
2.  Extract `merge_k_runs` logic that writes to intermediate disk runs.
3.  Implement `merge_all_runs` to loop until 1 run is left.
4.  Refactor `SortOp::new` to trigger multi-pass merge when needed.
5.  Read the final merged run back into memory to satisfy `SortOp` contract (or fix `SortOp` to stream properly if `TableScanner` shows how).

## Open Questions & Review Required

> [!WARNING]
> **Memory Model for `Operator::next()`:** 
> Our `Operator` trait (`fn next(&mut self) -> Option<Row>`) does not take a `BufferPool`. If the final sorted table exceeds memory, storing it as `Vec<Row>` inside `SortOp` will OOM. We must either:
> 1. Keep returning `Vec<Row>` for now (satisfies standard TPCH sizing, but fails on true massive scale).
> 2. See how `TableScanner` stores the `BufferPool` reference and do the same for `SortOp`.
> I will check `TableScanner` code first thing upon approval. 

Do you approve this plan to implement iterative multi-pass merging (`runs > B-1`)?
