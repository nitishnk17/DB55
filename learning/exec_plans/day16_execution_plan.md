# Day 16 Execution Plan: Grace Hash Join

> **Prerequisite**: Day 15 (Join Detection & BNLJ) verified and complete ✅
> **Report Reference**: Phase 3, Day 16 (April 4) — "Hash Join (Grace)"

---

## Day 15 Verification Summary

Before proceeding, here's the verification of Day 15 implementation against the execution plan:

### ✅ Day 15 Checklist — All Items Complete

| Day 15 Requirement | Status | Evidence |
|---|---|---|
| Extract `Run`, `RunReader`, `AnonBlockAllocator`, `rows_to_blocks` from `sort.rs` into `disk_run.rs` | ✅ | `disk_run.rs` (104 lines) contains `Run`, `RunReader`, `rows_to_blocks`; `sort.rs` imports from `disk_run` |
| `sort.rs` uses `disk_run` components | ✅ | Line 8: `use crate::disk_run::{rows_to_blocks, Run, RunReader};` |
| Join pattern detection in `query_executor.rs` | ✅ | Lines 32-83: Detects `Filter(EQ, Column)` over `Cross`, resolves column indices cross-boundary |
| Remaining non-equi predicates wrapped in `FilterOp` | ✅ | Lines 74-81: Removes the used equi-join predicate, wraps residuals in FilterOp |
| `JoinOp` implements BNLJ with materialized right | ✅ | `join.rs` Lines 28-39: right child materialized to anonymous blocks via `rows_to_blocks` + `Run` |
| BNLJ chunked outer loop | ✅ | `join.rs` Lines 56-93: Reads left in chunks (`max_chunk_rows`), streams right via `RunReader`, compares join keys |
| `JoinOp` implements `Operator` trait | ✅ | `join.rs` Lines 104-118: `next()` yields from pre-computed `joined_rows`, `schema()` returns concatenated schema |
| `mod join` declared in `main.rs` | ✅ | `main.rs` Line 19: `mod join;` |
| Project builds cleanly | ✅ | `cargo build -r --bin database` succeeds (1 minor warning: unused `get_anon_start_block` method) |

### ⚠️ Day 15 Issues / Notes

1. **JoinOp materializes ALL results in memory** (`joined_rows: Vec<Row>`). This is fine for small joins (Region ⋈ Nation) but will fail on large tables (customer ⋈ orders). Grace Hash Join (today) will address this.
2. **The `max_chunk_rows` in BNLJ uses a hardcoded 100-block estimate** rather than querying the buffer pool's actual frame count. Works but is not optimal.
3. **`AnonBlockAllocator` was NOT extracted** — instead `buffer_pool.allocate_anon_blocks()` serves that role. This is a slight deviation from the day15 plan, but functionally equivalent.

---

## Day 16 Objective

Implement **Grace Hash Join** as an alternative to BNLJ for equi-joins. Grace Hash Join is superior for large tables because it partitions data to disk first, then performs smaller in-memory joins per partition.

### Why Grace Hash Join?

| Metric | BNLJ (Day 15) | Grace Hash Join |
|---|---|---|
| I/O Cost | `bR + ⌈bR/(B-2)⌉ × bS` | `3(bR + bS)` |
| Memory | Needs outer chunk + full inner scan per chunk | Needs 1 partition to fit in memory |
| Best for | Small tables, or when one side fits in memory | Large tables where neither side fits |

---

## Implementation Steps

### Step 1: Create `hash_join.rs` module

Create a new file `database/src/hash_join.rs`. This module will house the Grace Hash Join operator.

**Struct definition:**
```rust
pub struct HashJoinOp {
    // Partition metadata
    partitions_left: Vec<Run>,      // Each partition stored as a Run on anonymous blocks
    partitions_right: Vec<Run>,     // Matching partitions for right side
    num_partitions: usize,
    
    // Schema info
    left_schema: Vec<ColumnSpec>,
    right_schema: Vec<ColumnSpec>,
    left_col_idx: usize,
    right_col_idx: usize,
    output_schema: Vec<String>,
    
    // Iteration state for streaming results
    current_partition: usize,       // Which partition pair we're processing
    current_matches: Vec<Row>,      // Buffered match results from current partition
    match_index: usize,            // Position within current_matches
}
```

### Step 2: Implement Partition Phase (`partition_input`)

Write a helper function that reads ALL rows from an operator, hashes each row on the join column, and distributes rows into `N` partitions. Each partition is written as a `Run` to anonymous disk blocks.

**Detailed steps:**
1. Choose `N` (number of partitions). A good heuristic: `N = ceil(estimated_rows / rows_per_memory_chunk)`. Start with a fixed `N = 64` or compute from buffer pool frame count.
2. Create `N` in-memory `Vec<Row>` buffers (one per partition bucket).
3. For each row from the input operator:
   - Hash the join column value → `bucket_id = hash(join_val) % N`
   - Push row into `buckets[bucket_id]`
4. After consuming all input, flush each non-empty bucket to disk:
   - Use `rows_to_blocks()` from `disk_run.rs` to encode
   - Use `buffer_pool.allocate_anon_blocks()` to get block IDs
   - Use `buffer_pool.write_block()` to write each block
   - Store the resulting `Run` in a `Vec<Run>`
5. Return the vector of `Run`s (one per partition).

**Hashing strategy:**
```rust
fn hash_value(val: &Data) -> u64 {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    match val {
        Data::Int32(v) => v.hash(&mut hasher),
        Data::Int64(v) => v.hash(&mut hasher),
        Data::Float32(v) => v.to_bits().hash(&mut hasher),
        Data::Float64(v) => v.to_bits().hash(&mut hasher),
        Data::String(v) => v.hash(&mut hasher),
    }
    hasher.finish()
}
```

### Step 3: Implement Build & Probe Phase (`process_partition`)

For each partition pair `(left_partition_i, right_partition_i)`:

1. **BUILD** — Load the **smaller** partition into an in-memory hash table:
   - Read all rows from the partition's `Run` using `RunReader`
   - Build a `HashMap<u64, Vec<Row>>` keyed by `hash(join_column_value)`
   - (For correctness, store the actual values and do exact comparison during probe to handle hash collisions)

2. **PROBE** — Scan the other (larger) partition:
   - Read rows one-by-one from the `RunReader`
   - Hash the join column → look up in the hash map
   - For each candidate match, compare actual join column values (not just hashes)
   - On match: combine left + right row values and push to `current_matches`

3. Return the collected `current_matches`.

**Key decision**: To avoid materializing ALL join results at once (which killed BNLJ for large data), process **one partition pair at a time** inside `next()`. When `current_matches` is exhausted, advance to the next partition pair.

### Step 4: Implement `HashJoinOp::new`

```rust
pub fn new(
    mut left: Box<dyn Operator>,
    mut right: Box<dyn Operator>,
    left_col_idx: usize,
    right_col_idx: usize,
    left_column_specs: Vec<ColumnSpec>,
    right_column_specs: Vec<ColumnSpec>,
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
) -> Self {
    let mut output_schema = left.schema();
    output_schema.extend(right.schema());
    
    let num_partitions = 64; // Or compute dynamically
    
    // Phase 1: Partition both inputs
    let partitions_left = partition_input(
        &mut left, left_col_idx, num_partitions, &left_column_specs, buffer_pool
    );
    let partitions_right = partition_input(
        &mut right, right_col_idx, num_partitions, &right_column_specs, buffer_pool
    );
    
    HashJoinOp {
        partitions_left,
        partitions_right,
        num_partitions,
        left_schema: left_column_specs,
        right_schema: right_column_specs,
        left_col_idx,
        right_col_idx,
        output_schema,
        current_partition: 0,
        current_matches: Vec::new(),
        match_index: 0,
    }
}
```

### Step 5: Implement `Operator` trait for `HashJoinOp`

```rust
impl Operator for HashJoinOp {
    fn next(&mut self) -> Option<Row> {
        loop {
            // 1. If we have buffered matches, return the next one
            if self.match_index < self.current_matches.len() {
                let row = self.current_matches[self.match_index].clone();
                self.match_index += 1;
                return Some(row);
            }
            
            // 2. Process the next partition pair
            if self.current_partition >= self.num_partitions {
                return None; // All partitions exhausted
            }
            
            // 3. Build & Probe current partition
            self.current_matches = self.process_partition(
                self.current_partition, buffer_pool
            );
            self.match_index = 0;
            self.current_partition += 1;
            // Loop back to check if this partition produced matches
        }
    }
    
    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }
}
```

> **Design challenge**: `next()` can't borrow `buffer_pool` because it's not stored in the struct. You have two options:
> 1. **Materialize all partition results in `new()`** (simpler, like current JoinOp — but defeats the purpose for very large joins)
> 2. **Store a reference/Rc<RefCell<>> to buffer_pool in the struct** (more complex but truly streaming)
> 
> **Recommendation for Day 16**: Start with option 1 (materialize in `new()`). If memory becomes an issue, refactor to option 2 later.

### Step 6: Wire into `query_executor.rs`

Modify the join detection logic in `build_operator()` to select between BNLJ and Grace Hash Join.

**Decision heuristic** (using statistics if available):
```rust
// Inside the equi-join detection branch in query_executor.rs:

// Check if stats suggest large tables
let use_hash_join = should_use_hash_join(&left_schema, &right_schema, ctx);

if use_hash_join {
    let left_column_specs = resolve_column_specs(&left_schema, ctx);
    let join_op = Box::new(crate::hash_join::HashJoinOp::new(
        left, right_op,
        left_col_idx, right_col_idx,
        left_column_specs, right_column_specs,
        buffer_pool,
    ));
    // ... wrap with remaining predicates as before
} else {
    // Existing BNLJ path
}
```

**Simple heuristic for `should_use_hash_join`**:
- Look for `CardinalityStat` in table column stats
- If either table has > 10,000 rows → use hash join
- Otherwise → use BNLJ (cheaper for small data)
- If no stats available → default to hash join (safer for unknown sizes)

### Step 7: Register the module

Add `mod hash_join;` to `database/src/main.rs` alongside the existing module declarations.

### Step 8: Test

1. **Small data test** (Region ⋈ Nation): should still pass with either join strategy
2. **Large data test** (customer ⋈ orders): this is the real benchmark
   - Create a new join query in `monitor_config.json`:
   ```json
   {
     "execution_name": "customer_orders_join",
     "disabled": false,
     "memory_limit_mb": 64,
     "query": { ... Filter(c_custkey = o_custkey) over Cross(Scan(customer), Scan(orders)) ... },
     "expected_output_file": "/abs/path/to/expected_customer_orders.csv"
   }
   ```
   - Generate expected output with SQLite:
   ```bash
   echo ".mode list
   .separator '|'
   SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, '' FROM customer JOIN orders ON c_custkey = o_custkey;" | sqlite3 scratch/compiled_datasets/tpch/sqlite.db > scratch/runtimes/tpch/expected_customer_orders.csv
   ```
3. Compare I/O costs (visible in monitor output) between BNLJ and Hash Join

---

## File Checklist

| File | Action |
|---|---|
| `database/src/hash_join.rs` | **[NEW]** Grace Hash Join implementation |
| `database/src/main.rs` | **[MODIFY]** Add `mod hash_join;` |
| `database/src/query_executor.rs` | **[MODIFY]** Add join strategy selection and `HashJoinOp` instantiation |
| `database/src/disk_run.rs` | **No changes** (reused as-is) |

---

## Open Questions

> [!IMPORTANT]
> 1. **Number of partitions**: Should we hardcode `N = 64` or compute dynamically from memory limit? Dynamic is better but adds complexity. Recommendation: start with 64, make it configurable later.
> 2. **Buffer pool access in `next()`**: The Volcano iterator model means `next()` has no access to buffer pool. Should we materialize all results in `new()` (simpler) or restructure to pass buffer pool through? **Recommendation: materialize in new() for now.**
> 3. **Partition overflow**: If a single partition is too large to fit in memory (skewed hash keys), we'd need recursive partitioning. Skip this for Day 16 and add it as a Day 17+ optimization if needed.

Do you approve this plan before we start implementing?
