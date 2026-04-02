# Day 11 Execution Plan — Sort Operator (In-Memory)

## Goal
Build a `SortOp` operator that materializes all rows from its child, sorts them by one or more columns (ascending/descending), and emits them in sorted order. By the end of today, you'll run Sort queries and get `Validation success!` from the monitor. **This completes all 5 operators** — Scan, Filter, Project, Cross, Sort.

---

## Strategic Decision: In-Memory Sort vs External Sort

> [!IMPORTANT]
> The report planned External Sort across Days 11-12 (sorted run creation + k-way merge). However, given the current architecture:
> - **TableScanner already materializes ALL rows** into memory
> - **CrossOp already materializes** the right child into memory
> - With 64MB memory limit and block-level reads, there's no streaming pipeline yet
>
> **Decision:** Implement an **in-memory sort** first. This gives you a correct, working Sort operator *today* that handles all test queries. External sort (with run creation, serialization, anonymous blocks, and k-way merge) becomes necessary later when you switch to a streaming TableScanner — but that's a separate optimization task.
>
> This effectively combines Day 11 + Day 12 into one day. You can use the freed-up Day 12 for integration testing or starting the external sort upgrade.

---

## Background Concepts (Read This First!)

### What Is a Sort Operator?
Sort is a **blocking** operator — unlike Filter/Project which process one row at a time, Sort must consume **ALL** input before producing **ANY** output. This is because you can't know which row comes first until you've seen them all.

```
    Sort(n_name ASC)           ← YOU BUILD THIS TODAY
         │
      Scan(nation)             ← 25 rows in disk order
```

**Input:** `[ALGERIA(0), ARGENTINA(1), BRAZIL(2), CANADA(3), CHINA(18), ...]` (disk order)
**Output:** `[ALGERIA(0), ARGENTINA(1), BRAZIL(2), CANADA(3), CHINA(18), ...]` (alphabetical — same here by coincidence, but generally different)

### Multi-Column Sorting
SQL often sorts by multiple columns: `ORDER BY a ASC, b DESC`. This means:
1. Sort primarily by `a` ascending
2. For rows where `a` is equal, sort by `b` descending

Example with `ORDER BY n_regionkey ASC, n_name DESC`:
```
(0, MOZAMBIQUE)    ← regionkey=0, names in DESCENDING order within each group
(0, MOROCCO)
(0, KENYA)
(0, ETHIOPIA)
(0, ALGERIA)
(1, UNITED STATES)  ← regionkey=1
(1, PERU)
...
```

### The Comparison Function
Rust's `sort_by` takes a comparator: `|a, b| -> Ordering`. For multi-key sorting, you chain comparisons:

```
For each sort_spec in sort_specs:
  1. Get column values from row_a and row_b
  2. Compare them using partial_cmp
  3. If ascending → use natural ordering
  4. If descending → reverse the ordering
  5. If Equal → continue to next sort_spec
  6. If not Equal → return this ordering
If all sort_specs are Equal → return Equal
```

### Sort and Project Interaction
From the assignment:
> "Project preserves the row order of its child. If a Project appears as an ancestor of Sort, the final output will reflect that sort order."

This means in a tree like:
```
Project(n_name → name)
  └── Sort(n_name ASC)
       └── Scan(nation)
```
Sort runs first (sorts by `n_name`), then Project picks and renames columns while **preserving** the sorted order. This works naturally with our iterator model — Project just calls `child.next()` one at a time.

> [!NOTE]
> **Important subtlety:** Sort may sort by columns that later get **dropped** by Project. For example:
> ```
> Project(n_name → name)            ← drops n_regionkey, n_nationkey, n_comment
>   └── Sort(n_regionkey ASC)       ← sorts by n_regionkey (which Project will drop!)
>        └── Scan(nation)
> ```
> This is valid! Sort sees all columns from Scan. After sorting, Project just picks the columns it needs. The order is preserved even though the sort key is dropped.

---

## Step 1: Add `Clone` to `SortSpec` in `common/src/query.rs`

`SortSpec` currently derives `Debug, Serialize, Deserialize` but **not** `Clone`. You need `Clone` because `build_operator` takes `&QueryOp`, so you'll need to clone `sort_specs` to give `SortOp` owned data.

Change line 53 in `common/src/query.rs`:

```rust
// Before:
#[derive(Debug, Serialize, Deserialize)]
pub struct SortSpec {

// After:
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortSpec {
```

This is a one-line change. `String` and `bool` are both `Clone`, so deriving `Clone` for `SortSpec` works automatically.

---

## Step 2: Create `database/src/sort.rs`

Create a new file `database/src/sort.rs`.

### Step 2.1 — Define the `SortOp` struct

```rust
use std::cmp::Ordering;
use std::collections::HashMap;
use common::query::SortSpec;
use crate::operator::Operator;
use crate::row::Row;

pub struct SortOp {
    sorted_rows: Vec<Row>,     // all rows, sorted
    current_index: usize,      // current position for next()
    output_schema: Vec<String>, // same schema as child (Sort doesn't change columns)
}
```

**Why only 3 fields?** Unlike Filter/Project/Cross, Sort doesn't keep a reference to the child operator. It consumes the entire child during construction (`new()`), sorts the rows, and stores the result. After that, `next()` just iterates through the sorted vector.

**Design difference from other operators:**
- **Filter/Project:** Keep `child` and pull lazily
- **Cross:** Keep `left` (lazy) and `right_rows` (materialized)
- **Sort:** Fully materialized — no `child` reference kept after construction

### Step 2.2 — Implement `SortOp::new()`

```rust
impl SortOp {
    pub fn new(mut child: Box<dyn Operator>, sort_specs: Vec<SortSpec>) -> Self {
        // 1. Capture schema from child BEFORE draining
        let output_schema = child.schema();

        // 2. Build column name → index mapping for sort key lookups
        let col_index_map: HashMap<String, usize> = output_schema
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // 3. Pre-compute sort key indices: Vec<(usize, bool)>
        //    Each entry is (column_index, ascending)
        let sort_keys: Vec<(usize, bool)> = sort_specs
            .iter()
            .map(|spec| {
                let idx = col_index_map[&spec.column_name];
                (idx, spec.ascending)
            })
            .collect();

        // 4. Materialize all rows from child
        let mut sorted_rows = Vec::new();
        while let Some(row) = child.next() {
            sorted_rows.push(row);
        }

        // 5. Sort using multi-key comparator
        sorted_rows.sort_by(|a, b| {
            compare_rows(a, b, &sort_keys)
        });

        SortOp {
            sorted_rows,
            current_index: 0,
            output_schema,
        }
    }
}
```

**Why pre-compute `sort_keys` as `Vec<(usize, bool)>`?** Converting column names to indices once avoids HashMap lookups during sorting. The `sort_by` comparator is called O(n log n) times, so every optimization in it matters. The sort keys become just an index + direction — the fastest possible lookup.

**Rust concept — closures capturing references:** The `sort_by` closure `|a, b| compare_rows(a, b, &sort_keys)` captures `&sort_keys` by reference. This is valid because `sort_keys` lives on the stack within `new()`, and `sort_by` runs synchronously before `new()` returns.

### Step 2.3 — Implement the `compare_rows` helper function

This is the **core sorting logic** — a standalone function that compares two rows by multiple sort keys:

```rust
fn compare_rows(a: &Row, b: &Row, sort_keys: &[(usize, bool)]) -> Ordering {
    for &(col_idx, ascending) in sort_keys {
        let val_a = &a.values[col_idx];
        let val_b = &b.values[col_idx];

        // Use partial_cmp — Data implements PartialOrd
        let cmp = val_a.partial_cmp(val_b).unwrap_or(Ordering::Equal);

        match cmp {
            Ordering::Equal => continue,  // tie → check next sort key
            other => {
                // If descending, reverse the ordering
                return if ascending { other } else { other.reverse() };
            }
        }
    }
    // All sort keys are equal — rows are equivalent in sort order
    Ordering::Equal
}
```

**Line-by-line breakdown:**

1. **Loop through sort keys in order:** The first sort key is the primary sort, second is the tiebreaker, etc.

2. **`partial_cmp`:** `Data` implements `PartialOrd` (not `Ord`), so we get `Option<Ordering>`. We use `.unwrap_or(Ordering::Equal)` as a fallback — this shouldn't happen since the query guarantees type matching, but it's safe.

3. **`Ordering::Equal => continue`:** If two values are equal on this key, move to the next key. This is how multi-column sorting works — you only look at the next key when the current one is a tie.

4. **Ascending vs descending:** For ascending, use the natural ordering. For descending, call `.reverse()` which flips `Less` ↔ `Greater` (and leaves `Equal` unchanged).

**Rust concept — `Ordering::reverse()`:** This is a method on `std::cmp::Ordering`:
- `Ordering::Less.reverse()` → `Ordering::Greater`
- `Ordering::Greater.reverse()` → `Ordering::Less`
- `Ordering::Equal.reverse()` → `Ordering::Equal`

**Why a standalone fn instead of a method?** The sort comparator closure can't borrow `self` (since `sort_by` is called on `self.sorted_rows`). A standalone function avoids borrow conflicts.

### Step 2.4 — Implement the `Operator` trait for `SortOp`

```rust
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
        // Sort doesn't change the schema — same columns as child
        self.output_schema.clone()
    }
}
```

**This is identical to TableScanner's `next()`:** Both iterate through a pre-built `Vec<Row>`. Sort just has the added construction-time sort step.

**Important:** `schema()` returns the **same** column names as the child. Sort doesn't add, remove, or rename columns — it only reorders rows.

---

## Step 3: Register the Module and Integrate

### Step 3.1 — Add `mod sort;` to `main.rs`

Open `database/src/main.rs` and add alongside the other `mod` declarations:

```rust
mod sort;
```

### Step 3.2 — Add the `Sort` case to `build_operator()` in `query_executor.rs`

```rust
use crate::sort::SortOp;  // add this import at the top

// Inside build_operator(), add this arm to the match:
QueryOp::Sort(sort_data) => {
    // Build child first
    let child = build_operator(&sort_data.underlying, ctx, buffer_pool);
    Box::new(SortOp::new(child, sort_data.sort_specs.clone()))
}
```

**Why `sort_data.sort_specs.clone()`?** Same pattern as Filter and Project — `build_operator` takes `&QueryOp`, so we need to give `SortOp::new()` owned data by cloning. This is why `SortSpec` needs `Clone` (Step 1).

### Step 3.3 — Remove the wildcard `_` panic arm

After adding Sort, all 5 operators are implemented! You can change the wildcard to a more descriptive message or remove it. The compiler will tell you if you've missed any variant.

Actually, you should check if `QueryOp` has only 5 variants. If so, the `_` arm becomes unreachable. You can keep it as a safety net:

```rust
_ => unreachable!("All operators should be handled"),
```

Or simply remove it — the compiler will error if a new variant is added later, which is actually better (forces you to implement it).

---

## Step 4: Testing

### Step 4.1 — Test 1: Simple Sort — Nation by name ascending

```sql
SELECT * FROM nation ORDER BY n_name ASC;
```

**AST structure:**
```
Sort(n_name ASC)
  └── Scan(nation)
```

**JSON query for `monitor_config.json`:**
```json
{
  "execution_name": "Sort - Nation by name ASC",
  "disabled": false,
  "query": {
    "root": {
      "Sort": {
        "sort_specs": [
          { "column_name": "n_name", "ascending": true }
        ],
        "underlying": {
          "Scan": {
            "table_id": "nation"
          }
        }
      }
    }
  },
  "expected_output_file": "<RUNTIME_PATH>/expected_sort_nation_name.csv",
  "memory_limit_mb": 64
}
```

**Generate expected output:**
```bash
sqlite3 <COMPILED_PATH>/sqlite.db \
  "SELECT n_nationkey || '|' || n_name || '|' || n_regionkey || '|' || n_comment || '|' FROM nation ORDER BY n_name ASC;" \
  > <RUNTIME_PATH>/expected_sort_nation_name.csv
```

### Step 4.2 — Test 2: Multi-column Sort — Nation by regionkey ASC, name DESC

This tests the multi-key tiebreaker logic:

```sql
SELECT * FROM nation ORDER BY n_regionkey ASC, n_name DESC;
```

**JSON query:**
```json
{
  "execution_name": "Sort - Nation by regionkey ASC, name DESC",
  "disabled": false,
  "query": {
    "root": {
      "Sort": {
        "sort_specs": [
          { "column_name": "n_regionkey", "ascending": true },
          { "column_name": "n_name", "ascending": false }
        ],
        "underlying": {
          "Scan": {
            "table_id": "nation"
          }
        }
      }
    }
  },
  "expected_output_file": "<RUNTIME_PATH>/expected_sort_nation_regionkey_name.csv",
  "memory_limit_mb": 64
}
```

**Generate expected output:**
```bash
sqlite3 <COMPILED_PATH>/sqlite.db \
  "SELECT n_nationkey || '|' || n_name || '|' || n_regionkey || '|' || n_comment || '|' FROM nation ORDER BY n_regionkey ASC, n_name DESC;" \
  > <RUNTIME_PATH>/expected_sort_nation_regionkey_name.csv
```

### Step 4.3 — Test 3: Sort + Project — Full pipeline

Tests the "Sort → Project preserves order" semantics from the assignment. Sort by `n_regionkey`, then project to just `(n_name, n_regionkey)`:

```sql
SELECT n_name, n_regionkey FROM nation ORDER BY n_regionkey ASC, n_name ASC;
```

**AST structure (note Sort is BELOW Project):**
```
Project(n_name → name, n_regionkey → region)
  └── Sort(n_regionkey ASC, n_name ASC)
       └── Scan(nation)
```

**JSON query:**
```json
{
  "execution_name": "Sort+Project - Nation sorted and projected",
  "disabled": false,
  "query": {
    "root": {
      "Project": {
        "column_name_map": [
          ["n_name", "name"],
          ["n_regionkey", "region"]
        ],
        "underlying": {
          "Sort": {
            "sort_specs": [
              { "column_name": "n_regionkey", "ascending": true },
              { "column_name": "n_name", "ascending": true }
            ],
            "underlying": {
              "Scan": {
                "table_id": "nation"
              }
            }
          }
        }
      }
    }
  },
  "expected_output_file": "<RUNTIME_PATH>/expected_sort_project_nation.csv",
  "memory_limit_mb": 64
}
```

**Generate expected output:**
```bash
sqlite3 <COMPILED_PATH>/sqlite.db \
  "SELECT n_name || '|' || n_regionkey || '|' FROM nation ORDER BY n_regionkey ASC, n_name ASC;" \
  > <RUNTIME_PATH>/expected_sort_project_nation.csv
```

### Step 4.4 — Build & Run

```bash
cargo build -r --bin database
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

> [!IMPORTANT]
> Replace `<RUNTIME_PATH>` with the full absolute path to `scratch/runtimes/tpch/` and `<COMPILED_PATH>` with the full absolute path to `scratch/compiled_datasets/tpch/`.

---

## Step 5: Edge Cases to Think About

### 5.1 — Empty Input
If the child produces zero rows, `sorted_rows` will be empty. `next()` returns `None` immediately. No issue.

### 5.2 — Single Sort Key
Most common case. The comparator just checks one column and returns immediately. No tiebreaker needed.

### 5.3 — All Rows Equal on Sort Key
For example, sorting `region` by a column where all values are the same. The comparator returns `Equal` for all pairs, so `sort_by` preserves the relative order (Rust's sort is **stable**). This is actually a nice property — stable sort means tied rows keep their original order.

### 5.4 — Descending Sort
Test this explicitly (Test 2 has `n_name DESC`). Make sure `.reverse()` is applied correctly — a common bug is reversing only some comparisons.

### 5.5 — String Sorting
The assignment says "String comparison is lexicographic by ASCII code point." Rust's `String::partial_cmp` does exactly this by default — lexicographic comparison of UTF-8 bytes (which is the same as ASCII code point order for ASCII-only strings). No special handling needed.

### 5.6 — Sort with Float Columns
`Data::Float32` and `Data::Float64` implement `PartialOrd` through our custom `partial_cmp`. Floats have `NaN` which makes `partial_cmp` return `None`. Our `unwrap_or(Ordering::Equal)` treats `NaN` comparisons as equal, which is a reasonable default. The assignment says queries are well-typed, so NaN shouldn't appear in practice.

### 5.7 — Sort After Cross+Filter (Join)
A query like:
```
Sort(n_name ASC)
  └── Filter(r_regionkey = n_regionkey)
       └── Cross(region, nation)
```
The Sort will receive 25 rows (join result) and sort them. This tests that Sort works correctly on non-trivial input from upstream operators.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `common/src/query.rs` | **[MODIFY]** | Add `Clone` derive to `SortSpec` |
| `database/src/sort.rs` | **[NEW]** | `SortOp` struct, `new()` with multi-key sort, `compare_rows()` helper, `Operator` impl |
| `database/src/main.rs` | **[MODIFY]** | Add `mod sort;` |
| `database/src/query_executor.rs` | **[MODIFY]** | Add `use crate::sort::SortOp;`, add `QueryOp::Sort` match arm |
| `scratch/runtimes/tpch/monitor_config.json` | **[MODIFY]** | Add 3 Sort test queries |
| `scratch/runtimes/tpch/expected_sort_*.csv` | **[NEW]** | 3 expected output files from SQLite |

---

## Quick Reference: Full `sort.rs` Structure

```
sort.rs
├── use statements (Ordering, HashMap, SortSpec, Operator, Row)
│
├── pub struct SortOp {
│       sorted_rows: Vec<Row>,
│       current_index: usize,
│       output_schema: Vec<String>
│   }
│
├── fn compare_rows(a: &Row, b: &Row, sort_keys: &[(usize, bool)]) → Ordering
│   └── loop through sort_keys:
│       ├── partial_cmp values
│       ├── if Equal → continue to next key
│       └── if not Equal → return (reversed if descending)
│
├── impl SortOp
│   └── pub fn new(child, sort_specs) → Self
│       ├── capture schema
│       ├── build col_index_map
│       ├── pre-compute sort_keys: Vec<(usize, bool)>
│       ├── materialize all rows
│       └── sort_by(compare_rows)
│
└── impl Operator for SortOp
    ├── fn next() → Option<Row>    // index-based iteration (same as TableScanner)
    └── fn schema() → Vec<String>  // same schema as child
```

---

## Checkpoint: What "Done" Looks Like

- [ ] `SortSpec` in `common/src/query.rs` derives `Clone`
- [ ] `database/src/sort.rs` exists with `SortOp`, `new()`, `compare_rows()`, and `Operator` impl
- [ ] `main.rs` has `mod sort;`
- [ ] `query_executor.rs` handles `QueryOp::Sort` and builds `SortOp`
- [ ] `cargo build -r --bin database` compiles with no errors
- [ ] The wildcard `_` arm in `build_operator()` is now unreachable (all 5 operators handled)
- [ ] Monitor config has Sort, Multi-key Sort, and Sort+Project test queries
- [ ] `Validation success!` for all Sort test queries ✅
- [ ] **All 5 query operators (Scan, Filter, Project, Cross, Sort) are now implemented!** 🎉

---
---

# Phase 2: External Sort Extension

> [!IMPORTANT]
> **Prerequisite:** Complete Phase 1 first and verify all Sort tests pass. Phase 2 upgrades the in-memory `SortOp` to a proper external merge sort that works within the memory limit. This is critical for large tables like `lineitem` (~6M rows) that cannot fit in memory.

## Why External Sort?

The in-memory sort from Phase 1 works fine for small tables (`nation` = 25 rows, `region` = 5 rows). But consider sorting `lineitem` with ~6 million rows:
- Each `lineitem` row is ~200+ bytes (16 columns, mix of Int64/Float64/String)
- 6M × 200 bytes ≈ 1.2 GB — far exceeds the 64MB memory limit

External sort solves this by:
1. **Sorting chunks** that fit in memory ("runs")
2. **Writing runs to disk** (anonymous blocks)
3. **Merging runs** using a k-way merge with minimal memory

```
                    ┌────────── External Sort ──────────┐
                    │                                   │
  Phase 1:          │   Read N rows → Sort → Write      │
  Run Creation      │   Read N rows → Sort → Write      │  → K sorted runs on disk
                    │   Read N rows → Sort → Write      │
                    │   ...                              │
                    │                                   │
  Phase 2:          │   Open all K runs simultaneously   │
  K-way Merge       │   Use min-heap to merge            │  → 1 fully sorted output
                    │   Emit smallest row each time      │
                    └───────────────────────────────────┘
```

---

## Step 6: Row Serialization — `encode_row()`

You already have `decode_row()` that reads bytes → `Row`. Now you need the **reverse**: `encode_row()` that converts a `Row` back to bytes.

### Step 6.1 — Add `encode_row` to `row.rs`

```rust
pub fn encode_row(row: &Row) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in &row.values {
        match value {
            Data::Int32(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::Int64(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::Float32(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::Float64(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::String(v) => {
                bytes.extend_from_slice(v.as_bytes());
                bytes.push(0x00); // null terminator
            }
        }
    }
    bytes
}
```

**Why this is the exact reverse of `decode_row`:**
- `Int32`: `decode` reads 4 bytes as `i32::from_le_bytes` → `encode` writes `v.to_le_bytes()` (4 bytes)
- `Int64`: 8 bytes, same pattern
- `Float32/Float64`: same pattern for IEEE-754
- `String`: `decode` reads until `0x00` null byte → `encode` writes UTF-8 bytes + `0x00`

> [!TIP]
> **Symmetry check:** For any row, `decode_row(encode_row(&row), schema)` should give back the same row (modulo the offset). This is a good unit test to write.

**Key difference from `decode_row`:** `encode_row` doesn't need a schema! The `Data` enum already carries type information. However, `decode_row` needs the schema because raw bytes don't carry type tags.

### Step 6.2 — Add `encode_block` helper

This packs multiple rows into a single block-sized buffer, following the exact same format as the original `.bin` files:

```rust
pub fn encode_block(rows: &[Row], block_size: usize) -> Vec<u8> {
    let mut block = vec![0u8; block_size];
    let mut offset = 0;
    let usable_space = block_size - 2; // last 2 bytes = row_count

    let mut row_count: u16 = 0;
    for row in rows {
        let encoded = encode_row(row);
        if offset + encoded.len() > usable_space {
            break; // row doesn't fit, stop packing
        }
        block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count += 1;
    }

    // Write row_count footer (last 2 bytes, u16 little-endian)
    block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
    block
}
```

**Block format reminder:**
```
byte 0: <Row 1 data><Row 2 data>...<Row N data>...(unused padding)...<row_count: u16 LE>
byte block_size-1
```

**Important:** A row must be fully contained within a single block — it never spans blocks. If a row doesn't fit in the remaining space, it goes into the next block.

---

## Step 7: Anonymous Block Management

### Step 7.1 — Add `get_anon_start_block` passthrough to `BufferPool`

The buffer pool already wraps the disk manager. Add a method to expose the anon region start:

```rust
// In buffer_pool.rs, add alongside existing passthroughs:
pub fn get_anon_start_block(&mut self) -> u64 {
    self.disk_manager.get_anon_start_block().unwrap()
}
```

### Step 7.2 — Add `write_block` method to `BufferPool`

Currently, the buffer pool only **reads** blocks. For external sort, you need to **write** to anonymous blocks. Add a direct write method:

```rust
/// Write a block directly to disk (bypassing the cache).
/// Used for writing sorted runs to the anonymous region.
pub fn write_block(&mut self, block_id: u64, data: &[u8]) {
    self.disk_manager.write_blocks(block_id, data).unwrap();
}
```

**Design choice — bypass vs cache:** For sorted runs, we write once and read once later during merge. Caching these intermediate blocks would pollute the buffer pool cache (evicting useful table data). A direct write-through is simpler and more cache-friendly.

> [!NOTE]
> Alternatively, you could load the block into a buffer pool frame, mark it dirty, and let eviction write it back. But direct writing is simpler for the initial implementation.

### Step 7.3 — Create `AnonBlockAllocator`

You need a way to allocate fresh anonymous block IDs. Since the anonymous region starts at `anon_start_block` and extends to 2⁶⁴ - 1, you can use a simple counter:

```rust
// Can live in sort.rs or a new file anon_allocator.rs
pub struct AnonBlockAllocator {
    next_block_id: u64,
}

impl AnonBlockAllocator {
    pub fn new(anon_start_block: u64) -> Self {
        AnonBlockAllocator {
            next_block_id: anon_start_block,
        }
    }

    pub fn allocate(&mut self, num_blocks: u64) -> u64 {
        let start = self.next_block_id;
        self.next_block_id += num_blocks;
        start
    }
}
```

**Why so simple?** The anonymous region is sparse — the disk simulator only allocates RAM for blocks you actually write. So sequential block IDs are fine (no fragmentation concern). We never "free" blocks in this simple implementation.

> [!WARNING]
> **Memory safety:** Each block you write costs `block_size` bytes of RAM in the disk simulator. For a 4KB block, writing 1000 blocks = 4MB. With the ~10GB allowed anonymous space, this gives you ~2.5 million blocks. For sorting `lineitem` (6M rows × ~200 bytes / 4094 usable bytes per block ≈ ~300 blocks per run), you'd need a few hundred blocks total — well within limits.

---

## Step 8: Sorted Run Creation (External Sort Phase 1)

A "run" is a sequence of consecutive anonymous blocks containing sorted rows.

### Step 8.1 — Define the `Run` struct

```rust
struct Run {
    start_block: u64,    // first anonymous block ID
    num_blocks: u64,     // number of blocks in this run
    num_rows: usize,     // total rows in the run (for debugging)
}
```

### Step 8.2 — Implement `create_sorted_runs()`

This function reads rows from the child operator in chunks, sorts each chunk, and writes it to anonymous blocks:

```rust
fn create_sorted_runs(
    child: &mut Box<dyn Operator>,
    sort_keys: &[(usize, bool)],
    schema: &[ColumnSpec],
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
    allocator: &mut AnonBlockAllocator,
    memory_budget_rows: usize,    // how many rows fit in the memory budget
    block_size: usize,
) -> Vec<Run> {
    let mut runs = Vec::new();

    loop {
        // 1. Fill a buffer with up to memory_budget_rows rows
        let mut buffer: Vec<Row> = Vec::new();
        for _ in 0..memory_budget_rows {
            match child.next() {
                Some(row) => buffer.push(row),
                None => break,
            }
        }
        if buffer.is_empty() {
            break; // all input consumed
        }

        // 2. Sort this buffer in-memory
        buffer.sort_by(|a, b| compare_rows(a, b, sort_keys));

        // 3. Serialize sorted rows into blocks and write to anonymous region
        let blocks = rows_to_blocks(&buffer, schema, block_size);
        let num_blocks = blocks.len() as u64;
        let start_block = allocator.allocate(num_blocks);

        for (i, block_data) in blocks.iter().enumerate() {
            buffer_pool.write_block(start_block + i as u64, block_data);
        }

        runs.push(Run {
            start_block,
            num_blocks,
            num_rows: buffer.len(),
        });
    }

    runs
}
```

### Step 8.3 — Implement `rows_to_blocks()` helper

Converts a `Vec<Row>` into `Vec<Vec<u8>>` (multiple block buffers):

```rust
fn rows_to_blocks(rows: &[Row], schema: &[ColumnSpec], block_size: usize) -> Vec<Vec<u8>> {
    let usable_space = block_size - 2;
    let mut blocks = Vec::new();
    let mut current_block = vec![0u8; block_size];
    let mut offset = 0;
    let mut row_count: u16 = 0;

    for row in rows {
        let encoded = encode_row(row);

        if offset + encoded.len() > usable_space {
            // Current block is full — finalize it
            current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
            blocks.push(current_block);

            // Start new block
            current_block = vec![0u8; block_size];
            offset = 0;
            row_count = 0;
        }

        current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count += 1;
    }

    // Don't forget the last block
    if row_count > 0 {
        current_block[block_size - 2..].copy_from_slice(&row_count.to_le_bytes());
        blocks.push(current_block);
    }

    blocks
}
```

**How this works:**
1. Start with an empty block buffer
2. For each row, encode it to bytes
3. If it fits in the current block, append it
4. If it doesn't fit, finalize the current block (write row_count footer) and start a new one
5. After all rows, finalize the last block

**Edge case:** A single row that's larger than `usable_space` (block_size - 2). The assignment guarantees "A row is always fully contained within a single block" — so this can't happen for the provided data. But you might want to add a panic/assert for safety.

---

## Step 9: K-Way Merge (External Sort Phase 2)

This is the most complex part. You need to simultaneously read from K sorted runs and merge them into a single sorted output.

### Step 9.1 — Implement `RunReader`

A `RunReader` reads rows one-at-a-time from a run stored in anonymous blocks:

```rust
struct RunReader {
    start_block: u64,
    num_blocks: u64,
    current_block_idx: u64,    // which block within the run (0-based)
    current_row_idx: usize,    // which row within the current block
    current_block_rows: Vec<Row>,  // decoded rows from the current block
    schema: Vec<ColumnSpec>,       // needed for decode_row
    exhausted: bool,
}
```

### Step 9.2 — Implement `RunReader` methods

```rust
impl RunReader {
    fn new(
        run: &Run,
        schema: Vec<ColumnSpec>,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) -> Self {
        let first_block_id = run.start_block;
        let block_data = buffer_pool.fetch_block(first_block_id);
        buffer_pool.unpin(first_block_id);
        let rows = decode_block(&block_data, &schema);

        RunReader {
            start_block: run.start_block,
            num_blocks: run.num_blocks,
            current_block_idx: 0,
            current_row_idx: 0,
            current_block_rows: rows,
            schema,
            exhausted: false,
        }
    }

    fn peek(&self) -> Option<&Row> {
        if self.exhausted {
            return None;
        }
        self.current_block_rows.get(self.current_row_idx)
    }

    fn advance(
        &mut self,
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
    ) {
        self.current_row_idx += 1;
        if self.current_row_idx >= self.current_block_rows.len() {
            // Move to next block in the run
            self.current_block_idx += 1;
            if self.current_block_idx >= self.num_blocks {
                self.exhausted = true;
                return;
            }
            // Fetch next block
            let block_id = self.start_block + self.current_block_idx;
            let block_data = buffer_pool.fetch_block(block_id);
            buffer_pool.unpin(block_id);
            self.current_block_rows = decode_block(&block_data, &self.schema);
            self.current_row_idx = 0;
        }
    }
}
```

**How RunReader works:**
- **Construction:** Fetches the first block of the run, decodes its rows
- **`peek()`:** Returns a reference to the current row without consuming it — needed for the heap comparator
- **`advance()`:** Moves to the next row; if the current block is exhausted, fetches the next block from disk

**Memory efficiency:** At any time, only ONE block per run is in memory (via the buffer pool). For K runs, that's K blocks × block_size bytes. With 4KB blocks and 64MB memory, you could merge **16,384 runs** simultaneously. In practice, you'll have far fewer runs.

### Step 9.3 — Implement the Min-Heap Merge

Rust's `BinaryHeap` is a **max-heap** by default. For a min-merge, you need to reverse the ordering.

```rust
use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct HeapEntry {
    row: Row,
    run_index: usize,  // which run this row came from
    sort_keys: Vec<(usize, bool)>,  // shared reference (via Rc or clone)
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
        // REVERSE the ordering for min-heap behavior!
        // BinaryHeap is a max-heap, so reversing makes smallest = highest priority
        compare_rows(&other.row, &self.row, &self.sort_keys)
    }
}
```

**Why reversed?** `BinaryHeap::pop()` returns the **maximum** element. By reversing the comparison in `cmp()`, the "smallest" row (in sort order) becomes the "largest" in heap order, so it gets popped first.

### Step 9.4 — Implement `merge_runs()`

```rust
fn merge_runs(
    runs: &[Run],
    sort_keys: Vec<(usize, bool)>,
    schema: Vec<ColumnSpec>,
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
) -> Vec<Row> {
    // 1. Create a RunReader for each run
    let mut readers: Vec<RunReader> = runs
        .iter()
        .map(|run| RunReader::new(run, schema.clone(), buffer_pool))
        .collect();

    // 2. Initialize the heap with the first row from each reader
    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter().enumerate() {
        if let Some(row) = reader.peek() {
            heap.push(HeapEntry {
                row: row.clone(),
                run_index: i,
                sort_keys: sort_keys.clone(),
            });
        }
    }

    // 3. Merge loop
    let mut result = Vec::new();
    while let Some(entry) = heap.pop() {
        result.push(entry.row);

        // Advance the reader this row came from
        let reader = &mut readers[entry.run_index];
        reader.advance(buffer_pool);

        // If there's another row in that run, push it onto the heap
        if let Some(next_row) = reader.peek() {
            heap.push(HeapEntry {
                row: next_row.clone(),
                run_index: entry.run_index,
                sort_keys: sort_keys.clone(),
            });
        }
    }

    result
}
```

**How the merge works step-by-step:**

Imagine 3 runs, each sorted:
```
Run 0: [A, D, G]
Run 1: [B, E, H]
Run 2: [C, F, I]
```

| Step | Heap (min first) | Pop | Push | Result |
|------|-----------------|-----|------|--------|
| Init | A₀, B₁, C₂ | | | [] |
| 1 | B₁, C₂ | A₀ | D₀ | [A] |
| 2 | C₂, D₀ | B₁ | E₁ | [A,B] |
| 3 | D₀, E₁ | C₂ | F₂ | [A,B,C] |
| 4 | E₁, F₂ | D₀ | G₀ | [A,B,C,D] |
| ... | ... | ... | ... | ... |
| 9 | (empty) | I₂ | — | [A,B,...,I] |

Each `pop` gives the globally smallest unprocessed row. The heap maintains the invariant that it always contains the next candidate from each active run.

**Complexity:**
- Heap operations: O(log K) per row, where K = number of runs
- Total: O(N log K) where N = total rows
- Memory: O(K × block_size) for the run readers + O(K) for the heap

---

## Step 10: Refactor `SortOp` — Decision Logic

Now refactor `SortOp::new()` to **choose** between in-memory sort and external sort:

### Step 10.1 — Updated `SortOp` struct

```rust
pub struct SortOp {
    sorted_rows: Vec<Row>,
    current_index: usize,
    output_schema: Vec<String>,
}
```

The struct stays the same! The difference is in `new()` — it might use external sort to populate `sorted_rows`.

### Step 10.2 — Updated `SortOp::new()` with external sort path

The key challenge: `SortOp::new()` currently doesn't have access to the buffer pool, block size, or column specs. You'll need to pass these in.

**Option A — Expand `new()` signature (recommended):**
```rust
impl SortOp {
    pub fn new(
        child: Box<dyn Operator>,
        sort_specs: Vec<SortSpec>,
        column_specs: Vec<ColumnSpec>,   // NEW: needed for encode/decode
        buffer_pool: &mut BufferPool<impl Read, impl Write>,  // NEW: for disk I/O
    ) -> Self {
        let output_schema = child.schema();
        // ... build sort_keys ...

        // Decision: use in-memory or external sort?
        let memory_budget_rows = estimate_memory_budget(block_size, &column_specs);

        // Try in-memory first: materialize all rows
        let mut all_rows = Vec::new();
        while let Some(row) = child.next() {
            all_rows.push(row);
            if all_rows.len() > memory_budget_rows {
                // Too many rows — switch to external sort
                return Self::external_sort(
                    all_rows, child, sort_keys, column_specs,
                    buffer_pool, output_schema,
                );
            }
        }

        // All rows fit in memory — simple sort
        all_rows.sort_by(|a, b| compare_rows(a, b, &sort_keys));
        SortOp { sorted_rows: all_rows, current_index: 0, output_schema }
    }
}
```

**Why both paths?** In-memory sort is faster (no disk I/O overhead). External sort is only needed when data exceeds memory. The decision point is `memory_budget_rows` — a rough estimate of how many rows fit in memory.

### Step 10.3 — Estimating Memory Budget

```rust
fn estimate_memory_budget(block_size: usize, column_specs: &[ColumnSpec]) -> usize {
    // Rough estimate: calculate average row size from column types
    let fixed_size: usize = column_specs.iter().map(|c| match c.data_type {
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::String => 50,  // assume average string length ~50 bytes
    }).sum();

    let row_overhead = 24 + column_specs.len() * 32; // Vec<Data> allocation overhead
    let effective_row_size = fixed_size + row_overhead;

    // Use ~70% of available buffer pool frames for sort buffer
    // (leaving 30% for run readers and other operators)
    let available_memory = block_size * 1000; // rough estimate
    available_memory / effective_row_size
}
```

> [!TIP]
> This estimate doesn't need to be perfect. Being slightly conservative (underestimating capacity) is safer — it just creates more, smaller runs. The k-way merge handles any number of runs efficiently.

### Step 10.4 — Update `build_operator` call site

Since `SortOp::new()` now needs `buffer_pool` and `column_specs`, update the call in `query_executor.rs`:

```rust
QueryOp::Sort(sort_data) => {
    let child = build_operator(&sort_data.underlying, ctx, buffer_pool);

    // Get column specs from the child's schema columns
    // We need ColumnSpec for encode/decode — reconstruct from child schema + ctx
    let child_schema = child.schema();
    let column_specs = resolve_column_specs(&child_schema, ctx);

    Box::new(SortOp::new(
        child,
        sort_data.sort_specs.clone(),
        column_specs,
        buffer_pool,
    ))
}
```

**Challenge — getting `ColumnSpec` from schema names:** The child operator only exposes column *names* via `schema()`, but for encoding/decoding you need the *types*. You'll need to look up each column name in the `DbContext` table specs to find its `DataType`. This is the `resolve_column_specs` helper:

```rust
fn resolve_column_specs(schema: &[String], ctx: &DbContext) -> Vec<ColumnSpec> {
    schema.iter().map(|col_name| {
        // Search all tables for this column name
        for table in ctx.get_table_specs() {
            for cs in &table.column_specs {
                if cs.column_name == *col_name {
                    return cs.clone();
                }
            }
        }
        panic!("Column '{}' not found in any table", col_name);
    }).collect()
}
```

> [!WARNING]
> **Limitation:** This lookup assumes column names are globally unique across tables. For the TPCH schema this is true (all columns are prefixed: `r_regionkey`, `n_nationkey`, etc.). For schemas with name collisions, you'd need a more sophisticated approach.

---

## Step 11: Testing the External Sort

### Step 11.1 — Test with a larger table

The `customer` table has ~150,000 rows — large enough to trigger external sort with a small memory limit:

```json
{
  "execution_name": "Sort - Customer by name ASC (external sort)",
  "disabled": false,
  "query": {
    "root": {
      "Sort": {
        "sort_specs": [
          { "column_name": "c_name", "ascending": true }
        ],
        "underlying": {
          "Scan": {
            "table_id": "customer"
          }
        }
      }
    }
  },
  "expected_output_file": "<RUNTIME_PATH>/expected_sort_customer_name.csv",
  "memory_limit_mb": 64
}
```

**Generate expected output:**
```bash
sqlite3 <COMPILED_PATH>/sqlite.db \
  "SELECT c_custkey || '|' || c_name || '|' || c_address || '|' || c_nationkey || '|' || c_phone || '|' || c_acctbal || '|' || c_mktsegment || '|' || c_comment || '|' FROM customer ORDER BY c_name ASC;" \
  > <RUNTIME_PATH>/expected_sort_customer_name.csv
```

> [!WARNING]
> **Float formatting:** The `customer` table has `c_acctbal` which is `Float64`. Your `Row::Display` uses default Rust float formatting, which might not match SQLite's output exactly. This may cause validation failure on this test. The float formatting fix is planned for a later day. For initial external sort testing, you might want to use a query that **projects out** the float column, or compare output manually.

### Step 11.2 — Test with Sort + Join

Sort the result of a Cross+Filter join:

```json
{
  "execution_name": "Sort - Sorted Join (region ⋈ nation by name)",
  "disabled": false,
  "query": {
    "root": {
      "Sort": {
        "sort_specs": [
          { "column_name": "n_name", "ascending": true }
        ],
        "underlying": {
          "Filter": {
            "predicates": [{
              "column_name": "r_regionkey",
              "operator": "EQ",
              "value": { "Column": "n_regionkey" }
            }],
            "underlying": {
              "Cross": {
                "left": { "Scan": { "table_id": "region" } },
                "right": { "Scan": { "table_id": "nation" } }
              }
            }
          }
        }
      }
    }
  },
  "expected_output_file": "<RUNTIME_PATH>/expected_sort_join_region_nation.csv",
  "memory_limit_mb": 64
}
```

---

## Step 12: Edge Cases for External Sort

### 12.1 — Single Run (All Data Fits)
If all input fits in one run, the merge phase has K=1 — it just reads one run sequentially. This degenerates to reading back what you just wrote, which is slightly slower than in-memory sort. The decision logic in Step 10.2 avoids this by staying in-memory when possible.

### 12.2 — Very Many Small Runs
If memory is extremely constrained and each run is tiny, you might get hundreds of runs. The k-way merge handles this — heap operations are O(log K) which is fine even for K=1000.

### 12.3 — Multi-Pass Merge (K > B-1)
In theory, if you have more runs than available buffer frames (K > B-1), you can't merge all runs at once. You'd need multiple merge passes: merge B-1 runs into one, repeat until only one remains.

**For this assignment:** With 4KB blocks and 64MB memory = 16,384 frames, you can merge up to 16,383 runs simultaneously. Each run holds at least one block-worth of rows, so you'd need >16K runs, meaning the original data exceeds 16K × 1 block = 64MB of raw data per run × 16K = this won't happen for TPCH scale. **Multi-pass merge is not needed for this assignment.**

### 12.4 — Empty Runs
If the child produces zero rows, you get zero runs. The merge function returns an empty Vec. This is handled naturally.

---

## Files You Will Create/Modify (Phase 2 additions)

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/row.rs` | **[MODIFY]** | Add `encode_row()` and `encode_block()` |
| `database/src/buffer_pool.rs` | **[MODIFY]** | Add `get_anon_start_block()` and `write_block()` |
| `database/src/sort.rs` | **[MODIFY]** | Add `AnonBlockAllocator`, `Run`, `RunReader`, `HeapEntry`, `create_sorted_runs()`, `rows_to_blocks()`, `merge_runs()`, refactor `SortOp::new()` |
| `database/src/query_executor.rs` | **[MODIFY]** | Update `QueryOp::Sort` arm to pass `buffer_pool` and `column_specs` |
| `scratch/runtimes/tpch/monitor_config.json` | **[MODIFY]** | Add external sort test queries |

---

## Quick Reference: External Sort Architecture

```
External Sort Pipeline:

  ┌─── Phase 1: Run Creation ────────────────────────┐
  │                                                   │
  │  Child.next() → buffer[] → sort_by() → encode()  │
  │                     ↓                             │
  │  buffer_pool.write_block(anon_id, block_data)     │
  │  Run { start_block, num_blocks }                  │
  │                                                   │
  │  Repeat until child exhausted                     │
  └───────────────────────────────────────────────────┘
                        ↓
           Vec<Run>:  [Run₀, Run₁, Run₂, ...]
                        ↓
  ┌─── Phase 2: K-Way Merge ─────────────────────────┐
  │                                                   │
  │  RunReader₀ ──┐                                   │
  │  RunReader₁ ──┼──→ BinaryHeap ──→ sorted output   │
  │  RunReader₂ ──┘    (min-heap)                     │
  │                                                   │
  │  Each reader fetches blocks on demand             │
  │  via buffer_pool.fetch_block(anon_id)             │
  └───────────────────────────────────────────────────┘
                        ↓
           SortOp { sorted_rows: merged_result }
```

---

## Phase 2 Checkpoint: What "Done" Looks Like

- [ ] `encode_row()` in `row.rs` correctly serializes all Data types
- [ ] `encode_block()` in `row.rs` packs rows into block-sized buffers with row_count footer
- [ ] `buffer_pool.rs` has `get_anon_start_block()` and `write_block()` methods
- [ ] `AnonBlockAllocator` provides monotonically increasing block IDs
- [ ] `create_sorted_runs()` reads child in chunks, sorts, writes to anonymous blocks
- [ ] `RunReader` reads anonymous blocks and deserializes rows one at a time
- [ ] `HeapEntry` with reversed `Ord` for min-heap behavior
- [ ] `merge_runs()` produces correctly merged output
- [ ] `SortOp::new()` chooses in-memory vs external based on data size
- [ ] All Phase 1 sort tests still pass ✅
- [ ] Customer sort test with external sort path passes ✅
- [ ] Sort+Join test passes ✅
