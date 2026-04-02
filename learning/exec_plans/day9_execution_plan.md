# Day 8 Verification + Day 9 Execution Plan

---

## Part 1: Day 8 Verification — Filter Operator

### Checklist from Day 8 Plan

| # | Checkpoint | Status | Notes |
|---|------------|--------|-------|
| 1 | `database/src/filter.rs` exists with `FilterOp`, predicate evaluation, and `Operator` impl | ✅ Pass | All functions present: `FilterOp` struct, `new()`, `Operator` impl, `resolve_value()`, `evaluate_predicate()`, `evaluate_all_predicates()` |
| 2 | `main.rs` has `mod filter;` | ✅ Pass | Line 21: `mod filter;` |
| 3 | `query_executor.rs` handles `QueryOp::Filter` and builds `FilterOp` | ✅ Pass | Lines 25-30: correct recursive build + FilterOp wrapping |
| 4 | `common/src/query.rs` types derive `Clone` | ✅ Pass | `ComparisionOperator`, `ComparisionValue`, `Predicate` all have `#[derive(Clone)]` |
| 5 | `cargo build -r --bin database` compiles with no **errors** | ✅ Pass | Build succeeds (4 warnings only, no errors) |
| 6 | Monitor config has a Filter test query | ✅ Pass | `monitor_config.json` lines 37-63: "Filter - Region key >= 2" |
| 7 | `Validation success!` for Filter query | ❓ Not verified | Need manual run to confirm |

### Code Quality Review

#### `filter.rs` — Excellent ✅
- `resolve_value()`: Correctly handles all 6 `ComparisionValue` variants (Column lookup + 5 literal types).
- `evaluate_predicate()`: Uses `partial_cmp` with pattern matching for GT/GTE/LT/LTE — this is the correct approach for `Data` which implements `PartialOrd` but not `Ord`.
- `evaluate_all_predicates()`: Uses `.all()` for short-circuiting AND logic.
- `FilterOp::new()`: Correctly caches `col_index_map` from `child.schema()` at construction time.
- `Operator::next()`: Correct `while let Some(row)` loop pattern.
- `Operator::schema()`: Correctly delegates to child — filter doesn't change the schema.

#### `query_executor.rs` — Correct ✅
- `QueryOp::Filter` arm: Recursively builds child first, then wraps with `FilterOp::new()`.
- Uses `filter_data.predicates.clone()` — necessary since `build_operator` takes `&QueryOp`.
- Wildcard `_` arm panics for unimplemented operators — fine for now, will need updating.

#### `common/src/query.rs` — Correct ✅
- `FilterData` does **not** derive `Clone` (line 36), but that's fine — the `build_operator` function only needs to clone `predicates` (which is `Vec<Predicate>` — cloneable), not the entire `FilterData` (which contains `Box<QueryOp>` requiring recursive cloning).

### Minor Issues / Warnings

> [!NOTE]
> **4 compiler warnings exist** (all are "unused" warnings, not correctness issues):
> 1. `unused import: operator::Operator` in `main.rs` — safe to clean up
> 2. `method get_anon_start_block is never used` — will be needed for Sort/anonymous blocks
> 3. `field column_specs is never read` in `TableScanner` — can remove or mark with `_`
> 4. `method mark_dirty is never used` — will be needed for anonymous block writes

> [!WARNING]
> **Float formatting in `Row::Display`** (row.rs lines 52-53): `Float32` and `Float64` use Rust's default `{}` formatting, which may not match SQLite's output format. The assignment says float output must match SQLite's `printf("%.15g", value)` for f64 and `printf("%.7g", value)` for f32. This is **not a Day 8 concern** (no float predicates in the test query), but will need addressing before full TPCH validation. The report plans this for Day 19-20.

> [!NOTE]
> **TableScanner pre-loads all rows into memory** (table_scanner.rs lines 26-33). This works for small tables like `region` (5 rows) but will **exceed the 64 MB memory limit** for large tables like `lineitem` (~6M rows). This is an architectural concern for later optimization, but worth being aware of. The proper fix is to make TableScanner truly streaming (fetch blocks one at a time in `next()`), but that's a separate task.

### Verdict: Day 8 is correctly implemented ✅

---

## Part 2: Day 9 Execution Plan — Project Operator with Schema Tracking

### Goal
Build a `ProjectOp` operator that wraps a child operator and **selects + renames** a subset of columns. By the end of today, you'll run a Scan + Filter + Project query and get `Validation success!` from the monitor.

---

### Background Concepts (Read This First!)

#### What Is a Project Operator?
In SQL, `SELECT a1 AS id, b2 FROM ...` is a projection — it picks specific columns and optionally renames them. In the query tree, `Project` sits above another operator and transforms each row by:
1. Selecting only the columns listed in `column_name_map`
2. Renaming them (from → to)
3. Outputting rows with **fewer columns** (potentially) and **new column names**

```
         ProjectOp           ← YOU BUILD THIS TODAY
            │
         FilterOp             ← built on Day 8
            │
        TableScanner           ← built on Day 6-7
```

When `main.rs` calls `project_op.next()`:
1. ProjectOp calls `self.child.next()` to get a row from the child.
2. It picks the specified columns by index, building a new `Row` with only those values.
3. It returns the new (narrower) row.
4. When the child returns `None`, ProjectOp returns `None`.

#### What Does `column_name_map` Look Like?
Looking at `common/src/query.rs`:

```rust
pub struct ProjectData {
    pub column_name_map: Vec<(String, String)>,  // Vec of (from_name, to_name)
    pub underlying: Box<QueryOp>,
}
```

Each tuple is `(input_column_name, output_column_name)`:
- `("r_regionkey", "id")` → take the `r_regionkey` column, output it as `id`
- `("r_name", "r_name")` → take `r_name`, keep the same name

**Key difference from Filter:** Project **changes** the schema. The output has different column names and potentially fewer columns than the input.

#### Why Schema Tracking Matters
Every operator implements `schema() -> Vec<String>`. Downstream operators use this to find column indices. If ProjectOp's schema is wrong, any Filter or Sort above it will look up wrong column indices and crash or produce wrong results.

Example chain:
```
Child schema:   ["r_regionkey", "r_name", "r_comment"]     (3 columns)
Project map:    [("r_regionkey", "id"), ("r_name", "name")]
Output schema:  ["id", "name"]                              (2 columns)
Output row:     [values[0], values[1]]                      (only 2 values)
```

---

### Step 1: Create `database/src/project.rs`

Create a new file `database/src/project.rs`.

#### Step 1.1 — Define the `ProjectOp` struct

```rust
use std::collections::HashMap;
use crate::operator::Operator;
use crate::row::Row;

pub struct ProjectOp {
    child: Box<dyn Operator>,
    /// Maps input column name → index in child's row.values[]
    input_indices: Vec<usize>,
    /// The output column names (the "to" names from column_name_map)
    output_schema: Vec<String>,
}
```

**Design decision — why `input_indices: Vec<usize>` instead of storing `column_name_map`?**

At construction time, you convert column names into indices. Then in `next()`, you just index into the row's values — no HashMap lookup per row. This is the same caching strategy used in FilterOp's `col_index_map`, but simpler since Project only needs an ordered list of indices (one per output column).

#### Step 1.2 — Implement `ProjectOp::new()`

```rust
impl ProjectOp {
    pub fn new(child: Box<dyn Operator>, column_name_map: Vec<(String, String)>) -> Self {
        // Build a lookup from the child's schema: column_name → index
        let child_schema = child.schema();
        let name_to_idx: HashMap<String, usize> = child_schema
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // For each (from, to) in the map:
        //   - Find `from` in child's schema → get its index
        //   - Store the index in `input_indices`
        //   - Store `to` in `output_schema`
        let mut input_indices = Vec::new();
        let mut output_schema = Vec::new();

        for (from_name, to_name) in &column_name_map {
            let idx = name_to_idx[from_name];  // panics if column not found (query guarantees valid)
            input_indices.push(idx);
            output_schema.push(to_name.clone());
        }

        ProjectOp {
            child,
            input_indices,
            output_schema,
        }
    }
}
```

**Rust concept — `&column_name_map`:** We iterate by reference since we only need to read the names, not take ownership. The `.clone()` calls create owned `String`s to store in `output_schema`.

**Why not store `column_name_map` directly?** You *could*, but pre-computing `input_indices` means `next()` does zero string/HashMap work per row — just a simple index operation. For tables with millions of rows, this matters.

#### Step 1.3 — Implement the `Operator` trait for `ProjectOp`

```rust
impl Operator for ProjectOp {
    fn next(&mut self) -> Option<Row> {
        // Pull the next row from child
        self.child.next().map(|row| {
            // Pick only the columns at our pre-computed indices
            let new_values: Vec<_> = self.input_indices
                .iter()
                .map(|&idx| row.values[idx].clone())
                .collect();
            Row { values: new_values }
        })
    }

    fn schema(&self) -> Vec<String> {
        // Return the OUTPUT schema — the renamed column names
        self.output_schema.clone()
    }
}
```

**Key insight — `schema()` returns the OUTPUT names, not the input names.** This is the critical difference from FilterOp. If a Sort or Filter sits above this Project, it will call `schema()` and get the *renamed* names. For example, if the project renames `r_regionkey` to `id`, a subsequent Filter looking for `id` will correctly find it at index 0 of the projected output.

**Rust concept — `.map()` on `Option`:** `self.child.next()` returns `Option<Row>`. Calling `.map(|row| ...)` on it transforms `Some(row)` into `Some(projected_row)` and leaves `None` untouched. It's a cleaner alternative to:
```rust
match self.child.next() {
    Some(row) => Some(/* project */),
    None => None,
}
```

---

### Step 2: Register the Module and Integrate

#### Step 2.1 — Add `mod project;` to `main.rs`

Open `database/src/main.rs` and add this line alongside the other `mod` declarations (after `mod filter;`):

```rust
mod project;
```

#### Step 2.2 — Add the `Project` case to `build_operator()` in `query_executor.rs`

```rust
use crate::project::ProjectOp;  // add this import at the top

// Inside build_operator(), add this arm to the match:
QueryOp::Project(project_data) => {
    // 1. Recursively build the child operator first
    let child = build_operator(&project_data.underlying, ctx, buffer_pool);
    // 2. Wrap it with ProjectOp
    Box::new(ProjectOp::new(child, project_data.column_name_map.clone()))
}
```

**Why `project_data.column_name_map.clone()`?** Same reason as Filter: `build_operator` takes `&QueryOp`, so we need to give `ProjectOp::new()` owned data by cloning.

#### Step 2.3 — Ensure `ProjectData` fields are cloneable

Check `common/src/query.rs` — `ProjectData` contains:
- `column_name_map: Vec<(String, String)>` — `String` is `Clone`, so `Vec<(String, String)>` is `Clone` ✅
- `underlying: Box<QueryOp>` — we don't clone this, we only clone `column_name_map`

No changes needed to `query.rs` for Day 9 (the `Clone` derives added on Day 8 are sufficient).

---

### Step 3: Testing

#### Step 3.1 — Create a Project Test Query

Test with a Scan + Project query on the `region` table. Project to keep only `r_regionkey` (renamed to `key`) and `r_name`:

Add this entry to `query_configs` in `scratch/runtimes/tpch/monitor_config.json`:

```json
{
  "execution_name": "Project - Region key and name",
  "disabled": false,
  "query": {
    "root": {
      "Project": {
        "column_name_map": [
          ["r_regionkey", "key"],
          ["r_name", "name"]
        ],
        "underlying": {
          "Scan": {
            "table_id": "region"
          }
        }
      }
    }
  },
  "expected_output_file": "<PATH>/expected_project_region.csv",
  "memory_limit_mb": 64
}
```

Replace `<PATH>` with the full path to `scratch/runtimes/tpch/`.

#### Step 3.2 — Create a Scan + Filter + Project Test Query

This tests the full pipeline: scan → filter → project. Filter for `r_regionkey >= 2`, then project to `(key, name)`:

```json
{
  "execution_name": "Filter+Project - Region key >= 2, projected",
  "disabled": false,
  "query": {
    "root": {
      "Project": {
        "column_name_map": [
          ["r_regionkey", "key"],
          ["r_name", "name"]
        ],
        "underlying": {
          "Filter": {
            "predicates": [
              {
                "column_name": "r_regionkey",
                "operator": "GTE",
                "value": { "I32": 2 }
              }
            ],
            "underlying": {
              "Scan": {
                "table_id": "region"
              }
            }
          }
        }
      }
    }
  },
  "expected_output_file": "<PATH>/expected_filter_project_region.csv",
  "memory_limit_mb": 64
}
```

#### Step 3.3 — Generate Expected Output CSVs

Use SQLite to generate the expected output. **Important:** The output format must be `value|value|` (pipe-separated with trailing pipe, no header row).

```bash
cd scratch/runtimes/tpch/

# For the project-only query:
sqlite3 ../../compiled_datasets/tpch/sqlite.db \
  "SELECT r_regionkey || '|' || r_name || '|' FROM region;" \
  > expected_project_region.csv

# For the filter + project query:
sqlite3 ../../compiled_datasets/tpch/sqlite.db \
  "SELECT r_regionkey || '|' || r_name || '|' FROM region WHERE r_regionkey >= 2;" \
  > expected_filter_project_region.csv
```

> [!IMPORTANT]
> **Check the exact format of `expected_1.csv`** (the working Scan test) to see exactly what the monitor expects. The monitor compares line-by-line, so format must be exact. Your `Row::Display` already produces `value|value|value|` format, so the expected CSVs must match.

**Alternative approach** (simpler — use `.separator` mode):
```bash
sqlite3 -separator '|' ../../compiled_datasets/tpch/sqlite.db \
  "SELECT r_regionkey, r_name, '' FROM region;" \
  > expected_project_region.csv
```

The `''` trick adds a trailing empty column which produces the trailing `|`. Verify by `cat`-ing the file to check format.

> [!TIP]
> Open `expected_1.csv` first and study its format. Then generate your new CSVs to match that exact style.

#### Step 3.4 — Build & Run

```bash
cargo build -r --bin database
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

You should see:
```
Validation success! for Simple Scan - Region
Validation success! for Filter - Region key >= 2
Validation success! for Project - Region key and name
Validation success! for Filter+Project - Region key >= 2, projected
```

---

### Step 4: Edge Cases to Think About

#### 4.1 — Column Reordering
The `column_name_map` defines the output order. If the input has `[a, b, c]` and the map is `[(c, z), (a, x)]`, the output should be `[c_value, a_value]` with schema `[z, x]`. Your `input_indices` design handles this naturally — it preserves the map's order.

#### 4.2 — Identity Projections
When `from_name == to_name` (e.g., `("r_name", "r_name")`), the column is kept with the same name. No special handling needed — your code treats this the same as any other mapping.

#### 4.3 — Project Preserves Row Order
Per the assignment: "Project preserves the row order of its child." Your implementation does this naturally since it calls `child.next()` sequentially and doesn't reorder.

#### 4.4 — Empty Result
If the child produces zero rows (e.g., Filter filters everything), ProjectOp's `next()` will immediately return `None` (from `.map()` on `None`). This is correct.

#### 4.5 — Single Column Projection
If the map has only one entry, you produce rows with a single value. Make sure `Row::Display` handles this — `value|` with trailing pipe. Your current implementation does this correctly since it loops over `values` and writes `val|` for each.

---

### Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/project.rs` | **[NEW]** | `ProjectOp` struct, `new()`, `Operator` impl with schema tracking |
| `database/src/main.rs` | **[MODIFY]** | Add `mod project;` |
| `database/src/query_executor.rs` | **[MODIFY]** | Add `use crate::project::ProjectOp;`, add `QueryOp::Project` match arm |
| `scratch/runtimes/tpch/monitor_config.json` | **[MODIFY]** | Add Project and Filter+Project test queries |
| `scratch/runtimes/tpch/expected_project_region.csv` | **[NEW]** | Expected output from SQLite for project query |
| `scratch/runtimes/tpch/expected_filter_project_region.csv` | **[NEW]** | Expected output from SQLite for filter+project query |

---

### Quick Reference: Full `project.rs` Structure

```
project.rs
├── use statements (HashMap, Operator, Row)
│
├── pub struct ProjectOp { child, input_indices, output_schema }
│
├── impl ProjectOp
│   └── pub fn new(child, column_name_map) → Self
│       └── builds input_indices + output_schema from child.schema()
│
└── impl Operator for ProjectOp
    ├── fn next() → Option<Row>     // .map() with index-based column picking
    └── fn schema() → Vec<String>   // returns output_schema (renamed columns)
```

---

### Checkpoint: What "Done" Looks Like

- [ ] `database/src/project.rs` exists with `ProjectOp`, `new()`, and `Operator` impl
- [ ] `main.rs` has `mod project;`
- [ ] `query_executor.rs` handles `QueryOp::Project` and builds `ProjectOp`
- [ ] `cargo build -r --bin database` compiles with no errors
- [ ] Monitor config has Project and Filter+Project test queries with expected output CSVs
- [ ] `Validation success!` for all test queries ✅
