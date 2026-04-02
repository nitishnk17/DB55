# Day 10 Execution Plan — Cross (Cartesian Product) Operator

## Goal
Build a `CrossOp` operator that takes two child operators (left, right) and produces the **Cartesian product** — every left row combined with every right row. By the end of today, you'll run a Cross query and a Cross + Filter (join simulation) query and get `Validation success!` from the monitor.

---

## Background Concepts (Read This First!)

### What Is a Cross Product?
In SQL, `FROM A, B` (or `CROSS JOIN`) produces every possible combination of rows from A and B. If A has 5 rows and B has 25 rows, the result is 5 × 25 = 125 rows.

```
Left (3 rows):    Right (2 rows):    Cross Product (6 rows):
┌───┐             ┌───┐             ┌───┬───┐
│ L1│             │ R1│             │ L1│ R1│
│ L2│      ×      │ R2│      =      │ L1│ R2│
│ L3│             └───┘             │ L2│ R1│
└───┘                               │ L2│ R2│
                                    │ L3│ R1│
                                    │ L3│ R2│
                                    └───┴───┘
```

Each output row is a **concatenation** of a left row's values and a right row's values.

### Where Does Cross Fit in the Query Tree?
Cross is the mechanism for **joins**. A SQL join like `SELECT * FROM A JOIN B ON A.id = B.id` is represented in the AST as:
```
Filter(id = id)           ← join condition (Column-to-Column comparison)
  └── Cross               ← YOU BUILD THIS TODAY
       ├── Scan(A)        ← left child
       └── Scan(B)        ← right child
```

The Filter on top removes non-matching pairs, turning the Cartesian product into a join. Today we only build the Cross operator — the Filter already does its job from Day 8.

### The Schema Problem
When you cross two tables, the output has **both sets of columns**. If left has `[a, b]` and right has `[x, y]`, the cross output has `[a, b, x, y]`. The assignment guarantees:
> "The two children of a Cross node will never share a column name."

So you can simply concatenate the schemas without worrying about naming conflicts.

### The Iteration Problem — Why We Must Materialize
Our `Operator` trait only has `next() -> Option<Row>` — there's no `reset()` method. For a cross product, you need to iterate through the right child **once for every left row**. Since you can't "rewind" an operator, you must **materialize** (collect into memory) one of the children.

**Strategy (materialize right):**
1. At construction time, drain the entire right child into `Vec<Row>` (store in memory).
2. In `next()`, iterate: for the current left row, pair it with each right row (using an index).
3. When you've exhausted all right rows for the current left, get the next left row and reset the right index.

**Why right and not left?** Convention — either works. Materializing the smaller side is better for memory, but since we don't know sizes at construction time (without statistics), materializing right is a simple default. The real optimization (choosing which side to materialize based on table stats) comes later.

> [!WARNING]
> **Memory concern:** Materializing all right rows works fine for small tables like `region` (5 rows) and `nation` (25 rows). For large tables (e.g., `lineitem` with ~6M rows), this will blow the 64MB memory limit. This is the **same problem** as TableScanner pre-loading all rows. The proper fix (external hash join, sort-merge join) comes on Days 15-17. For now, the naive approach gets correctness ✅.

### Key Assignment Rules for Cross
- Cross **does not guarantee any particular output ordering** — you can emit rows in any order.
- The two children of a Cross node **will never share a column name**.
- A Cross always has exactly **two children** (left and right).

---

## Step 1: Create `database/src/cross.rs`

Create a new file `database/src/cross.rs`.

### Step 1.1 — Define the `CrossOp` struct

```rust
use crate::operator::Operator;
use crate::row::Row;

pub struct CrossOp {
    left: Box<dyn Operator>,           // we pull from left one-at-a-time
    right_rows: Vec<Row>,              // right child fully materialized
    current_left_row: Option<Row>,     // the current left row we're pairing with
    right_index: usize,                // which right row we're currently at
    output_schema: Vec<String>,        // concatenation of left + right schemas
}
```

**Why five fields?**
- `left`: We pull from this lazily, one row at a time.
- `right_rows`: The entire right child, stored in memory. We iterate through this repeatedly.
- `current_left_row`: The "current" left row. We hold onto it while we pair it with every right row.
- `right_index`: Tracks our position in `right_rows`. When it reaches the end, we advance `left`.
- `output_schema`: Pre-computed: left schema + right schema concatenated.

### Step 1.2 — Implement `CrossOp::new()`

```rust
impl CrossOp {
    pub fn new(mut left: Box<dyn Operator>, mut right: Box<dyn Operator>) -> Self {
        // 1. Compute the output schema BEFORE draining right
        //    (schema() is available before next() is called)
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut output_schema = left_schema;
        output_schema.extend(right_schema);

        // 2. Materialize the right child: drain all rows into a Vec
        let mut right_rows = Vec::new();
        while let Some(row) = right.next() {
            right_rows.push(row);
        }

        // 3. Get the first left row
        let current_left_row = left.next();

        CrossOp {
            left,
            right_rows,
            current_left_row,
            right_index: 0,
            output_schema,
        }
    }
}
```

**Why get `schema()` before `next()`?** The `schema()` method returns column names and doesn't consume the operator. We capture both schemas before we start draining `right`, since the operator is consumed (moved into `right_rows`) during construction.

**Why call `left.next()` in `new()`?** We pre-fetch the first left row so that `next()` can immediately start producing output. If the left child is empty, `current_left_row` will be `None` and `next()` will immediately return `None`.

**Rust concept — `mut left` / `mut right` in parameters:** We need `mut` because `next()` requires `&mut self`. The `Box<dyn Operator>` is moved into the function, and we need the mutable binding to call `next()` on it.

### Step 1.3 — Implement the `Operator` trait for `CrossOp`

This is the trickiest `next()` so far — it needs nested iteration:

```rust
impl Operator for CrossOp {
    fn next(&mut self) -> Option<Row> {
        loop {
            // 1. If no current left row, we're done
            let left_row = match &self.current_left_row {
                Some(row) => row,
                None => return None,
            };

            // 2. If we still have right rows to pair with this left row
            if self.right_index < self.right_rows.len() {
                // Combine: left_row.values + right_rows[right_index].values
                let right_row = &self.right_rows[self.right_index];
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                self.right_index += 1;
                return Some(Row { values: combined_values });
            }

            // 3. Exhausted right rows for this left row → advance to next left
            self.current_left_row = self.left.next();
            self.right_index = 0;
            // Loop back to step 1 (if new left_row exists, start pairing again)
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }
}
```

**How this works step-by-step:**

Imagine left = `[L1, L2]`, right = `[R1, R2, R3]`:

| Call | current_left_row | right_index | Output |
|------|-----------------|-------------|--------|
| 1 | L1 | 0 → 1 | L1+R1 |
| 2 | L1 | 1 → 2 | L1+R2 |
| 3 | L1 | 2 → 3 | L1+R3 |
| 4 | L1 → L2 | 3 → 0 (reset) | *(loops back)* |
| 5 | L2 | 0 → 1 | L2+R1 |
| 6 | L2 | 1 → 2 | L2+R2 |
| 7 | L2 | 2 → 3 | L2+R3 |
| 8 | L2 → None | 3 → 0 (reset) | None |

**Rust concept — `match &self.current_left_row`:** We borrow the `Option<Row>` rather than taking ownership. The `&` is critical — without it, `match self.current_left_row` would try to **move** the value out of the struct, which Rust won't allow since we might use it again on the next call.

**Why `.clone()` for values?** We reuse `current_left_row` multiple times (once for each right row). And we reuse each `right_row` once for each left row. Cloning creates fresh copies for the output row. This is the cost of the naive approach — optimization later can reduce cloning.

**Key insight — the `loop`:** When we advance to the next left row (step 3), we don't produce output — we loop back to check if the new left row exists. If left is exhausted, we return `None`. If not, we start pairing with right from index 0.

---

## Step 2: Register the Module and Integrate

### Step 2.1 — Add `mod cross;` to `main.rs`

Open `database/src/main.rs` and add this line alongside the other `mod` declarations:

```rust
mod cross;
```

### Step 2.2 — Add the `Cross` case to `build_operator()` in `query_executor.rs`

```rust
use crate::cross::CrossOp;  // add this import at the top

// Inside build_operator(), add this arm to the match:
QueryOp::Cross(cross_data) => {
    // Build BOTH children recursively
    let left = build_operator(&cross_data.left, ctx, buffer_pool);
    let right = build_operator(&cross_data.right, ctx, buffer_pool);
    Box::new(CrossOp::new(left, right))
}
```

**Why build both children?** Cross is a **binary** operator — it has two inputs, not one like Filter and Project. We build both sub-trees independently and pass them to CrossOp.

**Note:** After adding this arm, the only remaining unimplemented operator in the wildcard `_` arm is `Sort` (Day 11-12).

---

## Step 3: Testing

### Step 3.1 — Test 1: Cross + Filter (Join Simulation) — `region ⋈ nation`

This is the most important test. It simulates a join between `region` (5 rows) and `nation` (25 rows) on the foreign key `r_regionkey = n_regionkey`. The query is equivalent to:

```sql
SELECT * FROM region, nation WHERE r_regionkey = n_regionkey;
```

**AST structure:**
```
Filter(r_regionkey = n_regionkey)    ← Column-to-Column comparison
  └── Cross
       ├── Scan(region)              ← 5 rows, schema: [r_regionkey, r_name, r_comment]
       └── Scan(nation)              ← 25 rows, schema: [n_nationkey, n_name, n_regionkey, n_comment]
```

**Result:** 25 rows (each nation matched with its region). The cross product is 5×25 = 125 rows, but the filter keeps only the 25 where region keys match.

The combined schema will be: `[r_regionkey, r_name, r_comment, n_nationkey, n_name, n_regionkey, n_comment]` — 7 columns per row.

**JSON query for monitor_config.json:**
```json
{
  "execution_name": "Cross+Filter - Region join Nation",
  "disabled": false,
  "query": {
    "root": {
      "Filter": {
        "predicates": [
          {
            "column_name": "r_regionkey",
            "operator": "EQ",
            "value": {
              "Column": "n_regionkey"
            }
          }
        ],
        "underlying": {
          "Cross": {
            "left": { "Scan": { "table_id": "region" } },
            "right": { "Scan": { "table_id": "nation" } }
          }
        }
      }
    }
  },
  "expected_output_file": "<RUNTIME_PATH>/expected_cross_filter_region_nation.csv",
  "memory_limit_mb": 64
}
```

**Generate expected output with SQLite:**
```bash
sqlite3 <COMPILED_PATH>/sqlite.db \
  "SELECT r_regionkey || '|' || r_name || '|' || r_comment || '|' || n_nationkey || '|' || n_name || '|' || n_regionkey || '|' || n_comment || '|' FROM region, nation WHERE r_regionkey = n_regionkey;" \
  > <RUNTIME_PATH>/expected_cross_filter_region_nation.csv
```

> [!IMPORTANT]
> Replace `<COMPILED_PATH>` with `scratch/compiled_datasets/tpch` and `<RUNTIME_PATH>` with `scratch/runtimes/tpch` (full absolute paths as used in existing entries).

### Step 3.2 — Test 2: Cross + Filter + Project (Join with Projection)

This tests the full pipeline: cross → filter → project. Equivalent SQL:
```sql
SELECT r_name, n_name FROM region, nation WHERE r_regionkey = n_regionkey;
```

**AST structure:**
```
Project(r_name → region_name, n_name → nation_name)
  └── Filter(r_regionkey = n_regionkey)
       └── Cross
            ├── Scan(region)
            └── Scan(nation)
```

**JSON query:**
```json
{
  "execution_name": "Cross+Filter+Project - Region-Nation names",
  "disabled": false,
  "query": {
    "root": {
      "Project": {
        "column_name_map": [
          ["r_name", "region_name"],
          ["n_name", "nation_name"]
        ],
        "underlying": {
          "Filter": {
            "predicates": [
              {
                "column_name": "r_regionkey",
                "operator": "EQ",
                "value": {
                  "Column": "n_regionkey"
                }
              }
            ],
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
  "expected_output_file": "<RUNTIME_PATH>/expected_cross_filter_project_region_nation.csv",
  "memory_limit_mb": 64
}
```

**Generate expected output:**
```bash
sqlite3 <COMPILED_PATH>/sqlite.db \
  "SELECT r_name || '|' || n_name || '|' FROM region, nation WHERE r_regionkey = n_regionkey;" \
  > <RUNTIME_PATH>/expected_cross_filter_project_region_nation.csv
```

### Step 3.3 — Build & Run

```bash
cargo build -r --bin database
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

You should see validation success for all queries including the new Cross tests.

> [!TIP]
> **Debugging tip:** If validation fails, the monitor compares output **line-by-line** but Cross/Filter/Scan don't guarantee ordering. The monitor should handle unordered comparisons for these operators. If it does a strict line-by-line comparison and your row order differs from SQLite's, you might need to check how the monitor does validation. If it does set-based comparison, order won't matter.

---

## Step 4: Edge Cases to Think About

### 4.1 — Empty Left or Right Child
If either child has zero rows, the cross product should produce **zero rows**:
- Empty left → `current_left_row` is `None` from the start → `next()` returns `None` immediately.
- Empty right → `right_rows` is empty → `right_index < right_rows.len()` is always false → advance left → loop → eventually `None`.

Both cases are handled naturally by the loop structure.

### 4.2 — Single Row on One Side
If left has 1 row and right has N rows, the result is N rows (each being the single left row combined with one right row). This is just a degenerate case of the normal iteration — after all N right rows are emitted, we advance left and get `None`.

### 4.3 — Large Tables
Cross-product of two large tables (e.g., `orders × lineitem`) would be astronomical. The naive approach will:
1. Try to materialize all of right child → blow memory
2. Even if it fit, produce billions of rows

This is why join optimization (Days 15-17) replaces Cross+Filter with Hash Join or Sort-Merge Join. For now, only test with small tables.

### 4.4 — Multiple Cross Nodes (Multi-way Joins)
A 3-table join `A ⋈ B ⋈ C` produces a tree like:
```
Filter(...)
  └── Cross
       ├── Filter(...)
       │    └── Cross
       │         ├── Scan(A)
       │         └── Scan(B)
       └── Scan(C)
```

The inner Cross produces A×B rows, which become the "left" child of the outer Cross. Your recursive `build_operator` handles this naturally — each Cross builds its children first.

### 4.5 — Column-to-Column Comparison in Filter
The join test uses `r_regionkey = n_regionkey` which is a Column-to-Column comparison (`ComparisionValue::Column("n_regionkey")`). Your `resolve_value()` in `filter.rs` already handles this — it looks up the column by name from the combined row. Since the combined row has both `r_regionkey` and `n_regionkey` in its schema, the lookup works.

> [!NOTE]
> **Type mismatch warning:** `r_regionkey` is `Int32` and `n_regionkey` is also `Int32` in this case, so comparison works fine. But be aware that in general, cross-table column comparisons must have matching types. The assignment guarantees queries are well-typed, so you don't need to handle mismatches.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/cross.rs` | **[NEW]** | `CrossOp` struct, `new()` (materialize right), `Operator` impl with nested iteration |
| `database/src/main.rs` | **[MODIFY]** | Add `mod cross;` |
| `database/src/query_executor.rs` | **[MODIFY]** | Add `use crate::cross::CrossOp;`, add `QueryOp::Cross` match arm |
| `scratch/runtimes/tpch/monitor_config.json` | **[MODIFY]** | Add Cross+Filter and Cross+Filter+Project test queries |
| `scratch/runtimes/tpch/expected_cross_filter_region_nation.csv` | **[NEW]** | Expected output from SQLite for join query |
| `scratch/runtimes/tpch/expected_cross_filter_project_region_nation.csv` | **[NEW]** | Expected output from SQLite for join+project query |

---

## Quick Reference: Full `cross.rs` Structure

```
cross.rs
├── use statements (Operator, Row)
│
├── pub struct CrossOp {
│       left, right_rows, current_left_row, right_index, output_schema
│   }
│
├── impl CrossOp
│   └── pub fn new(left, right) → Self
│       ├── compute output_schema = left.schema() + right.schema()
│       ├── materialize right child into right_rows: Vec<Row>
│       └── pre-fetch first left row
│
└── impl Operator for CrossOp
    ├── fn next() → Option<Row>
    │   └── loop:
    │       ├── if no current left → return None
    │       ├── if right_index < right_rows.len() → combine & return
    │       └── else → advance left, reset right_index, continue loop
    └── fn schema() → Vec<String>   // returns concatenated schema
```

---

## Checkpoint: What "Done" Looks Like

- [ ] `database/src/cross.rs` exists with `CrossOp`, `new()`, and `Operator` impl
- [ ] `main.rs` has `mod cross;`
- [ ] `query_executor.rs` handles `QueryOp::Cross` and builds `CrossOp`
- [ ] `cargo build -r --bin database` compiles with no errors
- [ ] Monitor config has Cross+Filter and Cross+Filter+Project test queries with expected output CSVs
- [ ] `Validation success!` for Cross+Filter (join) query ✅
- [ ] `Validation success!` for Cross+Filter+Project query ✅
- [ ] Only `Sort` remains as unimplemented in the wildcard `_` arm
