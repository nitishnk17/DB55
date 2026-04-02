# Day 8 Execution Plan — Filter Operator with Predicate Evaluation

## Goal
Build a `FilterOp` operator that wraps a child operator (e.g., `TableScanner`) and only passes through rows that satisfy **all** predicates. A predicate is something like `r_regionkey >= 2` or `n_nationkey = r_regionkey`. By the end of today, you'll run a Scan + Filter query and get `Validation success!` from the monitor.

---

## Background Concepts (Read This First!)

### What Is a Filter Operator?
In a query tree, Filter sits between two operators. It pulls rows from its child, checks each row against a list of conditions (predicates), and only emits rows where **every** predicate is true (AND logic).

```
         ProjectOp          ← (future: Day 9)
            │
         FilterOp            ← YOU BUILD THIS TODAY
            │
        TableScanner          ← already built (Day 6–7)
```

When `main.rs` calls `filter_op.next()`:
1. FilterOp calls `self.child.next()` to get a row from the child.
2. It checks all predicates against that row.
3. If the row passes **all** predicates → return it.
4. If it fails any predicate → discard it and pull the next row from the child.
5. Repeat until a matching row is found or the child is exhausted (returns `None`).

### What Is a Predicate?
A predicate is a single condition like `column_name OP value`. Looking at the AST definition in `common/src/query.rs`:

```rust
pub struct Predicate {
    pub column_name: String,          // left side: always a column name
    pub operator: ComparisionOperator, // EQ, NE, GT, GTE, LT, LTE
    pub value: ComparisionValue,      // right side: a literal OR another column
}
```

The **left side** is always a column name (a `String`).
The **right side** (`ComparisionValue`) can be one of:
- `Column(String)` — compare against another column in the same row (e.g., joins)
- `I32(i32)` — integer literal
- `I64(i64)` — long literal
- `F32(f32)` — float literal
- `F64(f64)` — double literal
- `String(String)` — string literal

### How Do You Find a Column's Value in a Row?
A `Row` stores values as `Vec<Data>` — values[0] is the first column, values[1] is the second, etc. The column names come from the child operator's `schema()` method, which returns `Vec<String>`. So to find the value of `r_regionkey`:

1. Call `child.schema()` → `["r_regionkey", "r_name", "r_comment"]`
2. Find the position of `"r_regionkey"` → index `0`
3. Access `row.values[0]` → `Data::Int32(2)` (for example)

This lookup from column name → index is something you'll do frequently, so it's worth caching at construction time.

### The Data Type System
The `Data` enum (from `common/src/lib.rs`) represents a typed value:
```rust
pub enum Data {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}
```

**Key fact:** `Data` already implements `PartialOrd` and `PartialEq`. This means you can directly write `data_a > data_b` or `data_a == data_b` as long as both are the same variant (e.g., both `Int32`). If they're different types (e.g., `Int32` vs `Float32`), `partial_cmp` returns `None`. You'll use this heavily.

### How Comparisons Map to Rust
| Predicate Operator | Rust Expression |
|---|---|
| `EQ` | `left == right` (uses `PartialEq`) |
| `NE` | `left != right` |
| `GT` | `left.partial_cmp(&right) == Some(Ordering::Greater)` |
| `GTE` | `matches!(left.partial_cmp(&right), Some(Ordering::Greater \| Ordering::Equal))` |
| `LT` | `left.partial_cmp(&right) == Some(Ordering::Less)` |
| `LTE` | `matches!(left.partial_cmp(&right), Some(Ordering::Less \| Ordering::Equal))` |

**Why `partial_cmp` instead of `>` / `<`?**
Rust's `>` operator requires the `PartialOrd` trait, and `Data` implements it. You *can* use `>` / `<` directly, but `partial_cmp` with pattern matching is more explicit and handles the `None` case (mismatched types). Either approach works — choose what feels clearer to you.

---

## 1. P1 Tasks: Predicate Evaluation Logic

### Step 1.1 — Create `database/src/filter.rs`
Create a new file `database/src/filter.rs`. Also add `mod filter;` to `main.rs` (alongside the other `mod` declarations).

### Step 1.2 — Write a Helper: Convert `ComparisionValue` to `Data`
When the predicate's right side is a literal (like `I32(42)`), you need to convert it to a `Data` value so you can compare it against the row's `Data`. When it's `Column("some_name")`, you need to look up that column's value from the row instead.

Write a helper function that, given a `ComparisionValue`, a `Row`, and a column-name-to-index mapping, produces the `Data` to use on the right side:

```rust
use std::collections::HashMap;
use common::{Data};
use common::query::{ComparisionValue};
use crate::row::Row;

fn resolve_value(
    value: &ComparisionValue,
    row: &Row,
    col_index_map: &HashMap<String, usize>,
) -> Data {
    match value {
        ComparisionValue::Column(col_name) => {
            let idx = col_index_map[col_name];  // look up the other column's index
            row.values[idx].clone()
        }
        ComparisionValue::I32(v) => Data::Int32(*v),
        ComparisionValue::I64(v) => Data::Int64(*v),
        ComparisionValue::F32(v) => Data::Float32(*v),
        ComparisionValue::F64(v) => Data::Float64(*v),
        ComparisionValue::String(v) => Data::String(v.clone()),
    }
}
```

**Why `clone()`?** For `Column(col_name)`, we need to extract a `Data` value out of the row. Since `Row` owns its `values`, we can't just borrow it easily across the comparison logic — cloning avoids borrow-checker headaches. The performance cost is negligible here since predicates are checked once per row.

### Step 1.3 — Write the Core: `evaluate_predicate()`
This function takes one `Predicate`, the current row, and the column index map, and returns `true` if the row satisfies the predicate:

```rust
use common::query::{Predicate, ComparisionOperator};
use std::cmp::Ordering;

fn evaluate_predicate(
    predicate: &Predicate,
    row: &Row,
    col_index_map: &HashMap<String, usize>,
) -> bool {
    // 1. Get the left side: the column value from the row
    let left_idx = col_index_map[&predicate.column_name];
    let left = &row.values[left_idx];

    // 2. Get the right side: resolve from literal or column reference
    let right = resolve_value(&predicate.value, row, col_index_map);

    // 3. Compare using the operator
    match predicate.operator {
        ComparisionOperator::EQ => left == &right,
        ComparisionOperator::NE => left != &right,
        ComparisionOperator::GT => {
            left.partial_cmp(&right) == Some(Ordering::Greater)
        }
        ComparisionOperator::GTE => {
            matches!(left.partial_cmp(&right), Some(Ordering::Greater | Ordering::Equal))
        }
        ComparisionOperator::LT => {
            left.partial_cmp(&right) == Some(Ordering::Less)
        }
        ComparisionOperator::LTE => {
            matches!(left.partial_cmp(&right), Some(Ordering::Less | Ordering::Equal))
        }
    }
}
```

**Rust concept — `matches!` macro:** `matches!(expr, pattern)` is a convenient way to check if an expression matches a pattern and returns a `bool`. It's shorthand for a `match` block that returns `true`/`false`.

**Rust concept — `&right` vs `right`:** `left` is a `&Data` (reference), and `right` is a `Data` (owned). When comparing, we use `&right` to make both sides references, which is what `PartialEq` and `PartialOrd` expect.

### Step 1.4 — Write `evaluate_all_predicates()`
Filters use AND logic — ALL predicates must pass. This is a one-liner with Rust's iterator methods:

```rust
fn evaluate_all_predicates(
    predicates: &[Predicate],
    row: &Row,
    col_index_map: &HashMap<String, usize>,
) -> bool {
    predicates.iter().all(|p| evaluate_predicate(p, row, col_index_map))
}
```

**Rust concept — `.all()`:** This is an iterator method that returns `true` only if the closure returns `true` for every element. It short-circuits: if any predicate fails, it stops checking the rest.

---

## 2. P2 Tasks: `FilterOp` Struct and `Operator` Implementation

### Step 2.1 — Define the `FilterOp` struct
```rust
use common::query::Predicate;
use crate::operator::Operator;

pub struct FilterOp {
    child: Box<dyn Operator>,              // the underlying operator (e.g., TableScanner)
    predicates: Vec<Predicate>,            // list of conditions to check
    col_index_map: HashMap<String, usize>, // column name → index (cached!)
}
```

**Why `Box<dyn Operator>`?** The child could be a `TableScanner`, another `FilterOp`, or any future operator. `Box<dyn Operator>` is Rust's way of saying "a heap-allocated object that implements the `Operator` trait" — this is dynamic dispatch (like polymorphism in Java/C++). The `dyn` keyword means "I don't know the concrete type at compile time."

**Why cache `col_index_map`?** Every row we check needs column lookups. If we rebuilt the map for every row, we'd be doing redundant work. Building it once in `new()` and reusing it is much more efficient.

### Step 2.2 — Implement `FilterOp::new()`
Build the column index map from the child's schema:

```rust
impl FilterOp {
    pub fn new(child: Box<dyn Operator>, predicates: Vec<Predicate>) -> Self {
        // Build column name → index mapping from the child's schema
        let col_index_map: HashMap<String, usize> = child
            .schema()
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        FilterOp {
            child,
            predicates,
            col_index_map,
        }
    }
}
```

**Rust concept — `.enumerate()`:** Wraps each item with its index, producing `(0, "r_regionkey"), (1, "r_name"), (2, "r_comment")`.

**Rust concept — `.collect()`:** Transforms an iterator into a collection. Rust infers from the type annotation `HashMap<String, usize>` that it should collect into a HashMap. The `(String, usize)` tuples become key-value pairs automatically.

### Step 2.3 — Implement the `Operator` trait for `FilterOp`
This is the heart of the filter. The `next()` method loops through the child's output, returning only rows that pass all predicates:

```rust
impl Operator for FilterOp {
    fn next(&mut self) -> Option<Row> {
        // Keep pulling rows from child until one passes all predicates
        while let Some(row) = self.child.next() {
            if evaluate_all_predicates(&self.predicates, &row, &self.col_index_map) {
                return Some(row);
            }
            // Row didn't pass → discard it and try the next one
        }
        // Child exhausted, no more rows
        None
    }

    fn schema(&self) -> Vec<String> {
        // Filter doesn't change the schema — same columns in, same columns out
        self.child.schema()
    }
}
```

**Rust concept — `while let Some(row) = ...`:** This is a loop that keeps going as long as `self.child.next()` returns `Some(row)`. When it returns `None`, the loop exits and we return `None`. It's Rust's idiomatic way of draining an iterator.

**Key insight:** Filter does **not** change the schema. It has the same columns as its child — it just has fewer rows. This is important because downstream operators (like Project) will rely on `schema()` to know what columns are available.

---

## 3. Both Tasks: Integration into the Query Executor

### Step 3.1 — Add the `Filter` case to `build_operator()`
Open `database/src/query_executor.rs` and add a match arm for `QueryOp::Filter`:

```rust
use crate::filter::FilterOp;  // add this import at the top

// Inside build_operator(), add this arm to the match:
QueryOp::Filter(filter_data) => {
    // 1. Recursively build the child operator first
    let child = build_operator(&filter_data.underlying, ctx, buffer_pool);
    // 2. Wrap it with FilterOp
    Box::new(FilterOp::new(child, filter_data.predicates.clone()))
}
```

**Why `filter_data.predicates.clone()`?** The `build_operator` function borrows `query_op` immutably (`&QueryOp`). The `FilterOp::new()` wants to take ownership of the predicates. We clone to give `FilterOp` its own copy. This is a one-time cost at query construction, not per-row.

**Why recursive?** A query tree is nested. `Filter { underlying: Scan("region") }` means: "build the Scan first, then wrap it in a Filter." The recursive call `build_operator(&filter_data.underlying, ...)` builds whatever the child is (could be another Filter, a Scan, etc.), and then we wrap the result.

### Step 3.2 — Make sure `Predicate` is cloneable
The `Predicate`, `ComparisionOperator`, and `ComparisionValue` structs in `common/src/query.rs` need to derive `Clone` so you can clone them in `build_operator`. Check if they already have `#[derive(Clone)]`. If not, add it:

```rust
// In common/src/query.rs — add Clone to these derives:
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ComparisionOperator { ... }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ComparisionValue { ... }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Predicate { ... }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterData { ... }
```

**Rust concept — `#[derive(Clone)]`:** This auto-generates a `.clone()` method for the struct. It works as long as all fields are themselves `Clone`. Since all the fields here are `String`, `i32`, `f32`, etc. (all `Clone`), it just works.

---

## 4. Testing: Add a Filter Query to Monitor Config

### Step 4.1 — Create a Filter Test Query
You need to add a new query to `monitor_config.json` that uses Filter. The `region` table has columns `r_regionkey (Int32)`, `r_name (String)`, `r_comment (String)`. A simple test: scan `region` and filter for `r_regionkey >= 2`.

Add this entry to the `query_configs` array in `scratch/runtimes/tpch/monitor_config.json`:

```json
{
  "execution_name": "Filter - Region key >= 2",
  "disabled": false,
  "query": {
    "root": {
      "Filter": {
        "predicates": [
          {
            "column_name": "r_regionkey",
            "operator": "GTE",
            "value": {
              "I32": 2
            }
          }
        ],
        "underlying": {
          "Scan": {
            "table_id": "region"
          }
        }
      }
    }
  },
  "expected_output_file": "<path-to>/expected_filter_region.csv",
  "memory_limit_mb": 64
}
```

### Step 4.2 — Generate the Expected Output CSV
The monitor compares your database output against an expected CSV file. You generate this using SQLite against the TPC-H data. From the `scratch/runtimes/tpch/` directory:

```bash
# Open the SQLite database (check which .db file exists in your scratch directory)
sqlite3 tpch.db

# Run the equivalent SQL query
.mode csv
.separator "|"
.output expected_filter_region.csv
SELECT * FROM region WHERE r_regionkey >= 2;
.output stdout
.quit
```

**Important:** The expected CSV must match your database's output format exactly. Your `Row::fmt()` uses `value|value|value|` (trailing pipe, no spaces). Make sure the SQLite output matches. You may need to tweak the `.separator` or add a trailing `|` to each line.

**Tip:** Look at `expected_1.csv` (the one that works for the Scan query) to see the exact format the monitor expects.

### Step 4.3 — Build & Run
```bash
cargo build -r --bin database
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

You should see:
```
Validation success! for Simple Scan - Region
Validation success! for Filter - Region key >= 2
```

---

## 5. Edge Cases to Think About

### 5.1 — Multiple Predicates (AND logic)
The `FilterData` has `predicates: Vec<Predicate>` — that's a list, not a single predicate. ALL must pass. Your `evaluate_all_predicates()` handles this with `.all()`. Test with a query like: `r_regionkey >= 1 AND r_regionkey <= 3`.

### 5.2 — Column-to-Column Comparison
When `value` is `ComparisionValue::Column("other_col")`, you're comparing two columns from the same row. This is how join conditions work: `Cross + Filter(a.id = b.id)`. Your `resolve_value()` function handles this — it looks up the other column's value from the same row.

### 5.3 — Empty Result
If no rows pass the filter, your `FilterOp.next()` will keep calling `child.next()` until it returns `None`, then return `None` itself. The monitor should receive just `validate\n` then `!\n` with no rows in between. This should work correctly with the current implementation.

### 5.4 — Type Mismatches
What if the predicate says `r_name > 42` (comparing a String column to an Int32 literal)? The `PartialOrd` impl for `Data` returns `None` for mismatched types. Your `evaluate_predicate` will return `false` for GT/GTE/LT/LTE (since `Some(Ordering::...)` won't match `None`), and `false` for EQ (since `PartialEq` returns `false` for mismatched types). This is reasonable behavior — mismatched comparisons fail silently.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/filter.rs` | **[NEW]** | `FilterOp` struct, `new()`, `Operator` impl, `evaluate_predicate()`, `resolve_value()`, `evaluate_all_predicates()` |
| `database/src/main.rs` | **[MODIFY]** | Add `mod filter;` |
| `database/src/query_executor.rs` | **[MODIFY]** | Add `use crate::filter::FilterOp;`, add `QueryOp::Filter` match arm |
| `common/src/query.rs` | **[MODIFY]** | Add `Clone` derive to `ComparisionOperator`, `ComparisionValue`, `Predicate`, `FilterData` |
| `scratch/runtimes/tpch/monitor_config.json` | **[MODIFY]** | Add a Filter test query entry |
| `scratch/runtimes/tpch/expected_filter_region.csv` | **[NEW]** | Expected output from SQLite for the filter query |

---

## Quick Reference: Full `filter.rs` Structure

Here's the skeleton of the complete file so you can see how all the pieces fit together:

```
filter.rs
├── use statements (HashMap, Data, Predicate, ComparisionOperator, etc.)
│
├── pub struct FilterOp { child, predicates, col_index_map }
│
├── impl FilterOp
│   └── pub fn new(child, predicates) → Self
│       └── builds col_index_map from child.schema()
│
├── impl Operator for FilterOp
│   ├── fn next() → Option<Row>      // while-let loop, check all predicates
│   └── fn schema() → Vec<String>    // delegates to child.schema()
│
├── fn resolve_value(value, row, col_index_map) → Data
│   └── match on Column vs literal variants
│
├── fn evaluate_predicate(pred, row, col_index_map) → bool
│   └── get left, resolve right, match on operator
│
└── fn evaluate_all_predicates(predicates, row, col_index_map) → bool
    └── predicates.iter().all(...)
```

---

## Checkpoint: What "Done" Looks Like
- [ ] `database/src/filter.rs` exists with `FilterOp`, predicate evaluation, and `Operator` impl
- [ ] `main.rs` has `mod filter;`
- [ ] `query_executor.rs` handles `QueryOp::Filter` and builds `FilterOp`
- [ ] `common/src/query.rs` types derive `Clone`
- [ ] `cargo build -r --bin database` compiles with no errors
- [ ] Monitor config has a Filter test query with expected output CSV
- [ ] `Validation success! for Filter - Region key >= 2` ✅
