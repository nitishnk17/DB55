# Day 5 Execution Plan — Table Scanner & Operator Trait

## Goal
Build a `TableScanner` that iterates through *all* rows of *any* table (not just one block), and wrap it behind a common `Operator` trait. This is a critical foundation — every query starts with a scan, and every future operator (Filter, Project, Sort, Cross) will be an `Operator` too, forming a composable pipeline.

---

## Background Concepts (Read This First!)

### What is a Table Scanner?
On Day 4, you decoded rows from a **single block**. But a table spans **many blocks** (the customer table has 618 blocks!). A Table Scanner walks through every block of a table, from the first to the last, decoding and returning rows **one at a time**.

### The Iterator / Volcano Model
Database query engines use a model called the **Volcano Model** (or iterator model):
- Every operator has a `next()` method that returns `Option<Row>`.
- `Some(row)` means "here's the next row."
- `None` means "I'm done, no more rows."
- Operators are **chained**: a Filter's `next()` calls its child's `next()`, checks the predicate, and either returns the row or asks for another.

```
Project.next()  →  Filter.next()  →  Scan.next()  →  reads from disk
       ↑                 ↑                 ↑
   returns row      checks predicate   decodes from block
```

This is **lazy** — rows are produced on-demand, not all loaded into memory at once.

### The Ownership Problem (Important Rust Concept!)
The report's pseudocode has `TableScanner` calling `buffer_pool.fetch_block(self.current_block)` directly. But we don't have a Buffer Pool yet (that's Day 7). For now, the scanner needs to hold a **mutable reference** to the `DiskManager` to read blocks.

In Rust, you can't have two things owning a mutable reference at the same time. The simplest approach for now: the `TableScanner` stores a **mutable reference** to the `DiskManager`. Later (Day 7), you'll swap this out for a Buffer Pool reference.

**Alternative (simpler for beginners):** Pre-read all blocks of a table into memory on scanner creation, then iterate through the in-memory data. This avoids the mutable borrow issue entirely and is perfectly fine given the 64MB memory limit.

---

## 1. P2 Tasks: Define the Operator Trait

### Step 1.1 — Create a new file `database/src/operator.rs`
Add `mod operator;` to `main.rs`.

### Step 1.2 — Define the Operator trait
```rust
use crate::row::Row;

pub trait Operator {
    /// Returns the next row from this operator, or None if exhausted.
    fn next(&mut self) -> Option<Row>;
    
    /// Returns the output schema (column names) of this operator.
    /// This is needed so downstream operators know what columns are available.
    fn schema(&self) -> Vec<String>;
}
```

**Why `schema()`?** When a Filter checks `WHERE c_nationkey = 15`, it needs to know which index in the `Row.values` vector corresponds to `c_nationkey`. The schema method returns the ordered list of column names, so you can do `schema.iter().position(|name| name == "c_nationkey")` to find the index.

---

## 2. P1 Tasks: Implement TableScanner

### Step 2.1 — Create `database/src/table_scanner.rs`
Add `mod table_scanner;` to `main.rs`.

### Step 2.2 — Define the TableScanner struct
```rust
use db_config::table::ColumnSpec;
use crate::row::Row;

pub struct TableScanner {
    column_specs: Vec<ColumnSpec>,       // schema of this table
    column_names: Vec<String>,           // column names for schema()
    all_rows: Vec<Row>,                  // all decoded rows (pre-loaded)
    current_index: usize,               // which row to return next
}
```

**Design choice:** We pre-load all rows on creation. This is simple and avoids complex lifetime/borrow issues with DiskManager. With a 64MB limit and tables like customer (~30K rows), this fits in memory.

### Step 2.3 — Implement `TableScanner::new()`
The constructor takes the `DiskManager`, table's `file_id`, and `column_specs`, and reads + decodes ALL blocks into memory:

```rust
impl TableScanner {
    pub fn new(
        disk_manager: &mut DiskManager<impl Read, impl Write>,
        file_id: &str,
        column_specs: Vec<ColumnSpec>,
    ) -> Self {
        // 1. Query disk for start block and number of blocks
        let start_block = disk_manager.get_file_start_block(file_id).unwrap();
        let num_blocks = disk_manager.get_file_num_blocks(file_id).unwrap();

        // 2. Read ALL blocks at once
        let all_block_data = disk_manager.read_blocks(start_block, num_blocks).unwrap();
        let block_size = disk_manager.block_size as usize;

        // 3. Decode each block and collect all rows
        let mut all_rows = Vec::new();
        for i in 0..num_blocks as usize {
            let block_start = i * block_size;
            let block_end = block_start + block_size;
            let block_slice = &all_block_data[block_start..block_end];
            let rows = decode_block(block_slice, &column_specs);
            all_rows.extend(rows);
        }

        // 4. Extract column names for schema()
        let column_names = column_specs.iter()
            .map(|c| c.column_name.clone())
            .collect();

        TableScanner {
            column_specs,
            column_names,
            all_rows,
            current_index: 0,
        }
    }
}
```

**Walkthrough of what happens:**
1. Ask DiskManager "where does this file start?" and "how many blocks does it have?"
2. Read all blocks in one big `read_blocks()` call (efficient — one disk command instead of hundreds).
3. Slice the big byte array into individual blocks (each `block_size` bytes), decode each block's rows, and flatten them all into one big `Vec<Row>`.
4. Store column names so `schema()` can return them.

### Step 2.4 — Implement the Operator trait for TableScanner
```rust
impl Operator for TableScanner {
    fn next(&mut self) -> Option<Row> {
        if self.current_index < self.all_rows.len() {
            let row = self.all_rows[self.current_index].clone();
            self.current_index += 1;
            Some(row)
        } else {
            None
        }
    }

    fn schema(&self) -> Vec<String> {
        self.column_names.clone()
    }
}
```

This is beautifully simple — just walk through the pre-loaded rows one by one.

---

## 3. Both Tasks: Integration Test

### Step 3.1 — Update `main.rs` to test the full table scan
Replace the Day 3/Day 4 test code with:

1. Create a `TableScanner` for the customer table.
2. Loop through all rows using the `Operator` trait's `next()`.
3. Count and print the total number of rows.

```rust
let customer_table = ctx.get_table_specs().iter()
    .find(|t| t.name == "customer").unwrap();

let mut scanner = table_scanner::TableScanner::new(
    &mut disk_manager,
    &customer_table.file_id,
    customer_table.column_specs.clone(),
);

let mut count = 0;
while let Some(row) = scanner.next() {  // using Operator trait
    if count < 5 { println!("Row {}: {}", count, row); }
    count += 1;
}
println!("Total customer rows: {}", count);
```

### Step 3.2 — Run and verify
```bash
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

**What to verify:**
- The total row count should match the number of lines in `scratch/datasets/tpch/customer.csv` (minus the header). For TPC-H scale factor 1, customer has **15,000 rows**.
- You can double-check with: `wc -l scratch/datasets/tpch/customer.csv` (subtract 1 for the header).
- First few rows should still match the CSV data.

### Step 3.3 — Common pitfalls
- **Off-by-one in block slicing.** Make sure `block_start = i * block_size` and `block_end = block_start + block_size`, NOT `(i+1) * block_size + 1`.
- **Forgetting to clone `column_specs`.** `TableSpec` doesn't implement `Copy`, so you need `.clone()` when passing it to the scanner constructor.
- **Import issues.** Make sure `table_scanner.rs` imports from `crate::row` and `crate::operator`.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/operator.rs` | **[NEW]** | `Operator` trait with `next()` and `schema()` |
| `database/src/table_scanner.rs` | **[NEW]** | `TableScanner` struct, `new()` constructor, `Operator` impl |
| `database/src/main.rs` | **[MODIFY]** | Add `mod operator;`, `mod table_scanner;`, replace Day 3/4 test with full scan test |
