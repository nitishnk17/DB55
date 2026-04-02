# Day 6 Execution Plan — Monitor Protocol (Output) & First Validation ✅

## Goal
Wire up the output pipeline so the database sends query results back to the monitor for validation. This is the day you get your **first green checkmark** — the monitor will compare your output row-by-row against expected output and confirm it matches.

---

## Background Concepts (Read This First!)

### The Output Protocol
After the database finishes computing a query, it must talk to the monitor using this exact protocol:

```
Database sends to Monitor:
─────────────────────────
validate\n                      ← "I'm ready to send results"
col1|col2|col3|...|colN|\n      ← one line per result row (pipe-separated, trailing pipe!)
col1|col2|col3|...|colN|\n      ← next row...
!\n                              ← "I'm done sending results"
```

**Critical format details:**
- Every column value is separated by `|`.
- There is a **trailing pipe** after the last column (e.g., `1|hello|42|` not `1|hello|42`).
- Each row ends with `\n`.
- After all rows, send `!\n` to signal the end.
- You must call `.flush()` after writing everything.

### How the Monitor Validates
Looking at the `monitor/src/main.rs` `validate()` function (lines 122–153):
1. It reads the expected output from a CSV file line by line.
2. It reads your database output line by line.
3. It **trims** both and compares them as strings.
4. When the expected file runs out of lines, it expects `!` from the database.
5. If any line doesn't match → `Validation failed!`.

### What's the Query Tree Builder?
Right now, `main.rs` reads the query JSON and prints it but never acts on it. The query is an AST tree (`QueryOp::Scan`, `QueryOp::Filter`, etc.). You need a function that recursively walks this tree and builds a chain of `Operator` objects. For Day 6, we only need to handle `Scan`.

### Current Issue: `TableA` is a Placeholder
The test queries in `monitor_config.json` use `"table_id": "TableA"`, but your actual tables are named `orders`, `customer`, `nation`, `region`, etc. You need to:
1. Update `monitor_config.json` to use a real table name (e.g., `region` — it's the smallest).
2. Generate the expected output CSV using the SQLite database.

---

## 1. P2 Tasks: Memory Limit Query (Already Done!)
The `get_memory_limit` query is already implemented in `main.rs` (lines 80–86). This task is **complete** — you already send `get_memory_limit\n` to the monitor and parse the response. ✅

---

## 2. P1 Tasks: Result Output to Monitor + Query Tree Builder

### Step 2.1 — Create `database/src/query_executor.rs`
Add `mod query_executor;` to `main.rs`.

This module will have a function that takes a `QueryOp` AST node and builds the corresponding `Operator`:

```rust
use common::query::QueryOp;
use crate::operator::Operator;
use crate::table_scanner::TableScanner;
use crate::disk_manager::DiskManager;
use db_config::DbContext;
use std::io::{Read, Write};

pub fn build_operator(
    query_op: &QueryOp,
    ctx: &DbContext,
    disk_manager: &mut DiskManager<impl Read, impl Write>,
) -> Box<dyn Operator> {
    match query_op {
        QueryOp::Scan(scan_data) => {
            // Look up the table spec by table_id
            let table_spec = ctx.get_table_specs().iter()
                .find(|t| t.name == scan_data.table_id)
                .expect(&format!("Table '{}' not found", scan_data.table_id));
            
            Box::new(TableScanner::new(
                disk_manager,
                &table_spec.file_id,
                table_spec.column_specs.clone(),
            ))
        }
        // We'll add Filter, Project, Cross, Sort in later days
        _ => panic!("Operator not yet implemented"),
    }
}
```

**Key Rust concept — `Box<dyn Operator>`:** Since different operators (`TableScanner`, `FilterOp`, etc.) are different types, we can't return just one type. `Box<dyn Operator>` means "a heap-allocated object that implements the `Operator` trait." This is called **dynamic dispatch** (or trait objects). It lets us return any operator type through the same interface.

### Step 2.2 — Restructure `main.rs`
Replace the Day 5 test code in `main.rs` with the proper execution flow:

```rust
// 1. Build the operator tree from the query AST
let mut root_op = query_executor::build_operator(&query.root, &ctx, &mut disk_manager);

// 2. Send "validate" to monitor
monitor_out.write_all(b"validate\n")?;

// 3. Loop through all rows and send each to the monitor
while let Some(row) = root_op.next() {
    // Row's Display trait already formats as "col1|col2|...|colN|"
    monitor_out.write_all(format!("{}\n", row).as_bytes())?;
}

// 4. Send end-of-output marker
monitor_out.write_all(b"!\n")?;
monitor_out.flush()?;
```

**Important:** Your `Row`'s `Display` implementation already outputs `col1|col2|...|colN|` (with trailing pipe). So `format!("{}\n", row)` gives exactly the format the monitor expects! 

### Step 2.3 — Remove the `get_memory_limit` code for now
The memory limit query must happen BEFORE `validate`. But right now, the monitor's `handle_db` function (look at `monitor/src/main.rs` line 164) reads commands in a loop:
- If it sees `get_memory_limit` → responds with the limit
- If it sees `validate` → starts comparing output rows

So the correct order in your `main.rs` should be:
1. Read query from monitor
2. Query `get_memory_limit` 
3. Build the operator tree
4. Send `validate\n`
5. Send rows
6. Send `!\n`

---

## 3. Both Tasks: Generate Expected Output & Test

### Step 3.1 — Generate expected output using SQLite
The generator created a `sqlite.db` file at `scratch/compiled_datasets/tpch/sqlite.db`. You can query it to generate expected output.

For the `region` table (5 rows, simplest table):
```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db \
  "SELECT r_regionkey, r_name, r_comment, '' FROM region;" \
  -separator '|' > scratch/runtimes/tpch/expected_1.csv
```

**Why `''` at the end?** The monitor expects a trailing `|` after the last column. Adding an empty column with `''` creates that trailing pipe in SQLite's output (since the separator is `|`).

### Step 3.2 — Update `monitor_config.json`
Change the first test query to scan the `region` table (instead of the placeholder `TableA`):

```json
{
    "execution_name": "Simple Scan - Region",
    "disabled": false,
    "query": {
        "root": {
            "Scan": {
                "table_id": "region"
            }
        }
    },
    "expected_output_file": ".../expected_1.csv",
    "memory_limit_mb": 64
}
```

### Step 3.3 — Build & Run
```bash
cargo build -r --bin database
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

### Step 3.4 — What to expect
If everything works, you should see:
```
Validation success! for Simple Scan - Region
```
🎉 **First green checkmark!**

If validation fails, the monitor will print:
```
Expected line output
<expected line>
but database returned
<your line>
error at line N
```
This tells you exactly which line mismatches. Common issues:
- **Float formatting:** SQLite might output `123.0` while Rust outputs `123`. You may need to tweak `Display` for `Float32`/`Float64` later.
- **Missing trailing pipe:** Ensure `Row`'s `Display` outputs `val|` for every column including the last.
- **Extra whitespace or newlines:** Use `.trim()` careful comparison if debugging.

### Step 3.5 — Test with a bigger table
Once region passes, try customer (15,000 rows):
```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db \
  "SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, '' FROM customer;" \
  -separator '|' > scratch/runtimes/tpch/expected_2.csv
```
Update the second query config to `"table_id": "customer"` and `"disabled": false`, then run again.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/query_executor.rs` | **[NEW]** | `build_operator()` function — maps `QueryOp` AST to `Box<dyn Operator>` |
| `database/src/main.rs` | **[MODIFY]** | Add `mod query_executor;`, replace test code with proper execution flow (build → validate → send rows → `!\n`) |
| `monitor_config.json` | **[MODIFY]** | Change `TableA` → `region`, update execution name |
| `expected_1.csv` | **[MODIFY]** | Generate using SQLite for the region table |
