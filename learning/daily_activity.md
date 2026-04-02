# Day 2 Activity & Learning Log (March 21, 2026)

## 1. Activities Completed
- **[P1] Data Generation:** Successfully converted the raw `.csv` and `.schema` files in `scratch/datasets/tpch/` into binary block-addressable `.bin` files using the `generator` utility. This established our test dataset in `scratch/compiled_datasets/tpch/`.
- **[P1/P2] Environment Verification:** Successfully produced the necessary runtime configurations including `db_config.json`, `disk_sim_config.json`, and `monitor_config.json` inside `scratch/runtimes/tpch/`.
- **[P2] Code Familiarization:** Explored the crucial starter files:
  - `common/src/query.rs` to understand how SQL queries are parsed into a recursive Abstract Syntax Tree (AST) of `QueryOp`s (Scan, Sort, Project, Filter, Cross).
  - `configs/db_config/src/table.rs` to understand `TableSpec` and `ColumnSpec`, which define the layout of data types we'll encounter.
  - `database/src/main.rs` to map the entrypoint, observing how the database loads configurations, sets up UNIX pipes for inter-process communication, and handles initial handshakes.
- **[Both] End-to-End Pipeline Verification:** Enabled a test query in `monitor_config.json` and executed the `monitor` binary. We successfully confirmed the data flow between the Monitor, Database, and Disk Simulator processes.

---

## 2. Key Learnings & Observations

### The Monitor-Database-Disk Handshake
By analyzing the terminal output when running the monitor, we gained a practical understanding of how our Database boots up and communicates:

1. **Initialization:** The database starts and prints the parsed tables (e.g., `orders`, `partsupp`, `customer`, `lineitem`), proving `DbContext::load_from_file` successfully loaded `db_config.json`.
2. **Query Intake:** The Database receives the active query from the Monitor via a UNIX pipe.
   - *Output Observed:* `Input query is: Query { root: Scan( ScanData { table_id: "TableA" } ) }`
3. **Disk Handshake:** The Database successfully sends a `get block-size\n` command to the Disk process and receives the response.
   - *Output Observed:* `block size is 4096`
4. **Data Retrieval:** The Database fetches the very first block (`block 0`) of data from the disk and prints the raw byte contents.
   - *Output Observed:* `First few bytes of block 0 contains ... \u{7}A1996-01-02\05-LOW\0Clerk#0"`
   - *Learning:* The disk simulator serves data sequentially as expected, and we can clearly see the UTF-8 representations of Strings (like dates and clerks) interspersed with raw bytes representing integers and floats.
5. **Memory Constraint & Crash:** The database requests its memory limit from the monitor.
   - *Output Observed:* `Memory limit is set to 64 MB`
   - *Learning:* Right after this, the monitor intentionally crashed (`Validation failed!`). This occurred because our database (currently just dummy logic) finished execution *without* sending the computed query rows back to the monitor using the `validate\n` command and row pipelines.

### Conclusion for Day 2
Day 2's core objective was familiarizing ourselves with the environment and proving the inter-process pipelines work. The crash at the end of the `monitor` run is a successful outcome; it confirms the pipelines are perfectly intact and waiting for us to write the actual Database logic to parse those disk bytes, format the rows, and pass them back to the validation script.

We are ready to handle Disk I/O properly on Day 3!

---
---

# Day 3 Activity & Learning Log (March 22, 2026)

## 1. Activities Completed
- **[P1] DiskManager Struct & Metadata Methods:** Created `database/src/disk_manager.rs` with a generic `DiskManager<R: Read, W: Write>` struct that wraps disk pipe communication.
- **[P2] Block Read/Write Methods:** Implemented `read_blocks()` and `write_blocks()` for raw byte I/O with the Disk Simulator.
- **[Both] Integration & Refactoring:** Refactored `database/src/main.rs` to use `DiskManager` instead of raw pipe string commands. Verified build succeeds.

---

## 2. Issues Found & Fixed During Review

### Original Implementation Issues
The initial attempt at `disk_manager.rs` had several fundamental problems:

| Issue | What Was Wrong | How It Was Fixed |
|-------|---------------|-----------------|
| **Wrong communication model** | Used `seek()` + `read_u64::<LittleEndian>()` as if reading a local file | Changed to text-based pipe protocol: `write_all("get ...\n")` → `read_line()` → `parse()` |
| **Wrong types for `file_id`** | `file_id` was `u64` | Changed to `&str` (table names like `"customer"`, not numeric IDs) |
| **Syntax errors** | `std::io:BufReader` (single colon), missing comma after `writer` field | Fixed to `std::io::BufReader` (double colon), added commas |
| **Wrong reader/writer types** | Used `std::fs::File` | Used generics `R: Read, W: Write` to match `io_setup.rs` returning `impl Read`/`impl Write` |
| **Missing constructor** | No `new()` method, no `block_size` initialization | Added `new()` that queries block size on creation |
| **Missing methods** | No `read_blocks()`, no `write_blocks()` | Implemented both with correct byte-level I/O |

### Final DiskManager API
```rust
pub struct DiskManager<R: Read, W: Write> { reader, writer, pub block_size }

// Constructor — queries block size automatically
DiskManager::new(disk_in, disk_out) -> Result<Self>

// P1 Metadata methods — send text command, parse text response
get_anon_start_block()       -> Result<u64>
get_file_start_block(file_id: &str) -> Result<u64>
get_file_num_blocks(file_id: &str)  -> Result<u64>

// P2 Block I/O methods — send text command, read/write raw bytes
read_blocks(start_id, count) -> Result<Vec<u8>>
write_blocks(start_id, data) -> Result<()>
```

---

## 3. Key Learnings

### Pipe vs File I/O
The Disk Simulator communicates via **UNIX pipes** (stdin/stdout), NOT via file system access. This means:
- You **cannot** `seek()`. Data flows in one direction — you send a command and read the response sequentially.
- Metadata responses (block size, start block, num blocks) come as **text lines** that you `read_line()` and `parse()`.
- Block data responses come as **raw bytes** that you `read_exact()` into a pre-sized buffer of `count × block_size` bytes.
- Block writes use `put block <id> <count>\n` followed by the raw bytes, with **no response** from the disk.

### Generic Types for Pipe Wrappers
The starter code's `setup_disk_io()` returns `(impl Read, impl Write)` — opaque types backed by `ReadFdWrapper`/`WriteFdWrapper`. Since these are not `std::fs::File`, the `DiskManager` must be generic over `R: Read` and `W: Write`, or accept `BufReader`/`BufWriter` wrappers.

### Verification
Build verified with `cargo build -r --bin database` — compiles cleanly with no warnings.

We are now ready for Day 4: Row Decoding!

---
---

# Day 4 Activity & Learning Log (March 23, 2026)

## 1. Activities Completed
- **[P1] Row Struct & `decode_row()`:** Created `database/src/row.rs` with a `Row` struct (wrapping `Vec<Data>`) and a `decode_row()` function that reads one row from raw bytes using the table schema.
- **[P1] Display Implementation:** Implemented `std::fmt::Display` for `Row` to output pipe-delimited values (`col1|col2|...|`), matching the monitor's expected output format.
- **[P2] Block Parsing with `decode_block()`:** Implemented `decode_block()` that reads the row count from the block's 2-byte footer (`u16` LE), then loops and decodes each row sequentially.
- **[Both] Integration Test:** Updated `main.rs` to look up the customer table schema from `DbContext`, call `decode_block()` on block 0, and print the first 5 decoded rows. Verified output matches `customer.csv`.

---

## 2. Issues Found & Fixed During Review

| # | Issue | Fix |
|---|-------|-----|
| 1 | `use commong::` — typo in import | → `use common::` |
| 2 | `#derive(Debug, Clone)]` — missing `[` in derive attribute | → `#[derive(Debug, Clone)]` |
| 3 | `pub fn_decode_block` — underscore instead of space | → `pub fn decode_block` |

The core `decode_row()` and `decode_block()` logic was correct on first attempt.

---

## 3. Key Learnings

### Binary Row Encoding
- Rows are stored as raw binary bytes, not text. Each column's bytes are concatenated in schema order with no separators.
- Fixed-size types (`Int32` = 4 bytes, `Int64` = 8 bytes, `Float32` = 4 bytes, `Float64` = 8 bytes) are read using `from_le_bytes()`.
- Strings are **variable-length**, terminated by a `0x00` null byte. You scan forward until you find it, then advance past it.

### Block Footer
- The **last 2 bytes** of every block contain the row count as a `u16` in little-endian. This is the only way to know how many rows are packed in a block, since remaining space is just padding.

### Reusing Starter Code Types
- **Key discovery:** The starter code already provides a `Data` enum in `common/src/lib.rs` that is identical to the `Value` enum described in the report. It already implements `PartialOrd` and `PartialEq`, which will be needed for Filter and Sort operators.
- Using `Data` instead of creating a custom `Value` enum avoids duplication and ensures compatibility with the rest of the codebase.

### Verification
- Build succeeded with `cargo build -r --bin database`.
- Ran via monitor; decoded rows from customer block 0 matched `scratch/datasets/tpch/customer.csv` exactly.

We are now ready for Day 5: Table Scanner!

---
---

# Day 5 Activity & Learning Log (March 24, 2026)

## 1. Activities Completed
- **[P2] Operator Trait:** Created `database/src/operator.rs` defining the `Operator` trait with `next() -> Option<Row>` and `schema() -> Vec<String>` — the foundation for the Volcano/iterator model.
- **[P1] TableScanner:** Created `database/src/table_scanner.rs` with a `TableScanner` struct that pre-loads all blocks of a table into memory and yields rows one-by-one via the `Operator` trait.
- **[Both] Full Table Scan Test:** Updated `main.rs` to create a `TableScanner` for the customer table and iterate through all rows. Verified total count = **15,000 rows** and first 5 rows match `customer.csv`.

---

## 2. Issues Found & Fixed During Review

| # | File | Issue | Fix |
|---|------|-------|-----|
| 1 | `operator.rs` | Stray `.` at end of file | Removed |
| 2 | `table_scanner.rs` | Missing all imports (`Read`, `Write`, `DiskManager`, `decode_block`, `Operator`) | Added complete import block |
| 3 | `table_scanner.rs` | Missing `struct TableScanner` definition | Added struct with 4 fields: `column_specs`, `column_names`, `all_rows`, `current_index` |
| 4 | `table_scanner.rs` | Stray `.` at end of file | Removed |
| 5 | `table.rs` | `ColumnSpec` / `TableSpec` didn't derive `Clone` | Added `Clone` to derive macro |
| 6 | `statistics.rs` | All stats types (`Range`, `Frequency`, `Density`, `HistogramData`, `CardinalityData`, `ColumnStat`) didn't derive `Clone` | Added `Clone` to all — needed because `ColumnSpec` contains `Option<Vec<ColumnStat>>` |

---

## 3. Key Learnings

### The Volcano / Iterator Model
Every operator in the query engine exposes the same `next()` interface. This means:
- Operators are **composable**: `Filter.next()` calls `Scan.next()` internally.
- Execution is **lazy**: rows are produced on-demand, not all materialized at once.
- Adding new operators is easy — just implement the `Operator` trait.

### Clone Derive Chain in Rust
When you `#[derive(Clone)]` on a struct, **every field** in that struct must also implement `Clone`. Since `ColumnSpec` contains `Option<Vec<ColumnStat>>`, and `ColumnStat` contains `Range` which contains `Data`, the entire chain needed `Clone` added: `Data` (already had it) → `Range` → `Frequency` → `Density` → `HistogramData` → `CardinalityData` → `ColumnStat` → `ColumnSpec` → `TableSpec`.

### Pre-loading Strategy
Instead of managing complex mutable borrows to `DiskManager` during iteration, the scanner reads **all blocks at once** in the constructor and decodes them into a `Vec<Row>`. This is simple, avoids lifetime issues, and fits within the 64MB memory limit for TPC-H scale factor 1.

### Verification
- `Total customer rows: 15000` ✅
- Disk I/O metrics show 618 blocks processed in a single read call — efficient batch read.
- First 5 rows match `customer.csv` exactly.

We are now ready for Day 6: Monitor Protocol (Output) — the first full validation pass!

---
---

# Day 6 Activity & Learning Log (March 25, 2026)

## 1. Activities Completed
- **[P1] Query Executor:** Created `database/src/query_executor.rs` with a `build_operator()` function that maps the `QueryOp` AST tree to a concrete `Box<dyn Operator>`. Currently handles `QueryOp::Scan` → `TableScanner`.
- **[P1] Monitor Output Protocol:** Rewrote `main.rs` with the proper execution flow: `get_memory_limit` → `build_operator()` → `validate\n` → send rows (pipe-delimited) → `!\n` → flush.
- **[P2] Memory Limit (Already Done):** The `get_memory_limit` query was already implemented from Day 5 code.
- **[Both] Expected Output & Validation:** Generated `expected_1.csv` using SQLite for the `region` table. Updated `monitor_config.json` to scan `region` instead of placeholder `TableA`. Achieved **first `Validation success!`** 🎉

---

## 2. Issues Found & Fixed During Review

| # | File | Issue | Fix |
|---|------|-------|-----|
| 1 | `query_executor.rs` | `DiskManager` written without generic params | → `DiskManager<impl Read, impl Write>` |
| 2 | `query_executor.rs` | `scan_op.table_name` — wrong field name | → `scan_data.table_id` (matching `ScanData` struct) |
| 3 | `query_executor.rs` | Stray `.` at end of file | Removed |
| 4 | `main.rs` | Still had Day 5 customer test code and commented-out validation | Replaced with proper execution flow |
| 5 | `monitor_config.json` | `table_id: "TableA"` — placeholder, not a real table | → `"region"` |
| 6 | `expected_1.csv` | Empty file (0 bytes) | Generated via SQLite with correct pipe-delimited format |

---

## 3. Key Learnings

### Monitor Output Protocol
The database must send results in this exact format:
```
validate\n          ← signal start of validation
col1|col2|...|colN|\n   ← one line per row (trailing pipe!)
!\n                  ← signal end of output
```
The monitor reads each line, trims whitespace, and compares against the expected CSV line-by-line.

### `Box<dyn Operator>` — Dynamic Dispatch in Rust
Since `build_operator()` can return different types (`TableScanner`, `FilterOp`, etc.) based on the query AST, we use `Box<dyn Operator>` — a heap-allocated trait object. This enables **dynamic dispatch**: the correct `next()` implementation is called at runtime based on the actual type inside the box. This is the Rust equivalent of polymorphism.

### Generating Expected Output with SQLite
The generator creates a `sqlite.db` alongside the binary data files. To produce expected output:
```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db \
  "SELECT col1, col2, ..., '' FROM table;" -separator '|'
```
The trailing `''` column creates the required trailing pipe after the last real column.

### Verification
```
Validation success! for Simple Scan - Region ✅
```
- Region table: 5 rows, 1 block, 1 disk read.
- This is the **first end-to-end validation** — query flows from Monitor → Database → Disk → decode → format → validate.

We are now ready for Day 7: Buffer Pool!
