# Day 4 Execution Plan — Row Decoding & Block Parsing

## Goal
Take the raw bytes you fetched from the Disk Simulator on Day 3 and turn them into meaningful rows of data. By the end of Day 4, you should be able to read a block, decode every row inside it, and print human-readable output (e.g., `Customer#000000001 | IVhzIApeRb | 15 | ...`).

---

## Background Concepts (Read This First!)

### How Data Is Stored On Disk
Databases don't store data as CSV text — that would waste space and be slow to parse. Instead, each table's data is packed into **fixed-size blocks** (4096 bytes in our case). Within each block, rows are stored as **raw binary bytes**, back-to-back.

### Block Layout
```
┌───────────────────────────────────────────────────────────┐
│ Row1_bytes | Row2_bytes | Row3_bytes | ... | padding | cnt│
└───────────────────────────────────────────────────────────┘
                                              ↑ last 2 bytes
                                              cnt = u16 LE
```
- **Rows** are packed sequentially from the start of the block.
- **`cnt`** (row count) is stored in the **last 2 bytes** of the block as a `u16` in little-endian format. This tells you how many rows are in this block.
- **Padding** is unused space between the last row and the `cnt` footer. You must ignore it.

### Row Layout
Each row is just the column values concatenated in schema order. There are no separators between columns — you know where each column ends based on the data type:

| Data Type | Size in Bytes | How to Read in Rust |
|-----------|--------------|---------------------|
| `Int32`   | 4 (fixed)    | `i32::from_le_bytes(bytes[offset..offset+4])` |
| `Int64`   | 8 (fixed)    | `i64::from_le_bytes(bytes[offset..offset+8])` |
| `Float32` | 4 (fixed)    | `f32::from_le_bytes(bytes[offset..offset+4])` |
| `Float64` | 8 (fixed)    | `f64::from_le_bytes(bytes[offset..offset+8])` |
| `String`  | **variable** | Read bytes until you hit a `0x00` (null terminator). The string content is the bytes *before* the null. Then advance past the null byte. |

**Example:** For schema `(id: Int32, name: String)`, the row `(42, "hi")` looks like:
```
2A 00 00 00 68 69 00
├─ id=42 ─┤ ├ "hi" ┤
           (little-endian i32)
```

### Important Rust Concept: `from_le_bytes`
- `le` stands for "little-endian" — the least significant byte comes first in memory.
- `from_le_bytes` takes a fixed-size byte array (`[u8; 4]` for i32, `[u8; 8]` for i64) and converts it to the numeric type.
- To get a fixed-size array from a slice, use `.try_into().unwrap()`:
  ```rust
  let slice: &[u8] = &bytes[0..4];
  let array: [u8; 4] = slice.try_into().unwrap();
  let value: i32 = i32::from_le_bytes(array);
  ```

### Key Discovery: `Data` Enum Already Exists!
The starter code in `common/src/lib.rs` already provides:
- `Data` enum — equivalent to the `Value` enum from report.md. It has `Data::Int32(i32)`, `Data::Int64(i64)`, `Data::Float32(f32)`, `Data::Float64(f64)`, `Data::String(String)`.
- `DataType` enum — the type tags (`DataType::Int32`, etc.).
- `PartialOrd` and `PartialEq` are already implemented for `Data`.

**You should reuse `Data` instead of creating a new `Value` enum.** This saves duplicate code and lets you leverage the comparison implementations later for Filter/Sort.

---

## 1. P1 Tasks: `Row` Struct & `decode_row()` Function

### Step 1.1 — Create a new file `database/src/row.rs`
Add `mod row;` to `main.rs` (just like you did for `disk_manager`).

### Step 1.2 — Define the `Row` struct
```rust
use common::{Data, DataType};
use db_config::table::ColumnSpec;

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Data>,
}
```
- `values` holds one `Data` value per column, in schema order.
- `#[derive(Debug)]` lets you print rows with `println!("{:?}", row)`.
- `#[derive(Clone)]` is needed later when operators need to duplicate rows (e.g., Cross product).

### Step 1.3 — Implement `decode_row()`
This function reads **one row** from a byte slice starting at a given offset, using the schema to know the types.

```rust
pub fn decode_row(bytes: &[u8], schema: &[ColumnSpec]) -> (Row, usize)
```
- **Input:** `bytes` = the raw block data (starting from the row's position), `schema` = the column definitions for this table.
- **Output:** `(Row, usize)` = the decoded row AND how many bytes were consumed (so the caller knows where the next row starts).
- **Logic:** Loop through each `ColumnSpec` in the schema. For each one, check `col.data_type`:
  - `DataType::Int32` → read 4 bytes, convert with `i32::from_le_bytes`, push `Data::Int32(v)`, advance offset by 4
  - `DataType::Int64` → read 8 bytes, convert with `i64::from_le_bytes`, push `Data::Int64(v)`, advance offset by 8
  - `DataType::Float32` → read 4 bytes, convert with `f32::from_le_bytes`, push `Data::Float32(v)`, advance offset by 4
  - `DataType::Float64` → read 8 bytes, convert with `f64::from_le_bytes`, push `Data::Float64(v)`, advance offset by 8
  - `DataType::String` → scan forward from offset until you find a `0x00` byte. The string is `bytes[offset..offset+end]`. Push `Data::String(s)`. Advance offset by `end + 1` (past the null terminator).

### Step 1.4 — Implement `Display` for `Row` (optional but helpful)
Implement `std::fmt::Display` for `Row` so you can print rows cleanly:
```rust
impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for value in &self.values {
            match value {
                Data::Int32(v)   => write!(f, "{}|", v)?,
                Data::Int64(v)   => write!(f, "{}|", v)?,
                Data::Float32(v) => write!(f, "{}|", v)?,
                Data::Float64(v) => write!(f, "{}|", v)?,
                Data::String(v)  => write!(f, "{}|", v)?,
            }
        }
        Ok(())
    }
}
```
This outputs each column value followed by a pipe `|`, matching the monitor's expected output format.

---

## 2. P2 Tasks: Block Parsing

### Step 2.1 — Implement `decode_block()`
This function takes a full block of raw bytes and the table schema, and returns all the rows inside it.

```rust
pub fn decode_block(block_data: &[u8], schema: &[ColumnSpec]) -> Vec<Row>
```

**Logic (step by step):**
1. **Read the row count from the footer:**
   - The last 2 bytes of the block contain the row count as a `u16` in little-endian.
   - `let row_count = u16::from_le_bytes(block_data[block_data.len()-2..].try_into().unwrap());`
2. **Iterate and decode rows:**
   - Start at byte offset `0`.
   - Loop `row_count` times:
     - Call `decode_row(&block_data[offset..], schema)` → get `(row, bytes_consumed)`.
     - Push `row` into a `Vec<Row>`.
     - Advance `offset += bytes_consumed`.
3. **Return the vector of rows.**

### Step 2.2 — Understanding why the footer matters
Without the footer, you'd have no way to know where the rows end and the padding begins. The padding is just leftover space at the end of the block (a block might not be completely full). The `cnt` footer is the only reliable way to know how many rows are packed into each block.

---

## 3. Both Tasks: Integration Test

### Step 3.1 — Update `main.rs` to test row decoding
After the existing Day 3 code that reads the first block of the customer table, add:

1. Look up the customer table's schema from `ctx.get_table_specs()`. Find the `TableSpec` where `name == "customer"`.
2. Call `decode_block(&block_data, &customer_table.column_specs)`.
3. Print each decoded row.
4. Build and run:
```bash
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

### Step 3.2 — What to verify
**Expected output for the customer table (first few rows):**
Cross-reference with `scratch/datasets/tpch/customer.csv`. The first row should contain something like:
```
1|Customer#000000001|IVhzIApeRb...|15|25-989-741-2988|711.56|BUILDING|to the even...|
```

The customer schema is:
```
c_custkey(I64) | c_name(String) | c_address(String) | c_nationkey(I32) | c_phone(String) | c_acctbal(Float64) | c_mktsegment(String) | c_comment(String)
```

If your decoded output matches the CSV data, Day 4 is complete!

### Step 3.3 — Common pitfalls to watch for
- **Forgetting the `+1` for string null terminator.** If you see garbage data after the first string column, you're probably not skipping past the `0x00` byte.
- **Using wrong byte widths.** Int32 = 4 bytes, Int64 = 8 bytes. Mixing them up will corrupt all subsequent columns in the row.
- **Footer off-by-one.** Make sure you read the last 2 bytes of the block (`block_data.len()-2..`), not `block_data.len()-2..block_data.len()-1`.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/row.rs` | **[NEW]** | `Row` struct, `decode_row()`, `decode_block()`, `Display` impl |
| `database/src/main.rs` | **[MODIFY]** | Add `mod row;`, add test code to decode block 0 of customer table |
