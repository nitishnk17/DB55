# Day 3 Execution Plan & Code Walkthrough

## Goal: Build the Disk I/O Layer
Right now, `database/src/main.rs` communicates with the Disk Simulator using raw UNIX pipes and `write_all("get block-size\n")`. This is messy. Your goal for Day 3 is to encapsulate this communication into a neat, reusable `DiskManager` struct.

---

## 1. P1 Tasks: DiskManager Struct & Metadata Methods
**Status:** Not Started.

**Execution Plan for P1:**
1. **Create a new file:** In `database/src/`, create a new file called `disk_manager.rs` (and don't forget to add `mod disk_manager;` to `main.rs`).
2. **Define the Struct:**
   Create the `DiskManager` struct. It should own the read and write pipes connected to the disk process, and preferably cache the `block_size` so you don't have to query it every time.
   ```rust
   pub struct DiskManager {
       reader: std::io::BufReader<std::fs::File>, // Or whatever type setup_disk_io returns
       writer: std::io::BufWriter<std::fs::File>,
       pub block_size: u64,
   }
   ```
3. **Implement Metadata Methods:**
   Write methods that send quick text queries to the disk simulator and parse the text response. According to `report.md`, you need to implement:
   - `get_anon_start_block(&mut self) -> u64`: Sends `get anon-start-block\n`
   - `get_file_start_block(&mut self, file_id: &str) -> u64`: Sends `get file start-block <fid>\n`
   - `get_file_num_blocks(&mut self, file_id: &str) -> u64`: Sends `get file num-blocks <fid>\n`
   
   *Tip:* Use `.write_all()` followed by `.flush()` on the writer, then use `.read_line()` on the reader and `.trim().parse()` to convert the string response into a `u64`.

---

## 2. P2 Tasks: Block Reading & Writing 
**Status:** Not Started.

**Execution Plan for P2:**
1. **Implement `read_blocks`:**
   ```rust
   pub fn read_blocks(&mut self, start_id: u64, count: u64) -> Vec<u8>
   ```
   - Send the command: `get block <start_id> <count>\n`
   - **Crucial Difference:** Unlike metadata which returns text, `read_blocks` returns **Raw Bytes**.
   - You must initialize a `Vec<u8>` of size `count * self.block_size`. Then use `self.reader.read_exact(&mut buf)` to pull exactly that many bytes from the pipe.

2. **Implement `write_blocks`:**
   ```rust
   pub fn write_blocks(&mut self, start_id: u64, data: &[u8])
   ```
   - Send the command: `put block <start_id> <count>\n` (where `count` is `data.len() / self.block_size`).
   - *Immediately* follow the newline by writing the raw `data` bytes.
   - Run `.flush()` to ensure the bytes are sent. (The disk simulator does not send a response back for puts).

---

## 3. Both Tasks: Integration and Testing

**Execution Plan for Both:**
1. **Refactor `main.rs`:**
   Remove the manual `disk_out.write_all("get block-size\n")` code from `database/src/main.rs`. 
   Instead, instantiate your new `DiskManager`:
   ```rust
   let mut disk_manager = DiskManager::new(disk_in, disk_out);
   ```

2. **The Output Test:**
   - Use `disk_manager.get_file_start_block("customer")` to find out where the customer file begins on disk.
   - Use `disk_manager.read_blocks(start_block, 1)` to fetch the first block of the customer table.
   - Print the first 50-100 bytes of the returned `Vec<u8>` using `String::from_utf8_lossy(&buf[..100])`.
   - Run the system with the Monitor just like you did on Day 2:
     ```bash
     cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
     ```
   - *Verification:* If the printed output contains recognizable text like `"Customer#000... "` interspersed with unreadable binary characters, the Disk layer is successfully implemented!
