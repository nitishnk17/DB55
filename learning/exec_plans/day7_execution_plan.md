# Day 7 Execution Plan — Buffer Pool Manager with LRU Eviction

## Goal
Build a Buffer Pool that caches disk blocks in memory. Instead of reading blocks directly from disk every time (like our current TableScanner does), the scanner will ask the Buffer Pool: "give me block X." The Buffer Pool either returns a cached copy (fast!) or fetches it from disk and caches it (slow, but only happens once per block). When the pool is full, it evicts the **Least Recently Used** block.

---

## Background Concepts (Read This First!)

### Why Do We Need a Buffer Pool?
Right now, `TableScanner::new()` calls `disk_manager.read_blocks()` to load **all** blocks of a table at once. This works, but has two problems:
1. **Wasteful:** If two queries scan the same table, we'd read all blocks from disk *twice*. A cache avoids re-reading.
2. **Memory control:** With a 64MB limit, we can only hold a fixed number of blocks. The Buffer Pool manages exactly how many blocks are in RAM and evicts old ones when space runs out.

### The Model: Frames, Pages, and the Page Table
Think of it like a library with a fixed number of shelves (frames):

| Term | What It Is |
|------|-----------|
| **Block** (on disk) | A chunk of data (4096 bytes) stored on the disk simulator |
| **Frame** (in memory) | A slot in RAM that can hold one block's data |
| **Page Table** | A lookup directory: "Block #42 is currently in Frame #7" |
| **Pin** | "I'm using this block right now, don't evict it" |
| **Dirty** | "This block was modified in memory and needs to be written back to disk before evicting" |

```
Buffer Pool (memory)
┌──────────┬──────────┬──────────┬──────────┐
│ Frame 0  │ Frame 1  │ Frame 2  │ Frame 3  │ ← fixed number of frames
│ Block 42 │ Block 7  │ (empty)  │ Block 99 │
│ pin=1    │ pin=0    │          │ pin=0    │
│ dirty=no │ dirty=no │          │ dirty=yes│
└──────────┴──────────┴──────────┴──────────┘

Page Table (HashMap):
  block_42 → frame_0
  block_7  → frame_1
  block_99 → frame_3
```

### LRU (Least Recently Used) Eviction
When all frames are full and you need to load a new block:
1. Find the frame that was used **longest ago** (least recently used).
2. If that frame is **pinned** (someone is still using it), skip it and try the next.
3. If that frame is **dirty**, write its data back to disk first.
4. Overwrite the frame with the new block's data.

**Implementation:** Use a `VecDeque<usize>` (deque of frame indices). On every access, move the frame to the front. When evicting, pop from the back.

---

## 1. P1 Tasks: BufferPool Struct & Frame

### Step 1.1 — Create `database/src/buffer_pool.rs`
Add `mod buffer_pool;` to `main.rs`.

### Step 1.2 — Define the Frame struct
```rust
pub struct Frame {
    pub data: Vec<u8>,          // block_size bytes of data
    pub block_id: Option<u64>,  // which disk block is stored here (None = empty)
    pub dirty: bool,            // modified in memory?
    pub pin_count: u32,         // >0 = in use, don't evict
}
```

### Step 1.3 — Define the BufferPool struct
```rust
use std::collections::{HashMap, VecDeque};
use crate::disk_manager::DiskManager;
use std::io::{Read, Write};

pub struct BufferPool<R: Read, W: Write> {
    frames: Vec<Frame>,
    page_table: HashMap<u64, usize>,  // block_id → frame_index
    lru_list: VecDeque<usize>,        // frame indices, front = most recent
    disk_manager: DiskManager<R, W>,
    block_size: usize,
}
```

**Why does BufferPool own DiskManager?** The BufferPool is now the *only* thing that talks to disk. By moving DiskManager into the pool, we enforce this constraint and avoid borrow-checker issues.

### Step 1.4 — Implement `BufferPool::new()`
```rust
impl<R: Read, W: Write> BufferPool<R, W> {
    pub fn new(num_frames: usize, mut disk_manager: DiskManager<R, W>) -> Self {
        let block_size = disk_manager.block_size as usize;
        let frames = (0..num_frames).map(|_| Frame {
            data: vec![0u8; block_size],
            block_id: None,
            dirty: false,
            pin_count: 0,
        }).collect();

        BufferPool {
            frames,
            page_table: HashMap::new(),
            lru_list: VecDeque::new(),
            disk_manager,
            block_size,
        }
    }
}
```

### Step 1.5 — Implement `fetch_block()`
This is the core method. It returns a reference to the block data:

```rust
pub fn fetch_block(&mut self, block_id: u64) -> &[u8] {
    // 1. Check if block is already cached
    if let Some(&frame_idx) = self.page_table.get(&block_id) {
        self.frames[frame_idx].pin_count += 1;
        // Move to front of LRU
        self.lru_list.retain(|&x| x != frame_idx);
        self.lru_list.push_front(frame_idx);
        return &self.frames[frame_idx].data;
    }

    // 2. Find a free frame or evict
    let frame_idx = self.find_free_or_evict();

    // 3. Read block from disk into this frame
    let data = self.disk_manager.read_blocks(block_id, 1).unwrap();
    self.frames[frame_idx].data.copy_from_slice(&data);
    self.frames[frame_idx].block_id = Some(block_id);
    self.frames[frame_idx].dirty = false;
    self.frames[frame_idx].pin_count = 1;

    // 4. Update page table and LRU
    self.page_table.insert(block_id, frame_idx);
    self.lru_list.push_front(frame_idx);

    &self.frames[frame_idx].data
}
```

**Rust borrow note:** Returning `&[u8]` from `&mut self` is tricky in Rust due to lifetime rules. A simpler approach: return `Vec<u8>` (clone the data). This costs a copy but avoids all borrow-checker fights:
```rust
pub fn fetch_block(&mut self, block_id: u64) -> Vec<u8> {
    // ... same logic but return self.frames[frame_idx].data.clone()
}
```

### Step 1.6 — Implement `unpin()` and `mark_dirty()`
```rust
pub fn unpin(&mut self, block_id: u64) {
    if let Some(&frame_idx) = self.page_table.get(&block_id) {
        if self.frames[frame_idx].pin_count > 0 {
            self.frames[frame_idx].pin_count -= 1;
        }
    }
}

pub fn mark_dirty(&mut self, block_id: u64) {
    if let Some(&frame_idx) = self.page_table.get(&block_id) {
        self.frames[frame_idx].dirty = true;
    }
}
```

---

## 2. P2 Tasks: LRU Eviction Policy

### Step 2.1 — Implement `find_free_or_evict()`
```rust
fn find_free_or_evict(&mut self) -> usize {
    // 1. Check for an empty frame
    for (i, frame) in self.frames.iter().enumerate() {
        if frame.block_id.is_none() {
            return i;
        }
    }

    // 2. Evict LRU: scan from back of lru_list for unpinned frame
    let mut evict_idx = None;
    for i in (0..self.lru_list.len()).rev() {
        let frame_idx = self.lru_list[i];
        if self.frames[frame_idx].pin_count == 0 {
            evict_idx = Some(i);
            break;
        }
    }
    let lru_pos = evict_idx.expect("All frames pinned — out of memory!");
    let frame_idx = self.lru_list.remove(lru_pos).unwrap();

    // 3. If dirty, write back to disk
    if self.frames[frame_idx].dirty {
        let old_block_id = self.frames[frame_idx].block_id.unwrap();
        self.disk_manager.write_blocks(old_block_id, &self.frames[frame_idx].data).unwrap();
    }

    // 4. Remove old mapping from page table
    if let Some(old_block_id) = self.frames[frame_idx].block_id {
        self.page_table.remove(&old_block_id);
    }

    frame_idx
}
```

### Step 2.2 — Also expose disk metadata through BufferPool
Since BufferPool now owns DiskManager, TableScanner needs to query metadata through it:
```rust
pub fn get_file_start_block(&mut self, file_id: &str) -> u64 {
    self.disk_manager.get_file_start_block(file_id).unwrap()
}

pub fn get_file_num_blocks(&mut self, file_id: &str) -> u64 {
    self.disk_manager.get_file_num_blocks(file_id).unwrap()
}

pub fn block_size(&self) -> usize {
    self.block_size
}
```

---

## 3. Both Tasks: Integration

### Step 3.1 — Update `main.rs`
Move `DiskManager` ownership into `BufferPool`:

```rust
// Initialize DiskManager
let disk_manager = disk_manager::DiskManager::new(disk_in, disk_out)?;

// Calculate number of frames from memory limit
let block_size = disk_manager.block_size as usize;
let num_frames = (memory_limit_mb as usize * 1024 * 1024) / block_size;

// Create BufferPool (takes ownership of disk_manager)
let mut buffer_pool = buffer_pool::BufferPool::new(num_frames, disk_manager);
```

**Note:** You need to query `get_memory_limit` BEFORE creating the buffer pool. So the flow becomes:
1. Create `DiskManager`
2. Read query from monitor
3. Query `get_memory_limit` from monitor
4. Create `BufferPool` with `num_frames` = `memory_limit / block_size`
5. Build operator tree (passing `&mut buffer_pool`)
6. Execute and send results

### Step 3.2 — Update `TableScanner::new()` to use BufferPool
Change the constructor to accept `&mut BufferPool` instead of `&mut DiskManager`:

```rust
pub fn new(
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
    file_id: &str,
    column_specs: Vec<ColumnSpec>,
) -> Self {
    let start_block = buffer_pool.get_file_start_block(file_id);
    let num_blocks = buffer_pool.get_file_num_blocks(file_id);
    let block_size = buffer_pool.block_size();

    let mut all_rows = Vec::new();
    for i in 0..num_blocks {
        let block_data = buffer_pool.fetch_block(start_block + i);
        let rows = decode_block(&block_data, &column_specs);
        all_rows.extend(rows);
    }
    // ... rest stays the same
}
```

### Step 3.3 — Update `query_executor::build_operator()` 
Change to accept `&mut BufferPool` instead of `&mut DiskManager`.

### Step 3.4 — Build & Run
```bash
cargo build -r --bin database
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json
```

**What to verify:**
- `Validation success! for Simple Scan - Region` still passes ✅
- The Disk I/O metrics should show the same number of block reads (the buffer pool adds caching but doesn't change correctness for a single scan).
- Optionally: add a second scan query for `customer` and enable it in `monitor_config.json` to test with more data.

---

## Files You Will Create/Modify

| File | Action | What Changes |
|------|--------|-------------|
| `database/src/buffer_pool.rs` | **[NEW]** | `Frame` struct, `BufferPool` struct, `new()`, `fetch_block()`, `unpin()`, `mark_dirty()`, `find_free_or_evict()`, metadata passthroughs |
| `database/src/table_scanner.rs` | **[MODIFY]** | Change `DiskManager` param to `BufferPool` param in `new()` |
| `database/src/query_executor.rs` | **[MODIFY]** | Change `DiskManager` param to `BufferPool` param |
| `database/src/main.rs` | **[MODIFY]** | Add `mod buffer_pool;`, create BufferPool after memory limit query, pass to `build_operator()` |
