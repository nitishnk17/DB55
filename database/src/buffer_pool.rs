use crate::disk_manager::DiskManager;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    LRU,
    MRU,
    CLOCK,
}

pub struct Frame {
    pub data: Vec<u8>,
    pub block_id: Option<u64>,
    pub dirty: bool,
    pub pin_count: u32,
    pub referenced: bool,
}

pub struct BufferPool<R: Read, W: Write> {
    frames: Vec<Frame>,
    page_table: HashMap<u64, usize>,
    lru_list: VecDeque<usize>,
    clock_hand: usize,
    policy: EvictionPolicy,
    disk_manager: DiskManager<R, W>,
    block_size: usize,
    /// Next block ID to hand out from the anonymous (scratch) region.
    next_anon_block_id: Option<u64>,
    /// Free list of recycled block IDs.
    free_anon_blocks: Vec<u64>,
}

impl<R: Read, W: Write> BufferPool<R, W> {
    pub fn new(num_frames: usize, disk_manager: DiskManager<R, W>) -> Self {
        let block_size = disk_manager.block_size as usize;
        // Frame.data is no longer allocated upfront: `fetch_block` (the only
        // consumer) is unused in the current pipeline since every operator
        // reads via `read_blocks_sequential` (which bypasses the cache).
        // Allocating `num_frames * block_size` bytes here would waste several
        // MB inside an already tight RLIMIT_AS budget.  Frames remain only
        // as a sizing counter for batch heuristics in operators.
        let frames = (0..num_frames)
            .map(|_| Frame {
                data: Vec::new(),
                block_id: None,
                dirty: false,
                pin_count: 0,
                referenced: false,
            })
            .collect();

        BufferPool {
            frames,
            page_table: HashMap::new(),
            lru_list: VecDeque::new(),
            clock_hand: 0,
            policy: EvictionPolicy::LRU,
            disk_manager,
            block_size,
            next_anon_block_id: None,
            free_anon_blocks: Vec::new(),
        }
    }

    pub fn set_eviction_policy(&mut self, policy: EvictionPolicy) {
        self.policy = policy;
    }

    pub fn num_frames(&self) -> usize {
        self.frames.len()
    }

    // ─── Anonymous (scratch) block allocator ─────────────────────────────

    /// Allocate `num_blocks` consecutive blocks in the anonymous scratch region.
    /// Returns the starting block ID of the allocated range.
    pub fn allocate_anon_blocks(&mut self, num_blocks: u64) -> u64 {
        if num_blocks == 1 {
            if let Some(recycled_block) = self.free_anon_blocks.pop() {
                return recycled_block;
            }
        }

        if self.next_anon_block_id.is_none() {
            self.next_anon_block_id = Some(self.disk_manager.get_anon_start_block().unwrap());
        }
        let start = self.next_anon_block_id.unwrap();
        self.next_anon_block_id = Some(start + num_blocks);
        start
    }

    /// Write `blocks` to disk, reusing freed anonymous block IDs whenever possible
    /// to keep the scratch-space high-watermark bounded.
    ///
    /// Free-list IDs are written individually; any remainder is batch-written to a
    /// fresh sequential range.  Returns the list of block IDs (same length as `blocks`).
    pub fn write_run_blocks(&mut self, blocks: &[Vec<u8>]) -> Vec<u64> {
        if blocks.is_empty() {
            return Vec::new();
        }

        // Keep free-list reuse only for single-block writes; for multi-block
        // spills we allocate contiguous fresh ranges for better prefetchability.
        if blocks.len() == 1 {
            if let Some(id) = self.free_anon_blocks.pop() {
                self.disk_manager.write_blocks(id, &blocks[0]).unwrap();
                self.invalidate_cached_block(id);
                return vec![id];
            }
        }

        let mut raw = Vec::with_capacity(blocks.len() * self.block_size);
        for block in blocks {
            raw.extend_from_slice(block);
        }
        self.write_raw_run_blocks(&raw, blocks.len())
    }

    /// Write a contiguous raw run payload where `raw.len() == num_blocks * block_size`.
    /// Uses contiguous allocation for multi-block writes to maximize sequential I/O.
    pub fn write_raw_run_blocks(&mut self, raw: &[u8], num_blocks: usize) -> Vec<u64> {
        if num_blocks == 0 {
            return Vec::new();
        }
        assert_eq!(raw.len(), num_blocks * self.block_size);

        if self.next_anon_block_id.is_none() {
            self.next_anon_block_id = Some(self.disk_manager.get_anon_start_block().unwrap());
        }
        let start = self.next_anon_block_id.unwrap();
        self.next_anon_block_id = Some(start + num_blocks as u64);
        self.disk_manager.write_blocks(start, raw).unwrap();

        let mut ids = Vec::with_capacity(num_blocks);
        for k in 0..num_blocks as u64 {
            let id = start + k;
            self.invalidate_cached_block(id);
            ids.push(id);
        }

        ids
    }

    /// Recycles the block IDs inside the given Run by pushing them to the free list.
    pub fn free_run(&mut self, run: &crate::disk_run::Run) {
        for &block_id in &run.block_ids {
            self.free_anon_blocks.push(block_id);
            // Invalidate the cache to prevent stale reads of recycled blocks!
            if let Some(frame_idx) = self.page_table.remove(&block_id) {
                self.frames[frame_idx].block_id = None;
                self.frames[frame_idx].dirty = false;
            }
        }
    }

    // ─── Cached block fetch (LRU) ─────────────────────────────────────────

    /// Fetch a single block by ID through the LRU cache.
    /// Returns a clone of the block data.
    ///
    /// On cache hit we push the frame to the front of the LRU list WITHOUT
    /// first removing its old entry (O(1) vs the previous O(n) retain).
    /// Stale duplicates are skipped during eviction scans instead.
    pub fn fetch_block(&mut self, block_id: u64) -> Vec<u8> {
        // Cache hit
        if let Some(&frame_idx) = self.page_table.get(&block_id) {
            self.frames[frame_idx].pin_count += 1;
            self.frames[frame_idx].referenced = true;
            if self.policy != EvictionPolicy::CLOCK {
                self.lru_list.push_front(frame_idx);
            }
            return self.frames[frame_idx].data.clone();
        }

        // Cache miss: find or evict a frame, then read from disk
        let frame_idx = self.find_free_or_evict();
        let data = self.disk_manager.read_blocks(block_id, 1).unwrap();
        // Allocate-on-demand: frame data starts empty (see `BufferPool::new`).
        if self.frames[frame_idx].data.len() != self.block_size {
            self.frames[frame_idx].data = vec![0u8; self.block_size];
        }
        self.frames[frame_idx].data.copy_from_slice(&data);
        self.frames[frame_idx].block_id = Some(block_id);
        self.frames[frame_idx].dirty = false;
        self.frames[frame_idx].pin_count = 1;
        self.frames[frame_idx].referenced = true;
        self.page_table.insert(block_id, frame_idx);
        if self.policy != EvictionPolicy::CLOCK {
            self.lru_list.push_front(frame_idx);
        }
        self.frames[frame_idx].data.clone()
    }

    pub fn unpin(&mut self, block_id: u64) {
        if let Some(&frame_idx) = self.page_table.get(&block_id) {
            if self.frames[frame_idx].pin_count > 0 {
                self.frames[frame_idx].pin_count -= 1;
            }
        }
    }

    #[allow(dead_code)]
    pub fn mark_dirty(&mut self, block_id: u64) {
        if let Some(&frame_idx) = self.page_table.get(&block_id) {
            self.frames[frame_idx].dirty = true;
        }
    }

    // ─── Sequential (non-cached) multi-block read ─────────────────────────

    /// Read `count` consecutive blocks starting at `start_block` in a single
    /// disk call, bypassing the LRU frame cache.
    ///
    /// Use this for sequential table scans where the pages will not be revisited.
    /// Bypassing the cache avoids "sequential flooding" — the phenomenon where a
    /// large sequential scan evicts all useful cached pages.
    ///
    /// Returns the raw bytes: `count * block_size` bytes in total.
    /// Callers slice the result as `&raw[i*block_size..(i+1)*block_size]`.
    pub fn read_blocks_sequential(&mut self, start_block: u64, count: u64) -> Vec<u8> {
        self.disk_manager.read_blocks(start_block, count).unwrap()
    }

    // ─── Direct disk write (bypasses cache) ───────────────────────────────

    /// Write multiple consecutive blocks directly to disk in a single simulator command.
    pub fn write_blocks(&mut self, start_block_id: u64, blocks: &[Vec<u8>]) {
        if blocks.is_empty() {
            return;
        }

        let mut raw = Vec::with_capacity(blocks.len() * self.block_size);
        for block in blocks {
            assert_eq!(block.len(), self.block_size);
            raw.extend_from_slice(block);
        }

        self.disk_manager
            .write_blocks(start_block_id, &raw)
            .unwrap();

        for offset in 0..blocks.len() {
            self.invalidate_cached_block(start_block_id + offset as u64);
        }
    }

    // ─── LRU eviction ─────────────────────────────────────────────────────

    fn find_free_or_evict(&mut self) -> usize {
        // 1. Prefer an empty frame
        for (i, frame) in self.frames.iter().enumerate() {
            if frame.block_id.is_none() {
                return i;
            }
        }

        // 2. Evict according to policy
        let mut evict_pos = None;
        let mut frame_idx_to_evict = 0;

        match self.policy {
            EvictionPolicy::LRU => {
                // Scan from back (least recently used).  Skip stale entries where
                // the frame has been reassigned to a different block since it was
                // pushed (duplicates from the O(1) cache-hit push strategy).
                while let Some(fi) = self.lru_list.pop_back() {
                    // Stale check: the frame's current block must still map to fi in page_table.
                    if let Some(blk) = self.frames[fi].block_id {
                        if self.page_table.get(&blk) != Some(&fi) {
                            continue; // stale duplicate, skip
                        }
                    }
                    if self.frames[fi].pin_count == 0 {
                        frame_idx_to_evict = fi;
                        evict_pos = Some(0); // just to mark found
                        break;
                    }
                    // Pinned — put it back at front to re-try later
                    self.lru_list.push_front(fi);
                }
                if evict_pos.is_none() {
                    panic!("All frames pinned — buffer pool exhausted!");
                }
            }
            EvictionPolicy::MRU => {
                // Scan from front (most recently used).  Skip stale entries.
                while let Some(fi) = self.lru_list.pop_front() {
                    if let Some(blk) = self.frames[fi].block_id {
                        if self.page_table.get(&blk) != Some(&fi) {
                            continue; // stale duplicate, skip
                        }
                    }
                    if self.frames[fi].pin_count == 0 {
                        frame_idx_to_evict = fi;
                        evict_pos = Some(0);
                        break;
                    }
                    self.lru_list.push_back(fi);
                }
                if evict_pos.is_none() {
                    panic!("All frames pinned — buffer pool exhausted!");
                }
            }
            EvictionPolicy::CLOCK => loop {
                let mut is_evicted = false;
                {
                    let frame = &mut self.frames[self.clock_hand];
                    if frame.pin_count == 0 {
                        if frame.referenced {
                            frame.referenced = false;
                        } else {
                            frame_idx_to_evict = self.clock_hand;
                            is_evicted = true;
                        }
                    }
                }
                if is_evicted {
                    self.clock_hand = (self.clock_hand + 1) % self.frames.len();
                    break;
                }
                self.clock_hand = (self.clock_hand + 1) % self.frames.len();
            },
        }

        let frame_idx = frame_idx_to_evict;

        // 3. Write back dirty frame
        if self.frames[frame_idx].dirty {
            let old_block_id = self.frames[frame_idx].block_id.unwrap();
            self.disk_manager
                .write_blocks(old_block_id, &self.frames[frame_idx].data)
                .unwrap();
        }

        // 4. Remove old mapping
        if let Some(old_block_id) = self.frames[frame_idx].block_id {
            self.page_table.remove(&old_block_id);
        }

        frame_idx
    }

    // ─── Disk metadata passthroughs ───────────────────────────────────────

    pub fn get_file_start_block(&mut self, file_id: &str) -> u64 {
        self.disk_manager.get_file_start_block(file_id).unwrap()
    }

    pub fn get_file_num_blocks(&mut self, file_id: &str) -> u64 {
        self.disk_manager.get_file_num_blocks(file_id).unwrap()
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    #[allow(dead_code)]
    pub fn get_anon_start_block(&mut self) -> u64 {
        self.disk_manager.get_anon_start_block().unwrap()
    }

    fn invalidate_cached_block(&mut self, block_id: u64) {
        if let Some(frame_idx) = self.page_table.remove(&block_id) {
            self.frames[frame_idx].block_id = None;
            self.frames[frame_idx].dirty = false;
        }
    }
}
