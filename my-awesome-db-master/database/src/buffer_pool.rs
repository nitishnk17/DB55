use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use crate::disk_manager::DiskManager;

pub struct Frame {
    pub data: Vec<u8>,
    pub block_id: Option<u64>,
    pub dirty: bool,
    pub pin_count: u32,
}

pub struct BufferPool<R: Read, W: Write> {
    frames: Vec<Frame>,
    page_table: HashMap<u64, usize>,
    lru_list: VecDeque<usize>,
    disk_manager: DiskManager<R, W>,
    block_size: usize,
    /// Next block ID to hand out from the anonymous (scratch) region.
    next_anon_block_id: Option<u64>,
}

impl<R: Read, W: Write> BufferPool<R, W> {
    pub fn new(num_frames: usize, disk_manager: DiskManager<R, W>) -> Self {
        let block_size = disk_manager.block_size as usize;
        let frames = (0..num_frames)
            .map(|_| Frame {
                data: vec![0u8; block_size],
                block_id: None,
                dirty: false,
                pin_count: 0,
            })
            .collect();

        BufferPool {
            frames,
            page_table: HashMap::new(),
            lru_list: VecDeque::new(),
            disk_manager,
            block_size,
            next_anon_block_id: None,
        }
    }

    // ─── Anonymous (scratch) block allocator ─────────────────────────────

    /// Allocate `num_blocks` consecutive blocks in the anonymous scratch region.
    /// Returns the starting block ID of the allocated range.
    ///
    /// Calling with `num_blocks = 0` is a "peek": it returns the current pointer
    /// without advancing it.  This lets callers record a start address before
    /// allocating blocks one-by-one inside a loop.
    pub fn allocate_anon_blocks(&mut self, num_blocks: u64) -> u64 {
        if self.next_anon_block_id.is_none() {
            self.next_anon_block_id =
                Some(self.disk_manager.get_anon_start_block().unwrap());
        }
        let start = self.next_anon_block_id.unwrap();
        self.next_anon_block_id = Some(start + num_blocks);
        start
    }

    // ─── Cached block fetch (LRU) ─────────────────────────────────────────

    /// Fetch a single block by ID through the LRU cache.
    /// Returns a clone of the block data.
    pub fn fetch_block(&mut self, block_id: u64) -> Vec<u8> {
        // Cache hit
        if let Some(&frame_idx) = self.page_table.get(&block_id) {
            self.frames[frame_idx].pin_count += 1;
            self.lru_list.retain(|&x| x != frame_idx);
            self.lru_list.push_front(frame_idx);
            return self.frames[frame_idx].data.clone();
        }

        // Cache miss: find or evict a frame, then read from disk
        let frame_idx = self.find_free_or_evict();
        let data = self.disk_manager.read_blocks(block_id, 1).unwrap();
        self.frames[frame_idx].data.copy_from_slice(&data);
        self.frames[frame_idx].block_id   = Some(block_id);
        self.frames[frame_idx].dirty      = false;
        self.frames[frame_idx].pin_count  = 1;
        self.page_table.insert(block_id, frame_idx);
        self.lru_list.push_front(frame_idx);
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

    /// Write a single block directly to disk (bypassing the LRU cache).
    /// Used when writing sorted runs / hash-join partitions to scratch space.
    pub fn write_block(&mut self, block_id: u64, data: &[u8]) {
        self.disk_manager.write_blocks(block_id, data).unwrap();
    }

    // ─── LRU eviction ─────────────────────────────────────────────────────

    fn find_free_or_evict(&mut self) -> usize {
        // 1. Prefer an empty frame
        for (i, frame) in self.frames.iter().enumerate() {
            if frame.block_id.is_none() {
                return i;
            }
        }

        // 2. Evict the least-recently-used unpinned frame
        let mut evict_pos = None;
        for i in (0..self.lru_list.len()).rev() {
            let frame_idx = self.lru_list[i];
            if self.frames[frame_idx].pin_count == 0 {
                evict_pos = Some(i);
                break;
            }
        }
        let lru_pos = evict_pos.expect("All frames pinned — buffer pool exhausted!");
        let frame_idx = self.lru_list.remove(lru_pos).unwrap();

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
}
