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
        }
    }

    /// Fetch a block by ID. Returns a clone of the block data.
    /// If cached, returns from cache. Otherwise reads from disk.
    pub fn fetch_block(&mut self, block_id: u64) -> Vec<u8> {
        // 1. Check if block is already cached
        if let Some(&frame_idx) = self.page_table.get(&block_id) {
            self.frames[frame_idx].pin_count += 1;
            // Move to front of LRU
            self.lru_list.retain(|&x| x != frame_idx);
            self.lru_list.push_front(frame_idx);
            return self.frames[frame_idx].data.clone();
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

        self.frames[frame_idx].data.clone()
    }

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

    // ─── LRU Eviction ────────────────────────────────────────────────

    fn find_free_or_evict(&mut self) -> usize {
        // 1. Look for an empty frame first
        for (i, frame) in self.frames.iter().enumerate() {
            if frame.block_id.is_none() {
                return i;
            }
        }

        // 2. Evict LRU: scan from back for unpinned frame
        let mut evict_pos = None;
        for i in (0..self.lru_list.len()).rev() {
            let frame_idx = self.lru_list[i];
            if self.frames[frame_idx].pin_count == 0 {
                evict_pos = Some(i);
                break;
            }
        }
        let lru_pos = evict_pos.expect("All frames pinned — out of memory!");
        let frame_idx = self.lru_list.remove(lru_pos).unwrap();

        // 3. If dirty, write back to disk
        if self.frames[frame_idx].dirty {
            let old_block_id = self.frames[frame_idx].block_id.unwrap();
            self.disk_manager
                .write_blocks(old_block_id, &self.frames[frame_idx].data)
                .unwrap();
        }

        // 4. Remove old mapping from page table
        if let Some(old_block_id) = self.frames[frame_idx].block_id {
            self.page_table.remove(&old_block_id);
        }

        frame_idx
    }

    // ─── Disk Metadata Passthroughs ──────────────────────────────────

    pub fn get_file_start_block(&mut self, file_id: &str) -> u64 {
        self.disk_manager.get_file_start_block(file_id).unwrap()
    }

    pub fn get_file_num_blocks(&mut self, file_id: &str) -> u64 {
        self.disk_manager.get_file_num_blocks(file_id).unwrap()
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }
}