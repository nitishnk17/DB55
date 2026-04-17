use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Read, Write};

/// DiskManager wraps the pipe-based communication with the Disk Simulator process.
/// All commands are sent as text lines and responses are either text lines or raw bytes.
pub struct DiskManager<R: Read, W: Write> {
    reader: BufReader<R>,
    writer: W,
    pub block_size: u64,
}

impl<R: Read, W: Write> DiskManager<R, W> {
    /// Create a new DiskManager by taking ownership of the disk read/write pipes.
    /// Immediately queries the block size from the disk simulator.
    pub fn new(disk_in: R, mut disk_out: W) -> Result<Self> {
        // Query block size on initialization
        disk_out.write_all(b"get block-size\n")?;
        disk_out.flush()?;

        let mut reader = BufReader::new(disk_in);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let block_size: u64 = line.trim().parse().context("Failed to parse block size")?;

        Ok(Self {
            reader,
            writer: disk_out,
            block_size,
        })
    }

    // ─── P1: Metadata Methods ────────────────────────────────────────────

    /// Returns the starting block ID of the anonymous (scratch/RW) region.
    pub fn get_anon_start_block(&mut self) -> Result<u64> {
        self.writer.write_all(b"get anon-start-block\n")?;
        self.writer.flush()?;
        self.read_line_as_u64()
    }

    /// Returns the starting block ID for the given file (table).
    pub fn get_file_start_block(&mut self, file_id: &str) -> Result<u64> {
        self.writer
            .write_all(format!("get file start-block {}\n", file_id).as_bytes())?;
        self.writer.flush()?;
        self.read_line_as_u64()
    }

    /// Returns the number of blocks occupied by the given file (table).
    pub fn get_file_num_blocks(&mut self, file_id: &str) -> Result<u64> {
        self.writer
            .write_all(format!("get file num-blocks {}\n", file_id).as_bytes())?;
        self.writer.flush()?;
        self.read_line_as_u64()
    }

    // ─── P2: Block Read/Write Methods ────────────────────────────────────

    /// Reads `count` consecutive blocks starting from `start_id`.
    /// Returns the raw bytes (count * block_size bytes).
    pub fn read_blocks(&mut self, start_id: u64, count: u64) -> Result<Vec<u8>> {
        self.writer
            .write_all(format!("get block {} {}\n", start_id, count).as_bytes())?;
        self.writer.flush()?;

        let total_bytes = (count * self.block_size) as usize;
        let mut buf = vec![0u8; total_bytes];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Writes raw byte data to consecutive blocks starting from `start_id`.
    /// The length of `data` must be a multiple of `block_size`.
    pub fn write_blocks(&mut self, start_id: u64, data: &[u8]) -> Result<()> {
        let count = data.len() as u64 / self.block_size;
        self.writer
            .write_all(format!("put block {} {}\n", start_id, count).as_bytes())?;
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }

    // ─── Helper ──────────────────────────────────────────────────────────

    /// Reads one line from the disk simulator and parses it as u64.
    fn read_line_as_u64(&mut self) -> Result<u64> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        line.trim()
            .parse()
            .context("Failed to parse u64 from disk response")
    }
}