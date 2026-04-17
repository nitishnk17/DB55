use std::io::{Read, Write};
use common::DataType;
use db_config::table::ColumnSpec;
use crate::buffer_pool::BufferPool;
use crate::row::{Row, decode_block_selected};
use crate::operator::Operator;
use std::collections::VecDeque;
use common::query::SortSpec;
use db_config::statistics::ColumnStat;

pub struct TableScanner {
    column_names: Vec<String>,
    column_types: Vec<DataType>,

    // Support for columnar projection
    full_types: Vec<DataType>,
    needed_indices: Vec<usize>,

    start_block: u64,
    num_blocks: u64,
    current_block: u64,
    batch_rows: VecDeque<Row>,

    // Physical ordering from stats
    physical_order: Option<Vec<SortSpec>>,
}

impl TableScanner {
    pub fn new(
        buffer_pool: &mut BufferPool<impl Read, impl Write>,
        file_id: &str,
        column_specs: &[ColumnSpec],
        needed_column_names: Option<&std::collections::HashSet<String>>,
    ) -> Self {
        let start_block = buffer_pool.get_file_start_block(file_id);
        let num_blocks  = buffer_pool.get_file_num_blocks(file_id);

        let mut column_names = Vec::new();
        let mut column_types = Vec::new();
        let mut needed_indices = Vec::new();
        let mut physical_order = Vec::new();
        let full_types: Vec<DataType> = column_specs.iter().map(|c| c.data_type.clone()).collect();

        for (i, spec) in column_specs.iter().enumerate() {
            let mut is_needed = true;
            if let Some(needed) = needed_column_names {
                if !needed.contains(&spec.column_name) {
                    is_needed = false;
                }
            }

            if is_needed {
                column_names.push(spec.column_name.clone());
                column_types.push(spec.data_type.clone());
                needed_indices.push(i);

                if let Some(stats) = &spec.stats {
                    if stats.iter().any(|s| matches!(s, ColumnStat::IsPhysicallyOrdered)) {
                        physical_order.push(SortSpec {
                            column_name: spec.column_name.clone(),
                            ascending: true,
                        });
                    }
                }
            }
        }

        TableScanner {
            column_names,
            column_types,
            full_types,
            needed_indices,
            start_block,
            num_blocks,
            current_block: start_block,
            batch_rows: VecDeque::new(),
            physical_order: if physical_order.is_empty() { None } else { Some(physical_order) },
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for TableScanner {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            if let Some(r) = self.batch_rows.pop_front() {
                return Some(r);
            }

            if self.current_block >= self.start_block + self.num_blocks {
                return None;
            }

            // Read up to 1024 blocks per I/O call (vs the old 32–256 cap).
            //
            // Each disk call carries seek + rotational latency overhead.
            // For a 60 000-block lineitem table the old cap of 256 meant ~234
            // round-trips; 1024 cuts that to ~59.  Increasing further yields
            // diminishing returns (transfer time dominates) while temporarily
            // holding more raw bytes in memory.
            let remaining = self.start_block + self.num_blocks - self.current_block;
            let batch_size = remaining.min(1024);

            let raw = pool.read_blocks_sequential(self.current_block, batch_size);
            let block_size = pool.block_size();

            for i in 0..batch_size as usize {
                let begin = i * block_size;
                let end   = begin + block_size;
                let block_data = &raw[begin..end];
                let rows = decode_block_selected(block_data, &self.full_types, &self.needed_indices);
                for r in rows {
                    self.batch_rows.push_back(r);
                }
            }
            self.current_block += batch_size;
        }
    }

    fn schema(&self) -> Vec<String> {
        self.column_names.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.column_types.clone()
    }

    fn order(&self) -> Option<Vec<SortSpec>> {
        self.physical_order.clone()
    }
}
