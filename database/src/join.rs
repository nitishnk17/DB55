use std::io::{Read, Write};
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::operator::Operator;
use crate::row::Row;

pub struct JoinOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_rows: Vec<Row>,
    current_left_row: Option<Row>,
    current_right_idx: usize,
    left_col_idx: usize,
    right_col_idx: usize,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> JoinOp<R, W> {
    pub fn new(
        mut left: Box<dyn Operator<R, W>>,
        mut right: Box<dyn Operator<R, W>>,
        left_col_idx: usize,
        right_col_idx: usize,
        _right_types: Vec<DataType>,
        buffer_pool: &mut BufferPool<R, W>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        // 1. Materialize the right child purely in memory
        let mut right_rows = Vec::new();
        while let Some(row) = right.next(buffer_pool) {
            right_rows.push(row);
        }

        let current_left_row = left.next(buffer_pool);

        JoinOp {
            left,
            right_rows,
            current_left_row,
            current_right_idx: 0,
            left_col_idx,
            right_col_idx,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for JoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            let left_row = match &self.current_left_row {
                Some(row) => row,
                None => return None,
            };

            while self.current_right_idx < self.right_rows.len() {
                let right_row = &self.right_rows[self.current_right_idx];
                self.current_right_idx += 1;

                let left_val = &left_row.values[self.left_col_idx];
                let right_val = &right_row.values[self.right_col_idx];

                if left_val == right_val {
                    let mut combined = left_row.values.clone();
                    combined.extend(right_row.values.clone());
                    return Some(Row { values: combined });
                }
            }

            // Exhausted right rows for current left row, advance left
            self.current_left_row = self.left.next(pool);
            self.current_right_idx = 0;
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
