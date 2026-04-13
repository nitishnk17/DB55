use crate::operator::Operator;
use crate::row::Row;

use common::DataType;
use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;

pub struct CrossOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right_rows: Vec<Row>,          // right child fully materialized
    current_left_row: Option<Row>, // the current left row we're pairing with
    right_index: usize,            // which right row we're currently at
    output_schema: Vec<String>,    // concatenation of left + right schemas
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> CrossOp<R, W> {
    pub fn new(mut left: Box<dyn Operator<R, W>>, mut right: Box<dyn Operator<R, W>>, pool: &mut BufferPool<R, W>) -> Self {
        // 1. Compute the output schema BEFORE draining right
        //    (schema() is available before next() is called)
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut output_schema = left_schema;
        output_schema.extend(right_schema);

        let mut output_types = left.data_types();
        output_types.extend(right.data_types());

        // 2. Materialize the right child: drain all rows into a Vec
        let mut right_rows = Vec::new();
        while let Some(row) = right.next(pool) {
            right_rows.push(row);
        }
        // 3. Get the first left row
        let current_left_row = left.next(pool);

        CrossOp {
            left,
            right_rows,
            current_left_row,
            right_index: 0,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for CrossOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            // 1. If no current left row, we're done
            let left_row = match &self.current_left_row {
                Some(row) => row,
                None => return None,
            };

            // 2. If we still have right rows to pair with this left row
            if self.right_index < self.right_rows.len() {
                // Combine: left_row.values + right_rows[right_index].values
                let right_row = &self.right_rows[self.right_index];
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                self.right_index += 1;
                return Some(Row {
                    values: combined_values,
                });
            }

            // 3. Exhausted right rows for this left row → advance to next left
            self.current_left_row = self.left.next(pool);
            self.right_index = 0;
            // Loop back to step 1 (if new left_row exists, start pairing again)
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
