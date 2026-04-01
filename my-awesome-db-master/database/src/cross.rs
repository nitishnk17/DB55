use crate::operator::Operator;
use crate::row::Row;

pub struct CrossOp {
    left: Box<dyn Operator>,       // we pull from left one-at-a-time
    right_rows: Vec<Row>,          // right child fully materialized
    current_left_row: Option<Row>, // the current left row we're pairing with
    right_index: usize,            // which right row we're currently at
    output_schema: Vec<String>,    // concatenation of left + right schemas
}

impl CrossOp {
    pub fn new(mut left: Box<dyn Operator>, mut right: Box<dyn Operator>) -> Self {
        // 1. Compute the output schema BEFORE draining right
        //    (schema() is available before next() is called)
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut output_schema = left_schema;
        output_schema.extend(right_schema);

        // 2. Materialize the right child: drain all rows into a Vec
        let mut right_rows = Vec::new();
        while let Some(row) = right.next() {
            right_rows.push(row);
        }

        // 3. Get the first left row
        let current_left_row = left.next();

        CrossOp {
            left,
            right_rows,
            current_left_row,
            right_index: 0,
            output_schema,
        }
    }
}

impl Operator for CrossOp {
    fn next(&mut self) -> Option<Row> {
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
            self.current_left_row = self.left.next();
            self.right_index = 0;
            // Loop back to step 1 (if new left_row exists, start pairing again)
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }
}
