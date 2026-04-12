use crate::operator::Operator;
use crate::row::Row;
use std::collections::HashMap;

use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;

pub struct ProjectOp<R: Read, W: Write> {
    child: Box<dyn Operator<R, W>>,
    /// Maps input column name → index in child's row.values[]
    input_indices: Vec<usize>,
    /// The output column names (the "to" names from column_name_map)
    output_schema: Vec<String>,
}

impl<R: Read, W: Write> ProjectOp<R, W> {
    pub fn new(child: Box<dyn Operator<R, W>>, column_name_map: Vec<(String, String)>) -> Self {
        // Build a lookup from the child's schema: column_name → index
        let child_schema = child.schema();
        let name_to_idx: HashMap<String, usize> = child_schema
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // For each (from, to) in the map:
        //   - Find `from` in child's schema → get its index
        //   - Store the index in `input_indices`
        //   - Store `to` in `output_schema`
        let mut input_indices = Vec::new();
        let mut output_schema = Vec::new();

        for (from_name, to_name) in &column_name_map {
            let idx = *name_to_idx
                .get(from_name)
                .expect(&format!("Project pushdown failed: could not find {} in child schema", from_name));
            input_indices.push(idx);
            output_schema.push(to_name.clone());
        }

        ProjectOp {
            child,
            input_indices,
            output_schema,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for ProjectOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        // Pull the next row from child
        self.child.next(pool).map(|row| {
            // Pick only the columns at our pre-computed indices
            let new_values: Vec<_> = self
                .input_indices
                .iter()
                .map(|&idx| row.values[idx].clone())
                .collect();
            Row { values: new_values }
        })
    }

    fn schema(&self) -> Vec<String> {
        // Return the OUTPUT schema — the renamed column names
        self.output_schema.clone()
    }
}
