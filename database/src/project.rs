use crate::operator::Operator;
use crate::row::Row;
use std::collections::HashMap;

use common::DataType;
use std::io::{Read, Write};
use crate::buffer_pool::BufferPool;
use common::query::SortSpec;

pub struct ProjectOp<R: Read, W: Write> {
    child: Box<dyn Operator<R, W>>,
    /// Maps input column name → index in child's row.values[]
    input_indices: Vec<usize>,
    /// The output column names (the "to" names from column_name_map)
    output_schema: Vec<String>,
    /// Data types for the projected output columns
    output_types: Vec<DataType>,
    /// Original column name to projected name map for preserving order
    rename_map: HashMap<String, String>,
}

impl<R: Read, W: Write> ProjectOp<R, W> {
    pub fn new(child: Box<dyn Operator<R, W>>, column_name_map: Vec<(String, String)>) -> Self {
        // Build a lookup from the child's schema: column_name → index
        let child_schema = child.schema();
        let child_types = child.data_types();
        let name_to_idx: HashMap<String, usize> = child_schema
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        let mut input_indices = Vec::new();
        let mut output_schema = Vec::new();
        let mut output_types = Vec::new();
        let mut rename_map = HashMap::new();

        for (from_name, to_name) in &column_name_map {
            let idx = *name_to_idx
                .get(from_name)
                .expect(&format!("Project pushdown failed: could not find {} in child schema", from_name));
            input_indices.push(idx);
            output_schema.push(to_name.clone());
            output_types.push(child_types[idx].clone());
            rename_map.insert(from_name.clone(), to_name.clone());
        }

        ProjectOp {
            child,
            input_indices,
            output_schema,
            output_types,
            rename_map,
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

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }

    fn order(&self) -> Option<Vec<SortSpec>> {
        self.child.order().and_then(|child_order| {
            let mut mapped_order = Vec::new();
            for spec in child_order {
                if let Some(new_name) = self.rename_map.get(&spec.column_name) {
                    mapped_order.push(SortSpec {
                        column_name: new_name.clone(),
                        ascending: spec.ascending,
                    });
                } else {
                    // Sort column was dropped, order no longer guaranteed for subsequent columns
                    break;
                }
            }
            if mapped_order.is_empty() { None } else { Some(mapped_order) }
        })
    }
}
