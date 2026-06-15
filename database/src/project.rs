use crate::operator::Operator;
use crate::row::Row;
use std::collections::HashMap;

use crate::buffer_pool::BufferPool;
use common::{Data, DataType};
use std::io::{Read, Write};

pub struct ProjectOp<R: Read, W: Write> {
    child: Box<dyn Operator<R, W>>,
    /// Maps input column name → index in child's row.values[]
    input_indices: Vec<usize>,
    mode: ProjectMode,
    /// The output column names (the "to" names from column_name_map)
    output_schema: Vec<String>,
    /// Data types for the projected output columns
    output_types: Vec<DataType>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProjectMode {
    Identity,
    OrderedUnique,
    GeneralUnique,
    Duplicate,
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

        // For each (from, to) in the map:
        //   - Find `from` in child's schema → get its index
        //   - Store the index in `input_indices`
        //   - Store `to` in `output_schema`
        let mut input_indices = Vec::new();
        let mut output_schema = Vec::new();
        let mut output_types = Vec::new();

        for (from_name, to_name) in &column_name_map {
            let idx = *name_to_idx.get(from_name).expect(&format!(
                "Project pushdown failed: could not find {} in child schema",
                from_name
            ));
            input_indices.push(idx);
            output_schema.push(to_name.clone());
            output_types.push(child_types[idx].clone());
        }

        let mut seen = std::collections::HashSet::with_capacity(input_indices.len());
        let has_duplicate_indices = input_indices.iter().any(|idx| !seen.insert(*idx));
        let is_identity = input_indices.len() == child_schema.len()
            && input_indices.iter().enumerate().all(|(i, idx)| *idx == i);
        let is_ordered_unique =
            !has_duplicate_indices && input_indices.windows(2).all(|pair| pair[0] < pair[1]);
        let mode = if has_duplicate_indices {
            ProjectMode::Duplicate
        } else if is_identity {
            ProjectMode::Identity
        } else if is_ordered_unique {
            ProjectMode::OrderedUnique
        } else {
            ProjectMode::GeneralUnique
        };

        ProjectOp {
            child,
            input_indices,
            mode,
            output_schema,
            output_types,
        }
    }

    #[inline]
    fn project_ordered_unique(&self, values: Vec<Data>) -> Vec<Data> {
        let mut source = values.into_iter();
        let mut new_values = Vec::with_capacity(self.input_indices.len());
        let mut source_idx = 0usize;
        for &target_idx in &self.input_indices {
            while source_idx < target_idx {
                source.next();
                source_idx += 1;
            }
            new_values.push(
                source
                    .next()
                    .expect("Project index should refer to a live input column"),
            );
            source_idx += 1;
        }
        new_values
    }

    #[inline]
    fn project_general_unique(&self, values: Vec<Data>) -> Vec<Data> {
        let mut source = values.into_iter().map(Some).collect::<Vec<_>>();
        let mut new_values = Vec::with_capacity(self.input_indices.len());
        for &idx in &self.input_indices {
            new_values.push(
                source[idx]
                    .take()
                    .expect("Project index should refer to a live input column"),
            );
        }
        new_values
    }
}

impl<R: Read, W: Write> Operator<R, W> for ProjectOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        // Pull the next row from child
        self.child.next(pool).map(|row| match self.mode {
            ProjectMode::Identity => row,
            ProjectMode::OrderedUnique => Row {
                values: self.project_ordered_unique(row.values),
            },
            ProjectMode::GeneralUnique => Row {
                values: self.project_general_unique(row.values),
            },
            ProjectMode::Duplicate => {
                let mut new_values = Vec::with_capacity(self.input_indices.len());
                for &idx in &self.input_indices {
                    new_values.push(row.values[idx].clone());
                }
                Row { values: new_values }
            }
        })
    }

    fn schema(&self) -> Vec<String> {
        // Return the OUTPUT schema — the renamed column names
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
