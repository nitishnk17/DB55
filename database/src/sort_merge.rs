use std::cmp::Ordering;
use std::io::{Read, Write};

use common::{Data, DataType};

use crate::buffer_pool::BufferPool;
use crate::operator::Operator;
use crate::row::Row;

pub struct SortMergeJoinOp<R: Read, W: Write> {
    left: Box<dyn Operator<R, W>>,
    right: Box<dyn Operator<R, W>>,
    left_col_idx: usize,
    right_col_idx: usize,
    left_row: Option<Row>,
    right_row: Option<Row>,
    active_key: Option<Data>,
    pending_left_row: Option<Row>,
    right_group: Vec<Row>,
    right_group_idx: usize,
    join_key_type: DataType,
    output_schema: Vec<String>,
    output_types: Vec<DataType>,
}

impl<R: Read, W: Write> SortMergeJoinOp<R, W> {
    #[inline]
    fn compare_key_data(left: &Data, right: &Data, key_type: &DataType) -> Ordering {
        match key_type {
            DataType::Int32 => match (left, right) {
                (Data::Int32(a), Data::Int32(b)) => a.cmp(b),
                _ => Ordering::Equal,
            },
            DataType::Int64 => match (left, right) {
                (Data::Int64(a), Data::Int64(b)) => a.cmp(b),
                _ => Ordering::Equal,
            },
            DataType::Float32 => match (left, right) {
                (Data::Float32(a), Data::Float32(b)) => a.total_cmp(b),
                _ => Ordering::Equal,
            },
            DataType::Float64 => match (left, right) {
                (Data::Float64(a), Data::Float64(b)) => a.total_cmp(b),
                _ => Ordering::Equal,
            },
            DataType::String => match (left, right) {
                (Data::String(a), Data::String(b)) => a.cmp(b),
                _ => Ordering::Equal,
            },
        }
    }

    #[inline]
    fn combine_rows(left_vals: &[Data], right_vals: &[Data]) -> Vec<Data> {
        let mut values = Vec::with_capacity(left_vals.len() + right_vals.len());
        values.extend(left_vals.iter().cloned());
        values.extend(right_vals.iter().cloned());
        values
    }

    pub fn new(
        mut left: Box<dyn Operator<R, W>>,
        mut right: Box<dyn Operator<R, W>>,
        left_col_idx: usize,
        right_col_idx: usize,
        buffer_pool: &mut BufferPool<R, W>,
    ) -> Self {
        let mut output_schema = left.schema();
        output_schema.extend(right.schema());

        let left_types = left.data_types();
        let mut output_types = left_types.clone();
        output_types.extend(right.data_types());
        let join_key_type = left_types[left_col_idx].clone();

        let left_row = left.next(buffer_pool);
        let right_row = right.next(buffer_pool);

        SortMergeJoinOp {
            left,
            right,
            left_col_idx,
            right_col_idx,
            left_row,
            right_row,
            active_key: None,
            pending_left_row: None,
            right_group: Vec::new(),
            right_group_idx: 0,
            join_key_type,
            output_schema,
            output_types,
        }
    }
}

impl<R: Read, W: Write> Operator<R, W> for SortMergeJoinOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        loop {
            if let Some(left_row) = &self.pending_left_row {
                if self.right_group_idx < self.right_group.len() {
                    let right_row = &self.right_group[self.right_group_idx];
                    self.right_group_idx += 1;

                    let combined = Self::combine_rows(&left_row.values, &right_row.values);
                    return Some(Row { values: combined });
                }

                let active_key = self.active_key.as_ref().unwrap();
                match &self.left_row {
                    Some(next_left)
                        if Self::compare_key_data(
                            &next_left.values[self.left_col_idx],
                            active_key,
                            &self.join_key_type,
                        ) == Ordering::Equal =>
                    {
                        self.pending_left_row = self.left_row.take();
                        self.left_row = self.left.next(pool);
                        self.right_group_idx = 0;
                        continue;
                    }
                    _ => {
                        self.pending_left_row = None;
                        self.active_key = None;
                        self.right_group.clear();
                        self.right_group_idx = 0;
                    }
                }
            }

            let left_row = match &self.left_row {
                Some(row) => row,
                None => return None,
            };
            let right_row = match &self.right_row {
                Some(row) => row,
                None => return None,
            };

            let left_key = &left_row.values[self.left_col_idx];
            let right_key = &right_row.values[self.right_col_idx];

            match Self::compare_key_data(left_key, right_key, &self.join_key_type) {
                Ordering::Less => {
                    self.left_row = self.left.next(pool);
                }
                Ordering::Greater => {
                    self.right_row = self.right.next(pool);
                }
                Ordering::Equal => {
                    let key = left_key.clone();
                    self.right_group.clear();

                    while let Some(row) = &self.right_row {
                        if Self::compare_key_data(
                            &row.values[self.right_col_idx],
                            &key,
                            &self.join_key_type,
                        ) != Ordering::Equal
                        {
                            break;
                        }
                        self.right_group.push(self.right_row.take().unwrap());
                        self.right_row = self.right.next(pool);
                    }

                    self.active_key = Some(key);
                    self.pending_left_row = self.left_row.take();
                    self.left_row = self.left.next(pool);
                    self.right_group_idx = 0;
                }
            }
        }
    }

    fn schema(&self) -> Vec<String> {
        self.output_schema.clone()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.output_types.clone()
    }
}
