use std::io::{Read, Write};
use common::DataType;
use crate::buffer_pool::BufferPool;
use crate::row::Row;

use common::query::SortSpec;

pub trait Operator<R: Read, W: Write> {
    /// Returns the next row from this operator, or None if exhausted.
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row>;
    /// Returns the output schema (column names) of this operator.
    /// This is needed so downstream operators know what columns are available.
    fn schema(&self) -> Vec<String>;
    /// Returns the data types for each output column.
    /// Used by Sort / Hash Join to correctly encode/decode rows
    /// written to disk scratch space.
    fn data_types(&self) -> Vec<DataType>;
    /// Returns the sort order guaranteed by this operator, if any.
    fn order(&self) -> Option<Vec<SortSpec>> { None }
}
