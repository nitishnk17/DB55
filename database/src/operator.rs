use std::io::{Read, Write};
use db_config::table::ColumnSpec;
use crate::buffer_pool::BufferPool;
use crate::row::Row;

pub trait Operator<R: Read, W: Write> {
    /// Returns the next row from this operator, or None if exhausted.
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row>;
    /// Returns the output schema (column names) of this operator.
    /// This is needed so downstream operators know what columns are available.
    fn schema(&self) -> Vec<String>;
    /// Returns the full column specs (name + data type + stats) for this operator's
    /// output columns.  Used by Sort / Hash Join to correctly encode/decode rows
    /// written to disk scratch space, avoiding the fragile reverse-lookup by name.
    fn column_specs(&self) -> Vec<ColumnSpec>;
}
