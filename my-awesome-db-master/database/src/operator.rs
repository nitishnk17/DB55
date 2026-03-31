use crate::row::Row;

pub trait Operator {
    /// Returns the next row from this operator, or None if exhausted.
    fn next(&mut self) -> Option<Row>;
    /// Returns the output schema (column names) of this operator.
    /// This is needed so downstream operators know what columns are available.
    fn schema(&self) -> Vec<String>;
}
