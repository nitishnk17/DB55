use std::collections::HashMap;
use std::cmp::Ordering;
use common::Data;
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use crate::operator::Operator;
use crate::row::Row;

pub struct FilterOp {
    child: Box<dyn Operator>,
    predicates: Vec<Predicate>,
    col_index_map: HashMap<String, usize>,
}

impl FilterOp {
    pub fn new(child: Box<dyn Operator>, predicates: Vec<Predicate>) -> Self {
        // Build column name → index mapping from the child's schema
        let col_index_map: HashMap<String, usize> = child
            .schema()
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        FilterOp {
            child,
            predicates,
            col_index_map,
        }
    }
}

impl Operator for FilterOp {
    fn next(&mut self) -> Option<Row> {
        // Keep pulling rows from child until one passes all predicates
        while let Some(row) = self.child.next() {
            if evaluate_all_predicates(&self.predicates, &row, &self.col_index_map) {
                return Some(row);
            }
        }
        None
    }

    fn schema(&self) -> Vec<String> {
        // Filter doesn't change the schema — same columns in, same columns out
        self.child.schema()
    }
}

/// Convert a ComparisionValue (from the query AST) into a Data value
/// that can be compared against a row's column value.
fn resolve_value(
    value: &ComparisionValue,
    row: &Row,
    col_index_map: &HashMap<String, usize>,
) -> Data {
    match value {
        ComparisionValue::Column(col_name) => {
            let idx = col_index_map[col_name];
            row.values[idx].clone()
        }
        ComparisionValue::I32(v) => Data::Int32(*v),
        ComparisionValue::I64(v) => Data::Int64(*v),
        ComparisionValue::F32(v) => Data::Float32(*v),
        ComparisionValue::F64(v) => Data::Float64(*v),
        ComparisionValue::String(v) => Data::String(v.clone()),
    }
}

/// Evaluate a single predicate against a row. Returns true if the row satisfies it.
fn evaluate_predicate(
    predicate: &Predicate,
    row: &Row,
    col_index_map: &HashMap<String, usize>,
) -> bool {
    // Get the left side: the column value from the row
    let left_idx = col_index_map[&predicate.column_name];
    let left = &row.values[left_idx];

    // Get the right side: resolve from literal or column reference
    let right = resolve_value(&predicate.value, row, col_index_map);

    // Compare using the operator
    match predicate.operator {
        ComparisionOperator::EQ => left == &right,
        ComparisionOperator::NE => left != &right,
        ComparisionOperator::GT => {
            left.partial_cmp(&right) == Some(Ordering::Greater)
        }
        ComparisionOperator::GTE => {
            matches!(left.partial_cmp(&right), Some(Ordering::Greater | Ordering::Equal))
        }
        ComparisionOperator::LT => {
            left.partial_cmp(&right) == Some(Ordering::Less)
        }
        ComparisionOperator::LTE => {
            matches!(left.partial_cmp(&right), Some(Ordering::Less | Ordering::Equal))
        }
    }
}

/// Evaluate ALL predicates against a row (AND logic). Returns true only if every predicate passes.
fn evaluate_all_predicates(
    predicates: &[Predicate],
    row: &Row,
    col_index_map: &HashMap<String, usize>,
) -> bool {
    predicates.iter().all(|p| evaluate_predicate(p, row, col_index_map))
}
