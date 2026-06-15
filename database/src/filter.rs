use crate::buffer_pool::BufferPool;
use crate::operator::Operator;
use crate::row::Row;
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use common::{Data, DataType};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{Read, Write};

pub struct FilterOp<R: Read, W: Write> {
    child: Box<dyn Operator<R, W>>,
    predicates: Vec<CompiledPredicate>,
}

enum RightValue {
    Column(usize),
    Literal(Data),
}

struct CompiledPredicate {
    left_idx: usize,
    operator: ComparisionOperator,
    right: RightValue,
}

impl<R: Read, W: Write> FilterOp<R, W> {
    pub fn new(child: Box<dyn Operator<R, W>>, predicates: Vec<Predicate>) -> Self {
        // Build column name -> index mapping from the child's schema.
        let col_index_map: HashMap<String, usize> = child
            .schema()
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        // Compile predicates once to avoid per-row hash lookups and value cloning.
        let predicates = predicates
            .into_iter()
            .map(|p| {
                let left_idx = *col_index_map
                    .get(&p.column_name)
                    .expect("Filter column not found in child schema");
                let right = match p.value {
                    ComparisionValue::Column(col_name) => {
                        let idx = *col_index_map
                            .get(&col_name)
                            .expect("Filter RHS column not found in child schema");
                        RightValue::Column(idx)
                    }
                    ComparisionValue::I32(v) => RightValue::Literal(Data::Int32(v)),
                    ComparisionValue::I64(v) => RightValue::Literal(Data::Int64(v)),
                    ComparisionValue::F32(v) => RightValue::Literal(Data::Float32(v)),
                    ComparisionValue::F64(v) => RightValue::Literal(Data::Float64(v)),
                    ComparisionValue::String(v) => RightValue::Literal(Data::String(v)),
                };

                CompiledPredicate {
                    left_idx,
                    operator: clone_cmp_op(&p.operator),
                    right,
                }
            })
            .collect();

        FilterOp { child, predicates }
    }

    #[inline]
    fn row_passes(&self, row: &Row) -> bool {
        for predicate in &self.predicates {
            let left = &row.values[predicate.left_idx];
            let right = match &predicate.right {
                RightValue::Column(idx) => &row.values[*idx],
                RightValue::Literal(v) => v,
            };

            let passed = match predicate.operator {
                ComparisionOperator::EQ => left == right,
                ComparisionOperator::NE => left != right,
                ComparisionOperator::GT => left.partial_cmp(right) == Some(Ordering::Greater),
                ComparisionOperator::GTE => {
                    matches!(
                        left.partial_cmp(right),
                        Some(Ordering::Greater | Ordering::Equal)
                    )
                }
                ComparisionOperator::LT => left.partial_cmp(right) == Some(Ordering::Less),
                ComparisionOperator::LTE => {
                    matches!(
                        left.partial_cmp(right),
                        Some(Ordering::Less | Ordering::Equal)
                    )
                }
            };

            if !passed {
                return false;
            }
        }
        true
    }
}

impl<R: Read, W: Write> Operator<R, W> for FilterOp<R, W> {
    fn next(&mut self, pool: &mut BufferPool<R, W>) -> Option<Row> {
        // Keep pulling rows from child until one passes all predicates.
        while let Some(row) = self.child.next(pool) {
            if self.row_passes(&row) {
                return Some(row);
            }
        }
        None
    }

    fn schema(&self) -> Vec<String> {
        // Filter doesn't change the schema — same columns in, same columns out
        self.child.schema()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.child.data_types()
    }
}

fn clone_cmp_op(op: &ComparisionOperator) -> ComparisionOperator {
    match op {
        ComparisionOperator::EQ => ComparisionOperator::EQ,
        ComparisionOperator::NE => ComparisionOperator::NE,
        ComparisionOperator::GT => ComparisionOperator::GT,
        ComparisionOperator::GTE => ComparisionOperator::GTE,
        ComparisionOperator::LT => ComparisionOperator::LT,
        ComparisionOperator::LTE => ComparisionOperator::LTE,
    }
}

/// Evaluate a single predicate against a row. Returns true if the row satisfies it.
fn evaluate_predicate(predicate: &CompiledPredicate, row: &Row) -> bool {
    let left = &row.values[predicate.left_idx];
    let right = match &predicate.right {
        RightValue::Column(idx) => &row.values[*idx],
        RightValue::Literal(v) => v,
    };

    match predicate.operator {
        ComparisionOperator::EQ => left == right,
        ComparisionOperator::NE => left != right,
        ComparisionOperator::GT => left.partial_cmp(right) == Some(Ordering::Greater),
        ComparisionOperator::GTE => {
            matches!(
                left.partial_cmp(right),
                Some(Ordering::Greater | Ordering::Equal)
            )
        }
        ComparisionOperator::LT => left.partial_cmp(right) == Some(Ordering::Less),
        ComparisionOperator::LTE => {
            matches!(
                left.partial_cmp(right),
                Some(Ordering::Less | Ordering::Equal)
            )
        }
    }
}

/// Evaluate ALL predicates against a row (AND logic). Returns true only if every predicate passes.
fn evaluate_all_predicates(predicates: &[CompiledPredicate], row: &Row) -> bool {
    predicates.iter().all(|p| evaluate_predicate(p, row))
}
