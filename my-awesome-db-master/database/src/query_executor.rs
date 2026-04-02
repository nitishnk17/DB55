use crate::buffer_pool::BufferPool;
use crate::cross::CrossOp;
use crate::filter::FilterOp;
use crate::operator::Operator;
use crate::project::ProjectOp;
use crate::sort::SortOp;
use crate::table_scanner::TableScanner;
use common::query::QueryOp;
use db_config::table::ColumnSpec;
use db_config::DbContext;
use std::io::{Read, Write};

pub fn build_operator(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<impl Read, impl Write>,
) -> Box<dyn Operator> {
    match query_op {
        QueryOp::Scan(scan_data) => {
            let table_spec = ctx
                .get_table_specs()
                .iter()
                .find(|t| t.name == scan_data.table_id)
                .unwrap_or_else(|| panic!("Table '{}' not found", scan_data.table_id));
            Box::new(TableScanner::new(
                buffer_pool,
                &table_spec.file_id,
                table_spec.column_specs.clone(),
            ))
        }
        QueryOp::Filter(filter_data) => {
            // Recursively build the child operator first
            let child = build_operator(&filter_data.underlying, ctx, buffer_pool);
            // Wrap it with FilterOp
            Box::new(FilterOp::new(child, filter_data.predicates.clone()))
        }
        QueryOp::Project(project_data) => {
            // 1. Recursively build the child operator first
            let child = build_operator(&project_data.underlying, ctx, buffer_pool);
            // 2. Wrap it with ProjectOp
            Box::new(ProjectOp::new(child, project_data.column_name_map.clone()))
        }
        QueryOp::Cross(cross_data) => {
            // Build BOTH children recursively
            let left = build_operator(&cross_data.left, ctx, buffer_pool);
            let right = build_operator(&cross_data.right, ctx, buffer_pool);
            Box::new(CrossOp::new(left, right))
        }
        QueryOp::Sort(sort_data) => {
            let child = build_operator(&sort_data.underlying, ctx, buffer_pool);
            let child_schema = child.schema();
            let column_specs = resolve_column_specs(&child_schema, ctx);
            Box::new(SortOp::new(
                child,
                sort_data.sort_specs.clone(),
                column_specs,
                buffer_pool,
            ))
        }
    }
}

/// Look up ColumnSpec (with DataType) for each column name by searching all tables.
fn resolve_column_specs(schema: &[String], ctx: &DbContext) -> Vec<ColumnSpec> {
    schema
        .iter()
        .map(|col_name| {
            for table in ctx.get_table_specs() {
                for cs in &table.column_specs {
                    if cs.column_name == *col_name {
                        return cs.clone();
                    }
                }
            }
            panic!("Column '{}' not found in any table", col_name);
        })
        .collect()
}
