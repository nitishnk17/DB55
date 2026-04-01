use crate::buffer_pool::BufferPool;
use crate::cross::CrossOp;
use crate::filter::FilterOp;
use crate::operator::Operator;
use crate::project::ProjectOp;
use crate::table_scanner::TableScanner;
use common::query::QueryOp;
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
                .expect(&format!("Table '{}' not found", scan_data.table_id));
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
        _ => panic!("Operator not yet implemented"),
    }
}
