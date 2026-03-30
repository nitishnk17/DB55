use common::query::QueryOp;
use crate::operator::Operator;
use crate::table_scanner::TableScanner;
use crate::disk_manager::DiskManager;
use db_config::DbContext;
use std::io::{Read, Write};

pub fn build_operator(
    query_op: &QueryOp,
    ctx: &DbContext,
    disk_manager: &mut DiskManager<impl Read, impl Write>,
) -> Box<dyn Operator> {
    match query_op {
        QueryOp::Scan(scan_data) => {
            // Look up the table spec by table_id
            let table_spec = ctx.get_table_specs().iter()
                .find(|t| t.name == scan_data.table_id)
                .expect(&format!("Table '{}' not found", scan_data.table_id));
            Box::new(TableScanner::new(
                disk_manager,
                &table_spec.file_id,
                table_spec.column_specs.clone(),
            ))
        }
        // We'll add Filter, Project, Cross, Sort in later days
        _ => panic!("Operator not yet implemented"),
    }
}
