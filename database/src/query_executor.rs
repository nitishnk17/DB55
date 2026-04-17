use crate::buffer_pool::BufferPool;
use crate::cross::CrossOp;
use crate::filter::FilterOp;
use crate::hash_join::HashJoinOp;
use crate::operator::Operator;
use crate::project::ProjectOp;
use crate::sort::SortOp;
use crate::table_scanner::TableScanner;
use common::query::{ComparisionOperator, ComparisionValue, Predicate, QueryOp};
use common::DataType;
use db_config::statistics::{CardinalityData, ColumnStat};
use db_config::DbContext;
use std::io::{Read, Write};

pub fn build_operator<R: Read + 'static, W: Write + 'static>(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
) -> Box<dyn Operator<R, W>> {
    let required = get_all_used_columns(query_op);
    build_operator_internal(query_op, ctx, buffer_pool, sort_memory_bytes, required)
}

fn build_operator_internal<R: Read + 'static, W: Write + 'static>(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
    required: std::collections::HashSet<String>,
) -> Box<dyn Operator<R, W>> {
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
                &table_spec.column_specs,
                Some(&required),
            ))
        }

        QueryOp::Filter(filter_data) => {
            let mut all_filter_predicates: Vec<Predicate> = filter_data.predicates.iter().map(|p| clone_predicate(p)).collect();
            let mut innermost: &QueryOp = &*filter_data.underlying;
            while let QueryOp::Filter(inner_f) = innermost {
                all_filter_predicates.extend(inner_f.predicates.iter().map(|p| clone_predicate(p)));
                innermost = &*inner_f.underlying;
            }

            if let QueryOp::Cross(_) = innermost {
                let leaves = flatten_cross_ops(innermost);
                let leaf_schemas: Vec<Vec<String>> = leaves.iter().map(|leaf| schema_of(leaf, ctx)).collect();
                let mut remaining_preds: Vec<Predicate> = Vec::new();
                let mut scalar_preds: Vec<Vec<Predicate>> = (0..leaves.len()).map(|_| Vec::new()).collect();
                for p in &all_filter_predicates {
                    let col_a = &p.column_name;
                    let owner_a = leaf_schemas.iter().position(|s| s.contains(col_a));
                    let is_scalar = match &p.value {
                        ComparisionValue::Column(col_b) => {
                            match leaf_schemas.iter().position(|s| s.contains(col_b)) {
                                Some(o_b) => owner_a == Some(o_b),
                                None => false,
                            }
                        }
                        _ => owner_a.is_some(),
                    };
                    if is_scalar { scalar_preds[owner_a.unwrap()].push(clone_predicate(p)); }
                    else { remaining_preds.push(clone_predicate(p)); }
                }
                let mut join_branch_required = required.clone();
                for p in &remaining_preds {
                    join_branch_required.insert(p.column_name.clone());
                    if let ComparisionValue::Column(c) = &p.value { join_branch_required.insert(c.clone()); }
                }
                let start_idx = {
                    let scores: Vec<(usize, u64)> = leaf_schemas.iter().enumerate().map(|(i, schema)| {
                        let base = estimate_cardinality(schema, ctx).unwrap_or(u64::MAX);
                        let est = if !scalar_preds[i].is_empty() { base / 10 } else { base };
                        (i, est)
                    }).collect();
                    scores.iter().min_by_key(|&&(_, c)| c).map(|&(i, _)| i).unwrap_or(0)
                };
                let mut joined = vec![false; leaves.len()];
                joined[start_idx] = true;
                let start_leaf_op = build_leaf_with_filter(leaves[start_idx], &scalar_preds[start_idx], &join_branch_required, ctx, buffer_pool, sort_memory_bytes);
                let mut current_schema = start_leaf_op.schema();
                let mut current_op: Box<dyn Operator<R, W>> = start_leaf_op;
                while joined.iter().any(|&m| !m) {
                    let mut best_leaf: Option<(usize, usize, String, String)> = None;
                    let mut min_output_size = f64::MAX;
                    let left_card = estimate_cardinality(&current_schema, ctx).unwrap_or(1_000_000) as f64;
                    for leaf_idx in 0..leaves.len() {
                        if joined[leaf_idx] { continue; }
                        let new_schema = &leaf_schemas[leaf_idx];
                        for pred_idx in 0..remaining_preds.len() {
                            if !matches!(remaining_preds[pred_idx].operator, ComparisionOperator::EQ) { continue; }
                            let col_a = &remaining_preds[pred_idx].column_name;
                            let col_b = match &remaining_preds[pred_idx].value { ComparisionValue::Column(c) => c, _ => continue };
                            let a_cur = current_schema.contains(col_a);
                            let b_new = new_schema.contains(col_b);
                            let b_cur = current_schema.contains(col_b);
                            let a_new = new_schema.contains(col_a);
                            if (a_cur && b_new) || (b_cur && a_new) {
                                let (l_col, r_col) = if a_cur { (col_a, col_b) } else { (col_b, col_a) };
                                let right_card_base = estimate_cardinality(new_schema, ctx).unwrap_or(1_000_000) as f64;
                                let right_card = if !scalar_preds[leaf_idx].is_empty() { right_card_base / 10.0 } else { right_card_base };
                                let l_ndv = estimate_ndv(l_col, ctx).unwrap_or(100) as f64;
                                let r_ndv = estimate_ndv(r_col, ctx).unwrap_or(100) as f64;
                                let est_output = (left_card * right_card) / l_ndv.max(r_ndv).max(1.0);
                                if est_output < min_output_size { min_output_size = est_output; best_leaf = Some((leaf_idx, pred_idx, l_col.clone(), r_col.clone())); }
                            }
                        }
                    }
                    if let Some((leaf_idx, pred_idx, col_l, col_r)) = best_leaf {
                        let right_op = build_leaf_with_filter(leaves[leaf_idx], &scalar_preds[leaf_idx], &join_branch_required, ctx, buffer_pool, sort_memory_bytes);
                        let right_schema = right_op.schema();
                        let l_actual_schema = current_op.schema();
                        let l_idx = l_actual_schema.iter().position(|x| x == &col_l).unwrap();
                        let r_idx = right_schema.iter().position(|x| x == &col_r).unwrap();
                        let ldt = current_op.data_types();
                        let rdt = right_op.data_types();
                        current_op = if should_use_hash_join(&current_schema, &right_schema, &ldt, &rdt, sort_memory_bytes, ctx) {
                            buffer_pool.set_eviction_policy(crate::buffer_pool::EvictionPolicy::LRU);
                            let num_parts = adaptive_hash_partitions(&current_schema, &right_schema, &ldt, &rdt, sort_memory_bytes, ctx);
                            Box::new(HashJoinOp::new(current_op, right_op, l_idx, r_idx, ldt, rdt, buffer_pool, num_parts))
                        } else {
                            buffer_pool.set_eviction_policy(crate::buffer_pool::EvictionPolicy::LRU);
                            Box::new(crate::join::JoinOp::new(current_op, right_op, l_idx, r_idx, rdt, buffer_pool))
                        };
                        current_schema.extend(right_schema);
                        joined[leaf_idx] = true;
                        remaining_preds.remove(pred_idx);
                    } else {
                        let leaf_idx = joined.iter().position(|&m| !m).unwrap();
                        let right_op = build_leaf_with_filter(leaves[leaf_idx], &scalar_preds[leaf_idx], &join_branch_required, ctx, buffer_pool, sort_memory_bytes);
                        let right_schema = right_op.schema();
                        current_op = Box::new(CrossOp::new(current_op, right_op, buffer_pool));
                        current_schema.extend(right_schema);
                        joined[leaf_idx] = true;
                    }
                    let future_cols: std::collections::HashSet<String> = remaining_preds.iter().flat_map(|pred| {
                        let mut v = vec![pred.column_name.clone()];
                        if let ComparisionValue::Column(c) = &pred.value { v.push(c.clone()); }
                        v
                    }).collect();
                    let keep: Vec<(String, String)> = current_schema.iter().filter(|c| required.contains(*c) || future_cols.contains(*c)).map(|c| (c.clone(), c.clone())).collect();
                    if keep.len() < current_schema.len() {
                        let new_schema: Vec<String> = keep.iter().map(|(_, to)| to.clone()).collect();
                        current_op = Box::new(ProjectOp::new(current_op, keep));
                        current_schema = new_schema;
                    }
                }
                return if remaining_preds.is_empty() { current_op } else { Box::new(FilterOp::new(current_op, remaining_preds)) };
            }
            let mut child_req = required.clone();
            for p in &filter_data.predicates { child_req.insert(p.column_name.clone()); if let ComparisionValue::Column(c) = &p.value { child_req.insert(c.clone()); } }
            let child = build_operator_internal(&filter_data.underlying, ctx, buffer_pool, sort_memory_bytes, child_req);
            let preds = filter_data.predicates.iter().map(|p| clone_predicate(p)).collect();
            Box::new(FilterOp::new(child, preds))
        }

        QueryOp::Project(project_data) => {
            let mut child_req = std::collections::HashSet::new();
            for (from, _) in &project_data.column_name_map { child_req.insert(from.clone()); }
            let child = build_operator_internal(&project_data.underlying, ctx, buffer_pool, sort_memory_bytes, child_req);
            Box::new(ProjectOp::new(child, project_data.column_name_map.clone()))
        }

        QueryOp::Cross(cross_data) => {
            let left  = build_operator_internal(&cross_data.left,  ctx, buffer_pool, sort_memory_bytes, required.clone());
            let right = build_operator_internal(&cross_data.right, ctx, buffer_pool, sort_memory_bytes, required);
            Box::new(CrossOp::new(left, right, buffer_pool))
        }

        QueryOp::Sort(sort_data) => {
            let mut child_req = required.clone();
            for spec in &sort_data.sort_specs { child_req.insert(spec.column_name.clone()); }
            let child = build_operator_internal(&sort_data.underlying, ctx, buffer_pool, sort_memory_bytes, child_req);
            if let Some(child_order) = child.order() {
                let mut satisfied = true;
                if child_order.len() < sort_data.sort_specs.len() { satisfied = false; }
                else {
                    for (i, spec) in sort_data.sort_specs.iter().enumerate() {
                        if child_order[i].column_name != spec.column_name || child_order[i].ascending != spec.ascending { satisfied = false; break; }
                    }
                }
                if satisfied { eprintln!("Sort Avoidance: Data already ordered. Skipping SortOp."); return child; }
            }
            let current_schema = child.schema();
            let keep: Vec<(String, String)> = current_schema.iter().filter(|c| required.contains(*c) || sort_data.sort_specs.iter().any(|s| &s.column_name == *c)).map(|c| (c.clone(), c.clone())).collect();
            let child = if keep.len() < current_schema.len() { Box::new(ProjectOp::new(child, keep)) } else { child };
            let data_types = child.data_types();
            Box::new(SortOp::new(child, &sort_data.sort_specs, data_types, buffer_pool, sort_memory_bytes))
        }
    }
}

fn clone_predicate(p: &Predicate) -> Predicate {
    Predicate { column_name: p.column_name.clone(), operator: clone_cmp_op(&p.operator), value: clone_cmp_value(&p.value) }
}
fn clone_cmp_op(op: &ComparisionOperator) -> ComparisionOperator {
    match op { ComparisionOperator::EQ => ComparisionOperator::EQ, ComparisionOperator::NE => ComparisionOperator::NE, ComparisionOperator::GT => ComparisionOperator::GT, ComparisionOperator::GTE => ComparisionOperator::GTE, ComparisionOperator::LT => ComparisionOperator::LT, ComparisionOperator::LTE => ComparisionOperator::LTE }
}
fn clone_cmp_value(v: &ComparisionValue) -> ComparisionValue {
    match v { ComparisionValue::Column(s) => ComparisionValue::Column(s.clone()), ComparisionValue::I32(n) => ComparisionValue::I32(*n), ComparisionValue::I64(n) => ComparisionValue::I64(*n), ComparisionValue::F32(n) => ComparisionValue::F32(*n), ComparisionValue::F64(n) => ComparisionValue::F64(*n), ComparisionValue::String(s) => ComparisionValue::String(s.clone()) }
}

fn adaptive_hash_partitions(left_schema: &[String], right_schema: &[String], left_types: &[DataType], right_types: &[DataType], sort_memory_bytes: usize, ctx: &DbContext) -> usize {
    let left_row_bytes = row_bytes_for_types(left_types);
    let right_row_bytes = row_bytes_for_types(right_types);
    let max_row_bytes = left_row_bytes.max(right_row_bytes).max(32);
    let target_build_mem = (sort_memory_bytes / 2).max(8 * 1024 * 1024);
    let rows_per_part = (target_build_mem / max_row_bytes).max(1_000);
    let left_card = estimate_cardinality(left_schema, ctx).unwrap_or(1_000_000) as usize;
    let right_card = estimate_cardinality(right_schema, ctx).unwrap_or(1_000_000) as usize;
    let max_card = left_card.max(right_card);
    let raw = ((max_card + rows_per_part - 1) / rows_per_part).max(1);
    let p = raw.next_power_of_two();
    p.clamp(4, 2048)
}

fn row_bytes_for_types(types: &[DataType]) -> usize {
    types.iter().map(|dt| match dt { DataType::Int32 | DataType::Float32 => 4usize, DataType::Int64 | DataType::Float64 => 8usize, DataType::String => 104usize }).sum::<usize>().max(32)
}

fn estimate_ndv(col_name: &str, ctx: &DbContext) -> Option<u64> {
    let base_name = if let Some(dot) = col_name.find('.') { &col_name[dot + 1..] } else { col_name };
    for table in ctx.get_table_specs() {
        for cs in &table.column_specs {
            if cs.column_name == base_name || cs.column_name == col_name {
                let mut card = None;
                let mut density = None;
                if let Some(stats) = &cs.stats {
                    for stat in stats {
                        match stat {
                            ColumnStat::CardinalityStat(CardinalityData(c)) => card = Some(*c),
                            ColumnStat::DensityStat(db_config::statistics::Density(d)) => density = Some(*d),
                            _ => {}
                        }
                    }
                }
                if let (Some(c), Some(d)) = (card, density) { return Some((c as f32 * d) as u64); }
                else if let Some(c) = card { return Some(c / 10); }
            }
        }
    }
    None
}

fn should_use_hash_join(_left_schema: &[String], right_schema: &[String], _left_types: &[DataType], right_types: &[DataType], sort_memory_bytes: usize, ctx: &DbContext) -> bool {
    // JoinOp builds the *right* side entirely in memory and streams the left.
    // Therefore we must use HashJoin whenever the right side would exceed the
    // available memory budget — regardless of how small the left side appears.
    //
    // The old check used min(left_bytes, right_bytes) which was wrong: if
    // estimate_cardinality returns a tiny number for the accumulated join result
    // (e.g. 5 rows from a region scan), min_bytes becomes negligible and JoinOp
    // is chosen even when right = lineitem (6 M rows → OOM).
    let right_card = estimate_cardinality(right_schema, ctx).unwrap_or(1_000_000);
    let right_bytes = right_card as usize * row_bytes_for_types(right_types);
    // Use 85 % of sort budget as the threshold so there is room for the hash
    // table overhead on top of the raw row bytes.
    let mem_threshold = (sort_memory_bytes * 85) / 100;
    right_bytes > mem_threshold
}

fn get_all_used_columns(query: &QueryOp) -> std::collections::HashSet<String> {
    let mut cols = std::collections::HashSet::new();
    match query {
        QueryOp::Scan(_) => {}
        QueryOp::Filter(f) => {
            cols.extend(get_all_used_columns(&f.underlying));
            for p in &f.predicates { cols.insert(p.column_name.clone()); if let ComparisionValue::Column(c) = &p.value { cols.insert(c.clone()); } }
        }
        QueryOp::Project(p) => {
            cols.extend(get_all_used_columns(&p.underlying));
            for (old, _) in &p.column_name_map { cols.insert(old.clone()); }
        }
        QueryOp::Cross(c) => { cols.extend(get_all_used_columns(&c.left)); cols.extend(get_all_used_columns(&c.right)); }
        QueryOp::Sort(s) => { cols.extend(get_all_used_columns(&s.underlying)); for sort_spec in &s.sort_specs { cols.insert(sort_spec.column_name.clone()); } }
    }
    cols
}

fn build_leaf_with_filter<R: Read + 'static, W: Write + 'static>(leaf: &QueryOp, scalar_preds: &[Predicate], required: &std::collections::HashSet<String>, ctx: &DbContext, buffer_pool: &mut BufferPool<R, W>, sort_memory_bytes: usize) -> Box<dyn Operator<R, W>> {
    let mut local_required = required.clone();
    for p in scalar_preds {
        local_required.insert(p.column_name.clone());
        if let ComparisionValue::Column(c) = &p.value { local_required.insert(c.clone()); }
    }
    let mut op = build_operator_internal(leaf, ctx, buffer_pool, sort_memory_bytes, local_required);
    if !scalar_preds.is_empty() { let preds: Vec<Predicate> = scalar_preds.iter().map(|p| clone_predicate(p)).collect(); op = Box::new(FilterOp::new(op, preds)); }
    let current_schema = op.schema();
    let total_cols = current_schema.len();
    let needed_here: Vec<(String, String)> = current_schema.into_iter().filter(|c| required.contains(c)).map(|c| (c.clone(), c)).collect();
    if !needed_here.is_empty() && needed_here.len() < total_cols { op = Box::new(ProjectOp::new(op, needed_here)); }
    op
}

fn flatten_cross_ops(op: &QueryOp) -> Vec<&QueryOp> { match op { QueryOp::Cross(c) => { let mut v = flatten_cross_ops(&c.left); v.extend(flatten_cross_ops(&c.right)); v } other => vec![other] } }

fn schema_of(op: &QueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        QueryOp::Scan(scan) => { ctx.get_table_specs().iter().find(|t| t.name == scan.table_id).map(|t| t.column_specs.iter().map(|c| c.column_name.clone()).collect()).unwrap_or_default() }
        QueryOp::Filter(f) => schema_of(&f.underlying, ctx),
        QueryOp::Project(p) => p.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
        QueryOp::Cross(c) => { let mut s = schema_of(&c.left, ctx); s.extend(schema_of(&c.right, ctx)); s }
        QueryOp::Sort(s) => schema_of(&s.underlying, ctx),
    }
}

fn estimate_cardinality(schema: &[String], ctx: &DbContext) -> Option<u64> {
    for col_name in schema {
        let names_to_try: &[&str] = if let Some(dot) = col_name.find('.') { let base = &col_name[dot + 1..]; &[col_name.as_str(), base] } else { &[col_name.as_str()] };
        for &name in names_to_try { for table in ctx.get_table_specs() { for cs in &table.column_specs { if cs.column_name == name { if let Some(stats) = &cs.stats { for stat in stats { if let ColumnStat::CardinalityStat(CardinalityData(card)) = stat { return Some(*card); } } } } } } }
    }
    None
}
