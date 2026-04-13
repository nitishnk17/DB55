use crate::buffer_pool::BufferPool;
use crate::cross::CrossOp;
use crate::filter::FilterOp;
use crate::hash_join::HashJoinOp;
use crate::operator::Operator;
use crate::project::ProjectOp;
use crate::sort::SortOp;
use crate::table_scanner::TableScanner;
use common::query::QueryOp;
use db_config::statistics::{CardinalityData, ColumnStat};
use db_config::table::ColumnSpec;
use db_config::DbContext;
use std::io::{Read, Write};

/// Recursively build a physical operator tree from a logical `QueryOp` AST.
///
/// `sort_memory_bytes` is the byte budget available to each SortOp for its
/// in-memory run-generation phase.  Pass ~50 % of the process memory limit.
pub fn build_operator<R: Read + 'static, W: Write + 'static>(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
) -> Box<dyn Operator<R, W>> {
    let global_needed = get_all_used_columns(query_op);
    build_operator_internal(query_op, ctx, buffer_pool, sort_memory_bytes, &global_needed)
}

fn build_operator_internal<R: Read + 'static, W: Write + 'static>(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
    global_needed: &std::collections::HashSet<String>,
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
                table_spec.column_specs.clone(),
            ))
        }

        QueryOp::Filter(filter_data) => {
            // ── Multi-table join rewrite ──────────────────────────────────────
            // A Filter (possibly stacked with other Filters) wrapping a Cross
            // (possibly nested) is a multi-way join.
            // First, collect ALL predicates from consecutive Filter nodes above
            // the Cross, so that join predicates in outer Filters are not missed.
            let mut all_filter_predicates = filter_data.predicates.clone();
            let mut innermost: &QueryOp = &*filter_data.underlying;
            while let QueryOp::Filter(inner_f) = innermost {
                all_filter_predicates.extend(inner_f.predicates.clone());
                innermost = &*inner_f.underlying;
            }

            // Now `innermost` is the first non-Filter node beneath the stack.
            if let QueryOp::Cross(_) = innermost {
                let leaves = flatten_cross_ops(innermost);
                let leaf_schemas: Vec<Vec<String>> = leaves.iter()
                    .map(|leaf| schema_of(leaf, ctx))
                    .collect();

                // ── Filter pushdown ───────────────────────────────────────────
                // Partition predicates into:
                //   scalar_preds[i] = predicates that only reference columns from leaf i
                //   remaining_preds  = join predicates + multi-table predicates
                let mut remaining_preds: Vec<common::query::Predicate> = Vec::new();
                let mut scalar_preds: Vec<Vec<common::query::Predicate>> =
                    (0..leaves.len()).map(|_| Vec::new()).collect();

                for p in &all_filter_predicates {
                    // A scalar pred: column_name in exactly one leaf AND value is not Column
                    // (or both sides in the same leaf)
                    let col_a = &p.column_name;
                    let owner_a = leaf_schemas.iter().position(|s| s.contains(col_a));
                    let is_scalar = match &p.value {
                        common::query::ComparisionValue::Column(col_b) => {
                            // Both columns in same leaf → scalar (intra-table)
                            match leaf_schemas.iter().position(|s| s.contains(col_b)) {
                                Some(o_b) => owner_a == Some(o_b),
                                None => false,
                            }
                        }
                        _ => owner_a.is_some(), // literal comparison → scalar for that leaf
                    };
                    if is_scalar {
                        scalar_preds[owner_a.unwrap()].push(p.clone());
                    } else {
                        remaining_preds.push(p.clone());
                    }
                }

                // ── Cardinality estimate after scalar pushdown ────────────────
                // Prefer to start with the leaf that has scalar filters (more selective).
                // Fall back to raw cardinality from stats.
                let start_idx = {
                    // Score: leaves with scalar filters score lower (preferred start)
                    let scores: Vec<(usize, u64)> = leaf_schemas.iter().enumerate().map(|(i, schema)| {
                        let base = estimate_cardinality(schema, ctx).unwrap_or(u64::MAX);
                        let has_scalar = !scalar_preds[i].is_empty();
                        // If it has scalar filters, assume 10x reduction
                        let est = if has_scalar { base / 10 } else { base };
                        (i, est)
                    }).collect();
                    scores.iter().min_by_key(|&&(_, c)| c).map(|&(i, _)| i).unwrap_or(0)
                };

                let mut joined = vec![false; leaves.len()];
                joined[start_idx] = true;
                // Build the starting leaf with its pushed-down scalar predicates
                let start_leaf_op = build_leaf_with_filter(
                    leaves[start_idx], &scalar_preds[start_idx], &global_needed, ctx, buffer_pool, sort_memory_bytes);
                let mut current_op = start_leaf_op;
                let mut current_schema = leaf_schemas[start_idx].clone();

                while joined.iter().any(|&m| !m) {
                    // Find next leaf connected to current result by an EQ join pred
                    let mut found = false;
                    'search: for leaf_idx in 0..leaves.len() {
                        if joined[leaf_idx] { continue; }
                        let new_schema = &leaf_schemas[leaf_idx];
                        for pred_idx in 0..remaining_preds.len() {
                            if let common::query::ComparisionOperator::EQ = remaining_preds[pred_idx].operator {
                                if let common::query::ComparisionValue::Column(col_b) = &remaining_preds[pred_idx].value.clone() {
                                    let col_a = remaining_preds[pred_idx].column_name.clone();
                                    let a_cur = current_schema.contains(&col_a);
                                    let b_new = new_schema.contains(col_b);
                                    let b_cur = current_schema.contains(col_b);
                                    let a_new = new_schema.contains(&col_a);
                                    if (a_cur && b_new) || (b_cur && a_new) {
                                        // Build this leaf with its pushed-down scalar preds
                                        let right_op = build_leaf_with_filter(
                                            leaves[leaf_idx], &scalar_preds[leaf_idx], &global_needed,
                                            ctx, buffer_pool, sort_memory_bytes);
                                        let right_schema = right_op.schema();
                                        let l_actual_schema = current_op.schema();
                                        let (l_idx, r_idx) = if a_cur && b_new {
                                            (l_actual_schema.iter().position(|x| x == &col_a).unwrap(),
                                             right_schema.iter().position(|x| x == col_b).unwrap())
                                        } else {
                                            (l_actual_schema.iter().position(|x| x == col_b).unwrap(),
                                             right_schema.iter().position(|x| x == &col_a).unwrap())
                                        };
                                        // Use column_specs() from the operators directly — avoids
                                        // the fragile reverse-lookup in resolve_column_specs which
                                        // could silently default to String for unrecognized names.
                                        let lcs = current_op.column_specs();
                                        let rcs = right_op.column_specs();

                                        current_op = if should_use_hash_join(&current_schema, &right_schema, ctx) {
                                            eprintln!("Join strategy: Grace Hash Join (Setting LRU cache)");
                                            buffer_pool.set_eviction_policy(crate::buffer_pool::EvictionPolicy::LRU);
                                            Box::new(HashJoinOp::new(current_op, right_op, l_idx, r_idx, lcs, rcs, buffer_pool))
                                        } else {
                                            // Implement adaptive dynamic eviction: nested loops benefit immensely from MRU
                                            // to prevent sequential flooding of the outer loop over inner data
                                            eprintln!("Join strategy: Block Nested Loop Join (Setting MRU cache)");
                                            buffer_pool.set_eviction_policy(crate::buffer_pool::EvictionPolicy::MRU);
                                            Box::new(crate::join::JoinOp::new(current_op, right_op, l_idx, r_idx, rcs, buffer_pool))
                                        };
                                        current_schema.extend(right_schema);
                                        joined[leaf_idx] = true;
                                        remaining_preds.remove(pred_idx);
                                        found = true;
                                        break 'search;
                                    }
                                }
                            }
                        }
                    }
                    if !found {
                        // No join predicate found: do a cross product with next unjoined leaf
                        let leaf_idx = joined.iter().position(|&m| !m).unwrap();
                        let right_op = build_leaf_with_filter(
                            leaves[leaf_idx], &scalar_preds[leaf_idx], &global_needed,
                            ctx, buffer_pool, sort_memory_bytes);
                        let right_schema = right_op.schema();
                        current_op = Box::new(CrossOp::new(current_op, right_op, buffer_pool));
                        current_schema.extend(right_schema);
                        joined[leaf_idx] = true;
                    }
                }

                return if remaining_preds.is_empty() {
                    current_op
                } else {
                    Box::new(FilterOp::new(current_op, remaining_preds))
                };
            }

            // Standard filter (no join rewrite)
            let child = build_operator_internal(&filter_data.underlying, ctx, buffer_pool, sort_memory_bytes, global_needed);
            Box::new(FilterOp::new(child, filter_data.predicates.clone()))
        }

        QueryOp::Project(project_data) => {
            let child = build_operator_internal(&project_data.underlying, ctx, buffer_pool, sort_memory_bytes, global_needed);
            Box::new(ProjectOp::new(child, project_data.column_name_map.clone()))
        }

        QueryOp::Cross(cross_data) => {
            let left  = build_operator_internal(&cross_data.left,  ctx, buffer_pool, sort_memory_bytes, global_needed);
            let right = build_operator_internal(&cross_data.right, ctx, buffer_pool, sort_memory_bytes, global_needed);
            Box::new(CrossOp::new(left, right, buffer_pool))
        }

        QueryOp::Sort(sort_data) => {
            let child = build_operator_internal(&sort_data.underlying, ctx, buffer_pool, sort_memory_bytes, global_needed);
            let column_specs  = child.column_specs();
            Box::new(SortOp::new(
                child,
                sort_data.sort_specs.clone(),
                column_specs,
                buffer_pool,
                sort_memory_bytes,   // ← pass the real budget
            ))
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Look up the ColumnSpec (DataType + stats) for each column name in the schema
/// by scanning all tables in the context.
///
/// For aliased columns like "l1.l_orderkey", we first try the full name, then
/// strip the alias prefix (everything before the first '.') and try the base name.
fn resolve_column_specs(schema: &[String], ctx: &DbContext) -> Vec<ColumnSpec> {
    schema
        .iter()
        .map(|col_name| {
            // Try exact match first
            for table in ctx.get_table_specs() {
                for cs in &table.column_specs {
                    if cs.column_name == *col_name {
                        return cs.clone();
                    }
                }
            }
            // Try stripping alias prefix (e.g. "l1.l_orderkey" → "l_orderkey")
            if let Some(dot_pos) = col_name.find('.') {
                let base_name = &col_name[dot_pos + 1..];
                for table in ctx.get_table_specs() {
                    for cs in &table.column_specs {
                        if cs.column_name == base_name {
                            return ColumnSpec {
                                column_name: col_name.clone(),
                                data_type: cs.data_type.clone(),
                                stats: cs.stats.clone(),
                            };
                        }
                    }
                }
            }
            // Fallback: create a synthetic String spec so the code doesn't panic on
            // computed / renamed columns that don't appear in any base table.
            eprintln!("Warning: column '{}' not found in any table spec; defaulting to String", col_name);
            ColumnSpec {
                column_name: col_name.clone(),
                data_type: common::DataType::String,
                stats: None,
            }
        })
        .collect()
}

/// Decide between Grace Hash Join and BNLJ based on cardinality statistics.
///
/// Rules:
///   • If either side has cardinality > threshold → Hash Join (handles large tables)
///   • If both sides are tiny (≤ threshold)       → BNLJ   (lower constant factor)
///   • If stats are unavailable                   → Hash Join (safe default)
fn should_use_hash_join(left_schema: &[String], right_schema: &[String], ctx: &DbContext) -> bool {
    let threshold = 1_000u64;

    let left_card  = estimate_cardinality(left_schema, ctx);
    let right_card = estimate_cardinality(right_schema, ctx);

    match (left_card, right_card) {
        (Some(l), Some(r)) => {
            eprintln!("Join heuristic: left_card={}, right_card={}, threshold={}", l, r, threshold);
            l > threshold || r > threshold
        }
        _ => {
            eprintln!("Join heuristic: no cardinality stats, defaulting to Hash Join");
            true
        }
    }
}

/// Recursively scan AST to extract every column name that upper levels depend on
fn get_all_used_columns(query: &QueryOp) -> std::collections::HashSet<String> {
    let mut cols = std::collections::HashSet::new();
    match query {
        QueryOp::Scan(_) => {}
        QueryOp::Filter(f) => {
            cols.extend(get_all_used_columns(&f.underlying));
            for p in &f.predicates {
                cols.insert(p.column_name.clone());
                if let common::query::ComparisionValue::Column(c) = &p.value {
                    cols.insert(c.clone());
                }
            }
        }
        QueryOp::Project(p) => {
            cols.extend(get_all_used_columns(&p.underlying));
            for (old, _) in &p.column_name_map {
                cols.insert(old.clone());
            }
        }
        QueryOp::Cross(c) => {
            cols.extend(get_all_used_columns(&c.left));
            cols.extend(get_all_used_columns(&c.right));
        }
        QueryOp::Sort(s) => {
            cols.extend(get_all_used_columns(&s.underlying));
            for sort_spec in &s.sort_specs {
                cols.insert(sort_spec.column_name.clone());
            }
        }
    }
    cols
}


/// Build a leaf operator, wrapping it in a FilterOp if there are pushed-down predicates, 
/// and finally a ProjectOp limiting it to the globally required columns.
fn build_leaf_with_filter<R: Read + 'static, W: Write + 'static>(
    leaf: &QueryOp,
    scalar_preds: &[common::query::Predicate],
    global_needed: &std::collections::HashSet<String>,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
) -> Box<dyn Operator<R, W>> {
    let mut op = build_operator_internal(leaf, ctx, buffer_pool, sort_memory_bytes, global_needed);
    
    if !scalar_preds.is_empty() {
        op = Box::new(FilterOp::new(op, scalar_preds.to_vec()));
    }

    let current_schema = op.schema();
    
    // Project pushdown: Keep only columns requested globally that are present in the current schema
    let needed_here: Vec<(String, String)> = current_schema
        .into_iter()
        .filter(|c| global_needed.contains(c))
        .map(|c| (c.clone(), c))
        .collect();
        
    // Only wrap with ProjectOp if it actually restricts columns
    if !needed_here.is_empty() && needed_here.len() < op.schema().len() {
        op = Box::new(ProjectOp::new(op, needed_here));
    }

    op
}

/// Flatten a nested Cross tree into a list of leaf QueryOps.
fn flatten_cross_ops(op: &QueryOp) -> Vec<&QueryOp> {
    match op {
        QueryOp::Cross(c) => {
            let mut v = flatten_cross_ops(&c.left);
            v.extend(flatten_cross_ops(&c.right));
            v
        }
        other => vec![other],
    }
}

/// Compute the output schema of a QueryOp without building the actual operator.
/// Used to verify join predicate column placement before building children.
fn schema_of(op: &QueryOp, ctx: &DbContext) -> Vec<String> {
    match op {
        QueryOp::Scan(scan) => {
            ctx.get_table_specs()
                .iter()
                .find(|t| t.name == scan.table_id)
                .map(|t| t.column_specs.iter().map(|c| c.column_name.clone()).collect())
                .unwrap_or_default()
        }
        QueryOp::Filter(f) => schema_of(&f.underlying, ctx),
        QueryOp::Project(p) => p.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
        QueryOp::Cross(c) => {
            let mut s = schema_of(&c.left, ctx);
            s.extend(schema_of(&c.right, ctx));
            s
        }
        QueryOp::Sort(s) => schema_of(&s.underlying, ctx),
    }
}

/// Return the first CardinailtyData value found for any column in the schema.
/// Handles aliased columns like "l1.l_orderkey" by stripping the alias prefix.
fn estimate_cardinality(schema: &[String], ctx: &DbContext) -> Option<u64> {
    for col_name in schema {
        // Try exact match, then alias-stripped match
        let names_to_try: &[&str] = if let Some(dot) = col_name.find('.') {
            let base = &col_name[dot + 1..];
            &[col_name.as_str(), base]
        } else {
            &[col_name.as_str()]
        };
        for &name in names_to_try {
            for table in ctx.get_table_specs() {
                for cs in &table.column_specs {
                    if cs.column_name == name {
                        if let Some(stats) = &cs.stats {
                            for stat in stats {
                                if let ColumnStat::CardinalityStat(CardinalityData(card)) = stat {
                                    return Some(*card);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}
