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
                &table_spec.column_specs,
            ))
        }

        QueryOp::Filter(filter_data) => {
            // ── Multi-table join rewrite ──────────────────────────────────────
            // A Filter (possibly stacked with other Filters) wrapping a Cross
            // (possibly nested) is a multi-way join.
            // First, collect ALL predicates from consecutive Filter nodes above
            // the Cross, so that join predicates in outer Filters are not missed.
            let mut all_filter_predicates: Vec<Predicate> = filter_data
                .predicates
                .iter()
                .map(|p| clone_predicate(p))
                .collect();
            let mut innermost: &QueryOp = &*filter_data.underlying;
            while let QueryOp::Filter(inner_f) = innermost {
                all_filter_predicates.extend(inner_f.predicates.iter().map(|p| clone_predicate(p)));
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
                let mut remaining_preds: Vec<Predicate> = Vec::new();
                let mut scalar_preds: Vec<Vec<Predicate>> =
                    (0..leaves.len()).map(|_| Vec::new()).collect();

                for p in &all_filter_predicates {
                    // A scalar pred: column_name in exactly one leaf AND value is not Column
                    // (or both sides in the same leaf)
                    let col_a = &p.column_name;
                    let owner_a = leaf_schemas.iter().position(|s| s.contains(col_a));
                    let is_scalar = match &p.value {
                        ComparisionValue::Column(col_b) => {
                            // Both columns in same leaf → scalar (intra-table)
                            match leaf_schemas.iter().position(|s| s.contains(col_b)) {
                                Some(o_b) => owner_a == Some(o_b),
                                None => false,
                            }
                        }
                        _ => owner_a.is_some(), // literal comparison → scalar for that leaf
                    };
                    if is_scalar {
                        scalar_preds[owner_a.unwrap()].push(clone_predicate(p));
                    } else {
                        remaining_preds.push(clone_predicate(p));
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
                            let is_eq = matches!(remaining_preds[pred_idx].operator, ComparisionOperator::EQ);
                            if !is_eq { continue; }
                            // Extract column names without borrowing remaining_preds across remove()
                            let col_a = remaining_preds[pred_idx].column_name.clone();
                            let col_b_opt = match &remaining_preds[pred_idx].value {
                                ComparisionValue::Column(c) => Some(c.clone()),
                                _ => None,
                            };
                            if let Some(col_b) = col_b_opt {
                                let a_cur = current_schema.contains(&col_a);
                                let b_new = new_schema.contains(&col_b);
                                let b_cur = current_schema.contains(&col_b);
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
                                         right_schema.iter().position(|x| x == &col_b).unwrap())
                                    } else {
                                        (l_actual_schema.iter().position(|x| x == &col_b).unwrap(),
                                         right_schema.iter().position(|x| x == &col_a).unwrap())
                                    };
                                    // Use data_types() from the operators directly
                                    let ldt = current_op.data_types();
                                    let rdt = right_op.data_types();

                                    current_op = if should_use_hash_join(&current_schema, &right_schema, ctx) {
                                        eprintln!("Join strategy: Grace Hash Join (Setting LRU cache)");
                                        buffer_pool.set_eviction_policy(crate::buffer_pool::EvictionPolicy::LRU);
                                        Box::new(HashJoinOp::new(current_op, right_op, l_idx, r_idx, ldt, rdt, buffer_pool))
                                    } else {
                                        eprintln!("Join strategy: Block Nested Loop Join (Setting MRU cache)");
                                        buffer_pool.set_eviction_policy(crate::buffer_pool::EvictionPolicy::MRU);
                                        Box::new(crate::join::JoinOp::new(current_op, right_op, l_idx, r_idx, rdt, buffer_pool))
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
            let preds = filter_data.predicates.iter().map(|p| clone_predicate(p)).collect();
            Box::new(FilterOp::new(child, preds))
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
            let data_types = child.data_types();
            Box::new(SortOp::new(
                child,
                &sort_data.sort_specs,
                data_types,
                buffer_pool,
                sort_memory_bytes,
            ))
        }
    }
}

// ─── Manual Clone Helpers ───────────────────────────────────────────────────
// These exist because the common/ crate types (Predicate, ComparisionOperator,
// ComparisionValue) don't derive Clone.  We reconstruct them field-by-field
// using only types that DO have Clone (String, i32, f32, etc.).

fn clone_predicate(p: &Predicate) -> Predicate {
    Predicate {
        column_name: p.column_name.clone(),
        operator: clone_cmp_op(&p.operator),
        value: clone_cmp_value(&p.value),
    }
}

fn clone_cmp_op(op: &ComparisionOperator) -> ComparisionOperator {
    match op {
        ComparisionOperator::EQ  => ComparisionOperator::EQ,
        ComparisionOperator::NE  => ComparisionOperator::NE,
        ComparisionOperator::GT  => ComparisionOperator::GT,
        ComparisionOperator::GTE => ComparisionOperator::GTE,
        ComparisionOperator::LT  => ComparisionOperator::LT,
        ComparisionOperator::LTE => ComparisionOperator::LTE,
    }
}

fn clone_cmp_value(v: &ComparisionValue) -> ComparisionValue {
    match v {
        ComparisionValue::Column(s) => ComparisionValue::Column(s.clone()),
        ComparisionValue::I32(n)    => ComparisionValue::I32(*n),
        ComparisionValue::I64(n)    => ComparisionValue::I64(*n),
        ComparisionValue::F32(n)    => ComparisionValue::F32(*n),
        ComparisionValue::F64(n)    => ComparisionValue::F64(*n),
        ComparisionValue::String(s) => ComparisionValue::String(s.clone()),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Decide between Grace Hash Join and BNLJ based on cardinality statistics.
///
/// Rules:
///   • If either side has cardinality > threshold → Hash Join (handles large tables)
///   • If both sides are tiny (≤ threshold)       → BNLJ   (lower constant factor)
///   • If stats are unavailable                   → Hash Join (safe default)
fn should_use_hash_join(_left_schema: &[String], _right_schema: &[String], _ctx: &DbContext) -> bool {
    // ALWAYS return true.
    // The pure in-memory BNLJ is too risky with the 64MB RLIMIT_AS limit 
    // because statistical cardinality estimates can be wildly inaccurate. 
    // Grace Hash Join safely spills to disk and handles relations of any size.
    true
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
                if let ComparisionValue::Column(c) = &p.value {
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
    scalar_preds: &[Predicate],
    global_needed: &std::collections::HashSet<String>,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
) -> Box<dyn Operator<R, W>> {
    let mut op = build_operator_internal(leaf, ctx, buffer_pool, sort_memory_bytes, global_needed);

    if !scalar_preds.is_empty() {
        let preds: Vec<Predicate> = scalar_preds.iter().map(|p| clone_predicate(p)).collect();
        op = Box::new(FilterOp::new(op, preds));
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

/// Look up the DataType for a column name by scanning all tables in the context.
/// Used as a fallback when we need type info from the catalog.
#[allow(dead_code)]
fn resolve_data_type(col_name: &str, ctx: &DbContext) -> DataType {
    // Try exact match first
    for table in ctx.get_table_specs() {
        for cs in &table.column_specs {
            if cs.column_name == col_name {
                return cs.data_type.clone();
            }
        }
    }
    // Try stripping alias prefix (e.g. "l1.l_orderkey" → "l_orderkey")
    if let Some(dot_pos) = col_name.find('.') {
        let base_name = &col_name[dot_pos + 1..];
        for table in ctx.get_table_specs() {
            for cs in &table.column_specs {
                if cs.column_name == base_name {
                    return cs.data_type.clone();
                }
            }
        }
    }
    // Fallback: default to String
    eprintln!("Warning: column '{}' not found in any table spec; defaulting to String", col_name);
    DataType::String
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
