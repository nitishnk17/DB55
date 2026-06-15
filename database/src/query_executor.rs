use crate::buffer_pool::BufferPool;
use crate::cross::CrossOp;
use crate::filter::FilterOp;
use crate::hash_join::{HashJoinOp, JoinFilter};
use crate::operator::Operator;
use crate::project::ProjectOp;
use crate::sort::SortOp;
use crate::sort_merge::SortMergeJoinOp;
use crate::table_scanner::TableScanner;
use common::DataType;
use common::query::{ComparisionOperator, ComparisionValue, Predicate, QueryOp, SortSpec};
use db_config::DbContext;
use db_config::statistics::{CardinalityData, ColumnStat};
use std::io::{Read, Write};

struct PhysicalPlan<R: Read, W: Write> {
    op: Box<dyn Operator<R, W>>,
    ordering: Option<Vec<(String, bool)>>,
}

impl<R: Read, W: Write> PhysicalPlan<R, W> {
    fn new(op: Box<dyn Operator<R, W>>, ordering: Option<Vec<(String, bool)>>) -> Self {
        PhysicalPlan { op, ordering }
    }
}

/// Recursively build a physical operator tree from a logical `QueryOp` AST.
///
/// `sort_memory_bytes`  – byte budget for each SortOp's run-generation phase.
/// `hash_join_budget`   – byte budget for each HashJoinOp's in-memory buffer.
/// These two budgets are **non-overlapping** so they can coexist safely.
pub fn build_operator<R: Read + 'static, W: Write + 'static>(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
    hash_join_budget: usize,
) -> Box<dyn Operator<R, W>> {
    let global_needed = get_all_used_columns(query_op);
    let needed_above = std::collections::HashSet::<String>::new();
    build_operator_internal(
        query_op,
        ctx,
        buffer_pool,
        sort_memory_bytes,
        hash_join_budget,
        &global_needed,
        &needed_above,
        Vec::new(),
        None,
    )
    .op
}

fn build_operator_internal<R: Read + 'static, W: Write + 'static>(
    query_op: &QueryOp,
    ctx: &DbContext,
    buffer_pool: &mut BufferPool<R, W>,
    sort_memory_bytes: usize,
    hash_join_budget: usize,
    global_needed: &std::collections::HashSet<String>,
    needed_above: &std::collections::HashSet<String>,
    pushed_predicates: Vec<Predicate>,
    dynamic_filter: Option<(String, std::sync::Arc<std::sync::Mutex<Option<JoinFilter>>>)>,
) -> PhysicalPlan<R, W> {
    match query_op {
        QueryOp::Scan(scan_data) => {
            let table_spec = ctx
                .get_table_specs()
                .iter()
                .find(|t| t.name == scan_data.table_id)
                .unwrap_or_else(|| panic!("Table '{}' not found", scan_data.table_id));

            let mut needed_indices = Vec::new();
            for (i, cs) in table_spec.column_specs.iter().enumerate() {
                if global_needed.contains(&cs.column_name) || global_needed.is_empty() {
                    needed_indices.push(i);
                }
            }
            if needed_indices.is_empty() && !table_spec.column_specs.is_empty() {
                needed_indices.push(0);
            }

            let ordering = scan_ordering(&table_spec.column_specs, &needed_indices);

            PhysicalPlan::new(
                Box::new(TableScanner::new(
                    buffer_pool,
                    &table_spec.file_id,
                    &table_spec.column_specs,
                    needed_indices,
                    pushed_predicates,
                    dynamic_filter,
                )),
                ordering,
            )
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
                let leaf_schemas: Vec<Vec<String>> =
                    leaves.iter().map(|leaf| schema_of(leaf, ctx)).collect();

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

                // Deterministic start to preserve logical left-to-right behavior.
                let start_idx = 0usize;
                let mut current_cardinality =
                    estimate_cardinality(&leaf_schemas[start_idx], ctx).unwrap_or(1);

                let mut joined = vec![false; leaves.len()];
                joined[start_idx] = true;
                // Build the starting leaf with its pushed-down scalar predicates
                let start_leaf_op = build_leaf_with_filter(
                    leaves[start_idx],
                    &scalar_preds[start_idx],
                    &global_needed,
                    ctx,
                    buffer_pool,
                    sort_memory_bytes,
                    hash_join_budget,
                    None,
                );
                let mut current_schema: Vec<String> = start_leaf_op.op.schema();
                let mut current_plan = start_leaf_op;

                while joined.iter().any(|&m| !m) {
                    let mut best_leaf_idx = None;
                    let mut best_pred_idx = None;
                    let mut min_projected_rows = f64::MAX;
                    // Deterministic leaf selection: scan leaves in original order and
                    // pick the first joinable one. This avoids full CROSS fallback while
                    // preserving stable output behavior for order-sensitive tests.
                    'find_next_leaf: for leaf_idx in 0..leaves.len() {
                        if joined[leaf_idx] {
                            continue;
                        }
                        let new_schema = &leaf_schemas[leaf_idx];
                        for pred_idx in 0..remaining_preds.len() {
                            if !matches!(
                                remaining_preds[pred_idx].operator,
                                ComparisionOperator::EQ
                            ) {
                                continue;
                            }
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
                                    best_leaf_idx = Some(leaf_idx);
                                    best_pred_idx = Some(pred_idx);
                                    min_projected_rows = current_cardinality as f64;
                                    break 'find_next_leaf;
                                }
                            }
                        }
                    }

                    if let Some(leaf_idx) = best_leaf_idx {
                        let pred_idx = best_pred_idx.unwrap();
                        current_cardinality = min_projected_rows.max(1.0) as u64;
                        let new_schema = &leaf_schemas[leaf_idx];

                        let col_a = remaining_preds[pred_idx].column_name.clone();
                        let col_b = match &remaining_preds[pred_idx].value {
                            ComparisionValue::Column(c) => c.clone(),
                            _ => unreachable!(),
                        };

                        let a_cur = current_schema.contains(&col_a);
                        let b_new = new_schema.contains(&col_b);

                        let joined_count = joined.iter().filter(|&&m| m).count();
                        let use_ordered_merge = if a_cur && b_new {
                            joined_count == 1 && can_use_ordered_merge_join(&col_a, &col_b, ctx)
                        } else {
                            joined_count == 1 && can_use_ordered_merge_join(&col_b, &col_a, ctx)
                        };

                        let dyn_filter = std::sync::Arc::new(std::sync::Mutex::new(None));
                        let right_filter_col = if a_cur && b_new {
                            col_b.clone()
                        } else {
                            col_a.clone()
                        };

                        let right_op = build_leaf_with_filter(
                            leaves[leaf_idx],
                            &scalar_preds[leaf_idx],
                            &global_needed,
                            ctx,
                            buffer_pool,
                            sort_memory_bytes,
                            hash_join_budget,
                            if use_ordered_merge {
                                None
                            } else {
                                Some((right_filter_col, dyn_filter.clone()))
                            },
                        );

                        let right_schema = right_op.op.schema();
                        let l_actual_schema = current_plan.op.schema();

                        let (l_idx, r_idx) = if a_cur && b_new {
                            (
                                l_actual_schema.iter().position(|x| x == &col_a).unwrap(),
                                right_schema.iter().position(|x| x == &col_b).unwrap(),
                            )
                        } else {
                            (
                                l_actual_schema.iter().position(|x| x == &col_b).unwrap(),
                                right_schema.iter().position(|x| x == &col_a).unwrap(),
                            )
                        };

                        let ldt = current_plan.op.data_types();
                        let rdt = right_op.op.data_types();

                        current_plan = if use_ordered_merge {
                            let output_order_col = l_actual_schema[l_idx].clone();
                            PhysicalPlan::new(
                                Box::new(SortMergeJoinOp::new(
                                    current_plan.op,
                                    right_op.op,
                                    l_idx,
                                    r_idx,
                                    buffer_pool,
                                )),
                                Some(vec![(output_order_col, true)]),
                            )
                        } else {
                            buffer_pool
                                .set_eviction_policy(crate::buffer_pool::EvictionPolicy::LRU);
                            PhysicalPlan::new(
                                Box::new(HashJoinOp::new(
                                    current_plan.op,
                                    right_op.op,
                                    l_idx,
                                    r_idx,
                                    ldt,
                                    rdt,
                                    buffer_pool,
                                    hash_join_budget,
                                    Some(dyn_filter),
                                )),
                                None,
                            )
                        };

                        current_schema.extend(right_schema);
                        joined[leaf_idx] = true;
                        remaining_preds.remove(pred_idx);

                        // ── Inter-join projection ─────────────────────────
                        // Strip columns that are no longer needed by any
                        // remaining join predicate, remaining filter, or
                        // the output above (needed_above).  This shrinks
                        // rows flowing into subsequent joins and into sort
                        // by 2-10x, drastically reducing disk I/O.
                        if !needed_above.is_empty() {
                            let mut still_needed = needed_above.clone();
                            for rp in &remaining_preds {
                                still_needed.insert(rp.column_name.clone());
                                if let ComparisionValue::Column(c) = &rp.value {
                                    still_needed.insert(c.clone());
                                }
                            }
                            for (li, j) in joined.iter().enumerate() {
                                if !j {
                                    for sp in &scalar_preds[li] {
                                        still_needed.insert(sp.column_name.clone());
                                        if let ComparisionValue::Column(c) = &sp.value {
                                            still_needed.insert(c.clone());
                                        }
                                    }
                                }
                            }
                            let keep: Vec<(String, String)> = current_schema
                                .iter()
                                .filter(|c| still_needed.contains(*c))
                                .map(|c| (c.clone(), c.clone()))
                                .collect();
                            if !keep.is_empty() && keep.len() < current_schema.len() {
                                let ordering = remap_ordering_for_project(
                                    current_plan.ordering.as_deref(),
                                    &keep,
                                );
                                current_plan = PhysicalPlan::new(
                                    Box::new(ProjectOp::new(current_plan.op, keep)),
                                    ordering,
                                );
                                current_schema = current_plan.op.schema();
                            }
                        }
                    } else {
                        // No join predicate found: do a cross product with next unjoined leaf
                        let leaf_idx = joined.iter().position(|&m| !m).unwrap();
                        let right_op = build_leaf_with_filter(
                            leaves[leaf_idx],
                            &scalar_preds[leaf_idx],
                            &global_needed,
                            ctx,
                            buffer_pool,
                            sort_memory_bytes,
                            hash_join_budget,
                            None,
                        );
                        let right_schema = right_op.op.schema();
                        current_plan = PhysicalPlan::new(
                            Box::new(CrossOp::new(current_plan.op, right_op.op, buffer_pool)),
                            None,
                        );
                        current_schema.extend(right_schema);
                        joined[leaf_idx] = true;
                    }
                }

                return if remaining_preds.is_empty() {
                    current_plan
                } else {
                    PhysicalPlan::new(
                        Box::new(FilterOp::new(current_plan.op, remaining_preds)),
                        current_plan.ordering,
                    )
                };
            }

            // Standard filter (no join rewrite)
            let und = build_operator_internal(
                &filter_data.underlying,
                ctx,
                buffer_pool,
                sort_memory_bytes,
                hash_join_budget,
                global_needed,
                needed_above,
                Vec::new(),
                dynamic_filter,
            );
            let preds: Vec<Predicate> = filter_data
                .predicates
                .iter()
                .map(|p| clone_predicate(p))
                .collect();
            PhysicalPlan::new(Box::new(FilterOp::new(und.op, preds)), und.ordering)
        }

        QueryOp::Project(project_data) => {
            // Compute which columns the Project reads from its child.
            // Pass this as needed_above so the child can prune early.
            let mut child_needed = std::collections::HashSet::<String>::new();
            for (old_name, _) in &project_data.column_name_map {
                child_needed.insert(old_name.clone());
            }
            let child = build_operator_internal(
                &project_data.underlying,
                ctx,
                buffer_pool,
                sort_memory_bytes,
                hash_join_budget,
                global_needed,
                &child_needed,
                Vec::new(),
                dynamic_filter,
            );
            let ordering = remap_ordering_for_project(
                child.ordering.as_deref(),
                &project_data.column_name_map,
            );
            PhysicalPlan::new(
                Box::new(ProjectOp::new(
                    child.op,
                    project_data.column_name_map.clone(),
                )),
                ordering,
            )
        }

        QueryOp::Cross(cross_data) => {
            let left = build_operator_internal(
                &cross_data.left,
                ctx,
                buffer_pool,
                sort_memory_bytes,
                hash_join_budget,
                global_needed,
                needed_above,
                Vec::new(),
                None,
            );
            let right = build_operator_internal(
                &cross_data.right,
                ctx,
                buffer_pool,
                sort_memory_bytes,
                hash_join_budget,
                global_needed,
                needed_above,
                Vec::new(),
                None,
            );
            PhysicalPlan::new(Box::new(CrossOp::new(left.op, right.op, buffer_pool)), None)
        }

        QueryOp::Sort(sort_data) => {
            // ── Early projection before sort ──────────────────────────
            // Compute which columns the sort actually needs:
            //   sort-key columns ∪ columns the parent (Project) needs.
            // Everything else is dead weight that bloats sort runs.
            let mut sort_needs = needed_above.clone();
            for spec in &sort_data.sort_specs {
                sort_needs.insert(spec.column_name.clone());
            }

            let mut child = build_operator_internal(
                &sort_data.underlying,
                ctx,
                buffer_pool,
                sort_memory_bytes,
                hash_join_budget,
                global_needed,
                &sort_needs,
                Vec::new(),
                dynamic_filter,
            );

            // Strip unnecessary columns before they enter the sort.
            // This can shrink rows 3-10x for multi-table join results,
            // allowing the sort to run entirely in-memory.
            if !needed_above.is_empty() {
                let child_schema = child.op.schema();
                let keep: Vec<(String, String)> = child_schema
                    .iter()
                    .filter(|c| sort_needs.contains(*c))
                    .map(|c| (c.clone(), c.clone()))
                    .collect();
                if !keep.is_empty() && keep.len() < child_schema.len() {
                    let ordering = remap_ordering_for_project(child.ordering.as_deref(), &keep);
                    child = PhysicalPlan::new(Box::new(ProjectOp::new(child.op, keep)), ordering);
                }
            }

            if ordering_satisfies(child.ordering.as_deref(), &sort_data.sort_specs) {
                return child;
            }

            let data_types = child.op.data_types();
            PhysicalPlan::new(
                Box::new(SortOp::new(
                    child.op,
                    &sort_data.sort_specs,
                    data_types,
                    buffer_pool,
                    sort_memory_bytes,
                )),
                Some(sort_specs_to_ordering(&sort_data.sort_specs)),
            )
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
        ComparisionOperator::EQ => ComparisionOperator::EQ,
        ComparisionOperator::NE => ComparisionOperator::NE,
        ComparisionOperator::GT => ComparisionOperator::GT,
        ComparisionOperator::GTE => ComparisionOperator::GTE,
        ComparisionOperator::LT => ComparisionOperator::LT,
        ComparisionOperator::LTE => ComparisionOperator::LTE,
    }
}

fn clone_cmp_value(v: &ComparisionValue) -> ComparisionValue {
    match v {
        ComparisionValue::Column(s) => ComparisionValue::Column(s.clone()),
        ComparisionValue::I32(n) => ComparisionValue::I32(*n),
        ComparisionValue::I64(n) => ComparisionValue::I64(*n),
        ComparisionValue::F32(n) => ComparisionValue::F32(*n),
        ComparisionValue::F64(n) => ComparisionValue::F64(*n),
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
fn should_use_hash_join(left_schema: &[String], right_schema: &[String], ctx: &DbContext) -> bool {
    let threshold = 1_000u64;

    let left_card = estimate_cardinality(left_schema, ctx);
    let right_card = estimate_cardinality(right_schema, ctx);

    match (left_card, right_card) {
        (Some(l), Some(r)) => l > threshold || r > threshold,
        _ => true,
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
    hash_join_budget: usize,
    dynamic_filter: Option<(String, std::sync::Arc<std::sync::Mutex<Option<JoinFilter>>>)>,
) -> PhysicalPlan<R, W> {
    let pushed: Vec<Predicate> = scalar_preds
        .iter()
        .filter(|p| {
            !matches!(p.value, ComparisionValue::Column(_))
                && matches!(
                    p.operator,
                    ComparisionOperator::EQ
                        | ComparisionOperator::LT
                        | ComparisionOperator::LTE
                        | ComparisionOperator::GT
                        | ComparisionOperator::GTE
                )
        })
        .map(|p| clone_predicate(p))
        .collect();
    let empty_needed = std::collections::HashSet::<String>::new();
    let mut plan = build_operator_internal(
        leaf,
        ctx,
        buffer_pool,
        sort_memory_bytes,
        hash_join_budget,
        global_needed,
        &empty_needed,
        pushed,
        dynamic_filter,
    );

    if !scalar_preds.is_empty() {
        let preds: Vec<Predicate> = scalar_preds.iter().map(|p| clone_predicate(p)).collect();
        plan = PhysicalPlan::new(Box::new(FilterOp::new(plan.op, preds)), plan.ordering);
    }

    let current_schema = plan.op.schema();

    // Project pushdown: Keep only columns requested globally that are present in the current schema
    let needed_here: Vec<(String, String)> = current_schema
        .into_iter()
        .filter(|c| global_needed.contains(c))
        .map(|c| (c.clone(), c))
        .collect();

    // Only wrap with ProjectOp if it actually restricts columns
    if !needed_here.is_empty() && needed_here.len() < plan.op.schema().len() {
        let ordering = remap_ordering_for_project(plan.ordering.as_deref(), &needed_here);
        plan = PhysicalPlan::new(Box::new(ProjectOp::new(plan.op, needed_here)), ordering);
    }

    plan
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
        QueryOp::Scan(scan) => ctx
            .get_table_specs()
            .iter()
            .find(|t| t.name == scan.table_id)
            .map(|t| {
                t.column_specs
                    .iter()
                    .map(|c| c.column_name.clone())
                    .collect()
            })
            .unwrap_or_default(),
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
    DataType::String
}

/// Return the first CardinailtyData value found for any column in the schema.
/// Handles aliased columns like "l1.l_orderkey" by stripping the alias prefix.
fn estimate_cardinality(schema: &[String], ctx: &DbContext) -> Option<u64> {
    for col_name in schema {
        // Try to find the unique count which roughly equals table size for PKs
        if let Some(card) = get_column_distinct_count(col_name, ctx) {
            return Some(card);
        }
    }
    None
}

/// Retrieve the number of distinct values for a given column from its statistics
fn get_column_distinct_count(col_name: &str, ctx: &DbContext) -> Option<u64> {
    let names_to_try: &[&str] = if let Some(dot) = col_name.find('.') {
        let base = &col_name[dot + 1..];
        &[col_name, base]
    } else {
        &[col_name]
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
    None
}

fn get_column_density(col_name: &str, ctx: &DbContext) -> Option<f32> {
    let names_to_try: &[&str] = if let Some(dot) = col_name.find('.') {
        let base = &col_name[dot + 1..];
        &[col_name, base]
    } else {
        &[col_name]
    };
    for &name in names_to_try {
        for table in ctx.get_table_specs() {
            for cs in &table.column_specs {
                if cs.column_name == name {
                    if let Some(stats) = &cs.stats {
                        for stat in stats {
                            if let ColumnStat::DensityStat(db_config::statistics::Density(
                                density,
                            )) = stat
                            {
                                return Some(*density);
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn is_physically_ordered_column(col_name: &str, ctx: &DbContext) -> bool {
    let names_to_try: &[&str] = if let Some(dot) = col_name.find('.') {
        let base = &col_name[dot + 1..];
        &[col_name, base]
    } else {
        &[col_name]
    };
    for &name in names_to_try {
        for table in ctx.get_table_specs() {
            for cs in &table.column_specs {
                if cs.column_name == name {
                    if let Some(stats) = &cs.stats {
                        if stats
                            .iter()
                            .any(|stat| matches!(stat, ColumnStat::IsPhysicallyOrdered))
                        {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

fn can_use_ordered_merge_join(left_col: &str, right_col: &str, ctx: &DbContext) -> bool {
    if !is_physically_ordered_column(left_col, ctx) || !is_physically_ordered_column(right_col, ctx)
    {
        return false;
    }

    let left_density = get_column_density(left_col, ctx).unwrap_or(1.0);
    let right_density = get_column_density(right_col, ctx).unwrap_or(1.0);
    left_density >= 0.10 && right_density >= 0.10
}

fn sort_specs_to_ordering(sort_specs: &[SortSpec]) -> Vec<(String, bool)> {
    sort_specs
        .iter()
        .map(|spec| (spec.column_name.clone(), spec.ascending))
        .collect()
}

fn ordering_satisfies(ordering: Option<&[(String, bool)]>, required_specs: &[SortSpec]) -> bool {
    let Some(actual) = ordering else {
        return false;
    };
    if required_specs.len() > actual.len() {
        return false;
    }
    required_specs
        .iter()
        .zip(actual.iter())
        .all(|(required, actual_spec)| {
            required.column_name == actual_spec.0 && required.ascending == actual_spec.1
        })
}

fn remap_ordering_for_project(
    ordering: Option<&[(String, bool)]>,
    column_name_map: &[(String, String)],
) -> Option<Vec<(String, bool)>> {
    let ordering = ordering?;
    let mut remapped = Vec::new();
    for (column_name, ascending) in ordering {
        let Some((_, output_name)) = column_name_map
            .iter()
            .find(|(input_name, _)| input_name == column_name)
        else {
            break;
        };
        remapped.push((output_name.clone(), *ascending));
    }
    if remapped.is_empty() {
        None
    } else {
        Some(remapped)
    }
}

fn scan_ordering(
    column_specs: &[db_config::table::ColumnSpec],
    needed_indices: &[usize],
) -> Option<Vec<(String, bool)>> {
    for &idx in needed_indices {
        let column = &column_specs[idx];
        if column
            .stats
            .as_ref()
            .map(|stats| {
                stats
                    .iter()
                    .any(|stat| matches!(stat, ColumnStat::IsPhysicallyOrdered))
            })
            .unwrap_or(false)
        {
            return Some(vec![(column.column_name.clone(), true)]);
        }
    }
    None
}
