# Day 15 Implementation Plan: Join Detection & BNLJ

This plan covers Phase 3 Day 15 requirements: detecting join patterns in the query tree and replacing the naive `Cross + Filter` with a physically optimized **Block Nested Loop Join (BNLJ)** operator.

## Architecture Refactoring: Reusing Disk Components
In Day 11/12, we built `Run`, `RunReader`, `AnonBlockAllocator`, and `rows_to_blocks` directly inside `sort.rs`. Since a robust block-nested loop join requires materializing inner relations to disk and streaming them block-by-block, we will reuse these components.

*   **Step 1:** Extract these components from `sort.rs` into a new module: `database/src/disk_run.rs`.
*   **Step 2:** Expose them with `pub(crate)` so both `sort.rs` and the new `join.rs` can use them.

## 1. Join Pattern Detection (`query_executor.rs`)

When translating the `QueryOp` AST into physical `Box<dyn Operator>` types, we want to intercept inefficient cross products.

1.  In `build_operator`, when encountering `QueryOp::Filter(filter_data)`:
2.  Check if `filter_data.underlying` is `QueryOp::Cross(cross_data)`.
3.  If it is, inspect the `predicates` inside the Filter. Look for an **Equi-Join predicate**:
    *   `operator == ComparisionOperator::EQ`
    *   `value == ComparisionValue::Column(col_b)`
4.  Verify cross-boundary access: Does `col_a` belong to the left schema and `col_b` belong to the right schema (or vice versa)? 
    *   We will recursively call `build_operator` on `left` and `right` to obtain their instances and query their `.schema()`.
5.  If a valid equijoin pattern is detected:
    *   We resolve the column indices for the join condition on the left and right schemas.
    *   We generate a `JoinOp` instance taking `left`, `right`, and the matching indices.
    *   If the original `Filter` had *other* predicates as well, we wrap the new `JoinOp` in a `FilterOp` to evaluate them post-join.

## 2. Block Nested Loop Join (`join.rs`)

The `JoinOp` must fulfill the specific memory constraints of the Volcano Iterator Model while obeying BNLJ: **(B - 2) pages for outer, 1 page for inner, 1 for output**.

### Initialization (`JoinOp::new`)
*   Read the entire `right` operator into anonymous disk blocks, producing a `Run`. This ensures we can restart scanning the inner relation multiple times without holding it in memory, overcoming the forward-only nature of the `Operator` iterator.
*   Calculate the size of `left_chunk` in rows based on `(B - 2) * block_size`.

### Execution (`JoinOp::next`)
We will use a State Machine to handle the chunked looping:
1.  **Load Left Chunk:** If the `left_chunk` buffer is empty, read rows from the `left` operator until we hit the memory budget. If no rows remain, `return None`.
    *   Immediately initialize a new `RunReader` targeting the `right` disk-run.
2.  **Join Processing:** 
    *   Read the next row from the `RunReader` (Inner side).
    *   Iterate through all rows in `left_chunk` (Outer side). 
    *   Compare the join keys. When a match is found, return the combined `Row`.
    *   *Note:* Because one inner row can match multiple outer rows, we must track our inner loop index meticulously so subsequent calls to `next()` resume checking the *same* inner row against the remainder of the `left_chunk`.
3.  **Advance Right:** When the current inner row has been checked against all outer chunk rows, advance the `RunReader`. If the inner reader is exhausted, clear the `left_chunk` so the next call loads a new block from the `left` operator (returning to State 1).

## 3. Testing Context
The TPCH suite (our `monitor_config.json`) already has a join query: **Cross+Filter - Region join Nation**. 
Currently, the runtime output shows this using `CrossOp`. After this implementation, it should use `JoinOp` and significantly decrease disk IO / time vs a naive cross product.

## Open Questions & Review Required
> [!IMPORTANT]
> The only significant design change is extracting the `Run` and `AnonBlockAllocator` from `sort.rs` into an independent `disk_run.rs` module so both operators can use them. 
> Do you approve this refactor and the execution plan for Day 15?
