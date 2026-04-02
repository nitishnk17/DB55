# Day 13 and Day 14 Completion Verification & Execution Plan

We have thoroughly analyzed the requirements mapped out in `report.md` for Day 13 and Day 14, and verified them against our current project state.

## Current Completion Status

### Day 13: Integration & Multi-Query Testing
**Objective**: Test all operator combinations, write SQL queries, generate expected output with SQLite, configure monitor and test execution path.
**Status**: **100% COMPLETE ✅**
- We have created an extensive suite of **11 query tests** in `monitor_config.json` spanning combinations of `Scan`, `Project`, `Filter`, `Cross`, and `Sort`.
- We successfully generated true-source outputs from SQLite using `trailing pipe tricks`.
- We ensured 100% test validity without failures, handling both deep iterator nesting and complex orderings perfectly.

### Day 14: Buffer & Cleanup
**Objective**: Bug-fix day, code cleanup, address any failing test cases, and fix error handling.
**Status**: **80% COMPLETE ⚠️** (Remaining: Cleanups & Lints)
- We already resolved the critical edge-case bugs early (e.g. SQLite float formatting differences between integer-decimals and zero-stripping).
- All tests pass, so there are no functional bugs holding us back. 
- **Missed Parts Identified**: 
  1. We have residual technical debt from early scaffolding (a few unused imports and unread struct fields).
  2. We have `eprintln!` debugging statements left in the code from our advanced external sort multi-pass feature building.
  3. We have some minor idiomatic rust warnings exposed by `cargo clippy`.

---

## Execution Plan for Day 14 (Remaining Cleanup)

This focused plan handles the remaining technical debt to give us a pristine codebase before we move into Phase 3 (Day 15 - Joins).

### Step 1: Remove Stale Code & Debug Statements
- **Target File**: `database/src/sort.rs`
    - Remove the unread `current_block_id` tracking in `merge_k_runs_to_disk` because anonymous block IDs are fire-and-forget in external sorting allocations.
    - Remove the trace debug logs (`eprintln!`) describing intermediate chunk counts in external sort to ensure standard output remains extremely clean for the testing interface.
- **Target File**: `database/src/table_scanner.rs`
    - Remove the `column_specs` struct field; our scanner implementation correctly leverages it exclusively in the constructor (`new`) so holding it is unnecessary state.

### Step 2: Fix Remaining Clippy / Idiomatic Warnings
- **Target File**: `database/src/main.rs`
    - Remove the `use operator::Operator;` line which became an unused block-level import after refactoring.
- **Target File**: `database/src/buffer_pool.rs`
    - Remove the unused `mark_dirty` helper method, as our current operator designs don't perform in-place updates.
    - Refactor the doubly-nested `if let Some... if count > 0` pin management into a cleaner, single `&&` boolean check to resolve the `collapsible_if` warning.
- **Target File**: `database/src/query_executor.rs`
    - Change `.expect(&format!("..."))` to `.unwrap_or_else(|| panic!("..."))` during table existence lookups to prevent eager format string allocations on successful paths.

### Step 3: Final Build & Validation
- Run `cargo clippy --bin database` to guarantee absolutely 0 warnings are emitted.
- Run `cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json` one last time to ensure we haven't broken the test pipeline during cleanup. 

---

> [!TIP]
> This wraps up Phase 2 exactly as specified. Once we finish these cleanups, the database is in a robust position for Phase 3 (Implementing Advanced Joins).

Please approve this plan so we can eliminate the last bits of technical debt and complete Phase 2!
