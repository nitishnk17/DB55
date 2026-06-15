# Out-of-Core TPC-H Query Execution Engine

This repository contains a high-performance database execution engine implemented in Rust, specifically engineered to process TPC-H datasets that exceed available physical memory. The system is built to navigate extreme resource constraints, including a 64MB virtual memory limit (RLIMIT_AS) and restricted file system access.

## Core Architectural Pillars

### 1. Adaptive Memory Partitioning
The engine does not use a one-size-fits-all memory budget. Instead, it profiles each query's AST before execution to detect the presence of `SORT` or `CROSS` operations. Based on this profile, it dynamically partitions the 64MB budget:
*   **Adaptive Budgeting**: Adjusts `pool_pct`, `sort_pct`, and `overhead` to prioritize memory for the most bottleneck-prone operators.
*   **On-Demand Allocation**: The Buffer Pool avoids upfront allocation of frame data, only allocating the 4KB buffers as blocks are fetched, ensuring every byte of `RLIMIT_AS` is used efficiently.
*   **Safety Reserves**: Maintains a fixed 10-14MB overhead buffer to account for Rust's dynamic linking, stack usage, and shared library mappings.

### 2. Advanced Storage Engine & Buffer Pool
*   **Sequential Flooding Prevention**: Operators like `TableScanner` bypass the LRU cache using a dedicated `read_blocks_sequential` interface. This prevents large sequential scans from evicting hot pages (e.g., join build-sides) from the buffer pool.
*   **O(1) LRU Management**: The buffer pool utilizes a high-performance eviction strategy that avoids O(N) list traversals during cache hits by allowing duplicate entries in the LRU list and lazily skipping stale references during eviction scans.
*   **Anonymous Block Recycling**: To comply with the 10GB scratch space limit, the engine implements a free-list for anonymous block IDs. Temporary disk runs from external sorts and joins are recycled via `free_run`, keeping the disk high-watermark bounded.

### 3. Query Execution & Optimization
The system utilizes a **Volcano Iterator Model** enhanced with several rule-based and cost-inspired optimizations:
*   **Multi-Table Join Rewriting**: Automatically flattens nested `Cross` operations and reorders them using a deterministic greedy approach based on cardinality statistics.
*   **Dynamic Filtering**: Implements a bitset-based dynamic filtering mechanism where the build side of a join provides a filter to the probe side's scanner, enabling early row rejection before the probe even reaches the join operator.
*   **Multi-Stage Projection Pushdown**: Columns are pruned at every possible level—from the initial scan to intermediate join results—minimizing the width of rows flowing through the pipeline and reducing disk I/O for external sorts.
*   **Hybrid Join Selection**: Automatically switches between **Chained Hash Join** (optimized for heap-pressure) and **Sort-Merge Join** (triggered by physical column ordering and high density).

## Setup and Installation

### Prerequisites
*   **Rust Toolchain**: Edition 2024 (requires Rust >= 1.85).
*   **SQLite3**: Necessary for result verification during test generation.
*   **Standard Build Tools**: `make`, `gcc`.

### Installation
```bash
# Clone and build
git clone <repository-url>
cd my-db
cargo build -r
```

## Operational Guide

### 0. Dataset Acquisition
Download the `tpch_scratch.tar.gz` starter dataset from the following location:
*   **Link**: [TPC-H Datasets (Google Drive)](https://drive.google.com/drive/folders/1NsbGUAsfacgNLeDUC6KrM_HruTTPfe8v)
*   **Extraction**: Extract the contents into the `scratch/` directory such that the CSV files are located in `scratch/datasets/tpch/`.

### 1. Data Compilation
Compile the raw TPC-H CSV data into the optimized binary format:
```bash
cargo run -r --bin generator -- all \
    -d scratch/datasets/tpch \
    -c scratch/compiled_datasets/tpch \
    -r scratch/runtimes/tpch \
    -b target/release \
    -s 4096
```

### 2. Test Suite Generation
Generate the 60+ TPC-H correctness tests:
```bash
cargo run -r --bin tests_gen -- -c scratch/compiled_datasets/tpch -r scratch/runtimes/tpch
```

### 3. Execution & Validation
Run the integration suite via the monitor utility:
```bash
cargo run -r --bin monitor -- -c scratch/runtimes/tpch/monitor_config.json
```

## System Constraints
The engine is verified to comply with the following grading server limits:
*   **Memory (RLIMIT_AS)**: 64 MB (Virtual Address Space)
*   **Disk Scratch Space**: 10 GB (Unique Anonymous Blocks)
*   **I/O**: Pipe-based protocol (No direct file writes via RLIMIT_FSIZE)
*   **Concurrency**: Single-process execution (RLIMIT_NPROC=1)
