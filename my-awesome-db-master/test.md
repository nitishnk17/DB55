# Manual Testing Guide

## Folder Structure

```
my-awesome-db-dev-public_tests/     ← test harness (given to you)
  target/release/
    tests_gen                        ← generates expected outputs from SQLite
    monitor                          ← runs your database and validates output
    disk                             ← disk simulator binary

Github/DB55/my-awesome-db-master/   ← your submission
  target/release/
    database                         ← your database binary
    disk                             ← your disk simulator binary
  scratch/
    compiled_datasets/tpch/
      sqlite.db                      ← TPC-H dataset (used by tests_gen)
      nation.bin, lineitem.bin, ...  ← binary table files (used by your db)
    runtimes/tpch/
      monitor_config.json            ← test config (updated by tests_gen)
      db_config.json                 ← table schema for your database
      disk_sim_config.json           ← disk simulator config
      expected_output_1.csv          ← expected output for test 1 (generated)
      expected_output_2.csv          ← expected output for test 2 (generated)
      ...                            ← up to expected_output_60.csv
```

---

## Step 1: Build Your Submission

```bash
cd "/Users/nitishkumar/Downloads/IIT Delhi/DBMS/Assignment/Ass2/Github/DB55/my-awesome-db-master"

cargo build --release
```

Expected output: `Finished release profile [optimized]`

---

## Step 2: Generate Expected Outputs from SQLite

This runs all 60 SQL queries through `sqlite3` and writes the correct answers to `expected_output_N.csv` files.

```bash
cd "/Users/nitishkumar/Downloads/IIT Delhi/DBMS/Assignment/Ass2/Github/DB55/my-awesome-db-master"

../../../my-awesome-db-dev-public_tests/target/release/tests_gen \
    --compiled-dataset-folder "./scratch/compiled_datasets/tpch" \
    --runtime-folder "./scratch/runtimes/tpch"
```

Expected output: prints `Processing query SELECT ...` for all 60 queries.

---

## Step 3: Run All 60 Tests

```bash
cd "/Users/nitishkumar/Downloads/IIT Delhi/DBMS/Assignment/Ass2/Github/DB55/my-awesome-db-master"

../../../my-awesome-db-dev-public_tests/target/release/monitor \
    --config scratch/runtimes/tpch/monitor_config.json 2>/dev/null
```

Expected output: `Validation success! for SELECT ...` for all 60 queries, exit code `0`.

---

## Step 4: Run a Single Test

To run only test N (e.g., test 1):

```bash
python3 - <<'EOF'
import json, copy

with open("scratch/runtimes/tpch/monitor_config.json") as f:
    cfg = json.load(f)

N = 1  # change this to run a different test (1 to 60)

single = copy.deepcopy(cfg)
single["query_configs"] = [cfg["query_configs"][N - 1]]

with open("/tmp/single_test.json", "w") as f:
    json.dump(single, f, indent=2)

print(f"Test {N}: {single['query_configs'][0]['execution_name']}")
EOF

../../../my-awesome-db-dev-public_tests/target/release/monitor \
    --config /tmp/single_test.json 2>/dev/null
```

---

## Step 5: Run a Range of Tests

To run tests 51 to 60 (benchmark queries):

```bash
python3 - <<'EOF'
import json, copy

START = 51   # first test (1-indexed)
END   = 60   # last test (1-indexed)

with open("scratch/runtimes/tpch/monitor_config.json") as f:
    cfg = json.load(f)

subset = copy.deepcopy(cfg)
subset["query_configs"] = cfg["query_configs"][START - 1:END]

with open("/tmp/range_test.json", "w") as f:
    json.dump(subset, f, indent=2)

print(f"Running tests {START} to {END}")
EOF

../../../my-awesome-db-dev-public_tests/target/release/monitor \
    --config /tmp/range_test.json 2>/dev/null
```

---

## Step 6: Check the Expected Output for Any Test

To manually see what output is expected for test N:

```bash
cat scratch/runtimes/tpch/expected_output_1.csv   # test 1
cat scratch/runtimes/tpch/expected_output_54.csv  # test 54
```

---

## Test Categories

| Tests  | Category                          |
|--------|-----------------------------------|
| 1–5    | Full table scans                  |
| 6–15   | Single-table filters (WHERE)      |
| 16–30  | Sort + filter (ORDER BY)          |
| 31–35  | Cross products                    |
| 36–40  | 2-table equi-joins                |
| 41–45  | 2-table join + filter + sort      |
| 46–50  | 3-table joins                     |
| 51–60  | TPC-H benchmark queries (Q1–Q10)  |

---

## What Each Tool Does

- **`tests_gen`** — reads `monitor_config.json` (for binary paths), runs each SQL query through `sqlite3`, and writes `expected_output_N.csv` files. Also rewrites `monitor_config.json` with the new query list.
- **`monitor`** — spawns your `disk` and `database` binaries, sends each query, and compares your output against `expected_output_N.csv`. Stops on first failure (exit code `1`), exits `0` if all pass.
- **`database`** — your query engine. Reads query from monitor, reads blocks from disk, streams results back to monitor.
- **`disk`** — simulates a spinning disk with realistic seek/latency metrics. Serves binary block data to your database.

---

## Interpreting Results

**Pass:**
```
Validation success! for SELECT n_nationkey, n_name, n_regionkey, '' FROM nation;
```

**Fail:**
```
Error: Validation failed! for SELECT ...
Caused by: Expected line output
  0|ALGERIA|0|
but database returned
  ...
error at line 1
```
