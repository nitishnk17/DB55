# Day 2 Execution Plan & Starter Code Walkthrough

## 1. P1 Tasks: Data Import & Exploration
**Status Verification:** Partially Complete.
- **Downloaded & Extracted:** The `tpch_scratch.tar.gz` and its contents correctly exist inside `scratch/datasets/tpch/` (you have [customer.csv](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/datasets/tpch/customer.csv), [lineitem.csv](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/datasets/tpch/lineitem.csv), etc.).
- **Missing Step:** The system hasn't yet generated the [.bin](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/compiled_datasets/tpch/part.bin) files and configs. `scratch/compiled_datasets/tpch` is empty. 

**Execution Plan for P1:**
1. Run the following command from the `my-awesome-db-master` directory:
```bash
cargo run -r --bin generator -- all \
    -d scratch/datasets/tpch \
    -c scratch/compiled_datasets/tpch \
    -r scratch/runtimes/tpch \
    -b target/release \
    -s 4096
```
2. This will read the [.csv](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/datasets/tpch/part.csv) and [.schema](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/datasets/tpch/part.schema) files in `datasets/tpch/` and pack them into the binary disk block format expected by the Disk Simulator. It will also produce the necessary [db_config.json](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/runtimes/tpch/db_config.json).
3. Verify that `scratch/compiled_datasets/` populates with [.bin](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/scratch/compiled_datasets/tpch/part.bin) files representing the formatted disk regions.

---

## 2. P2 Tasks: Study Starter Code

Based on the source files `database.rs`, [query.rs](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs), and [table.rs](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs), here is a step-by-step breakdown of what you need to understand:

### A. The AST Structure ([Query](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#4-7) & `QueryOp`)
Located in [common/src/query.rs](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs).
When the monitor sends a query it comes in as a JSON string and is deserialized into a [Query](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#4-7) object by the database.
- [Query](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#4-7) contains a `root` of type `QueryOp`.
- `QueryOp` is an enum acting as the Abstract Syntax Tree (AST) node. It can be a [Scan](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#64-67), [Sort](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#59-63), [Project](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#42-46), [Filter](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#36-40), or [Cross](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#48-52). 
- Each variant contains a struct with its specific data (e.g., [FilterData](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#36-40) contains a list of [Predicate](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#29-34)s and a boxed underlying `QueryOp` acting as its child node). 
- Because operators like [Filter](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#36-40) or [Cross](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#48-52) box another `QueryOp`, it creates a recursive tree, perfectly mirroring a query execution plan.

### B. Table & Column Specifications ([TableSpec](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs#14-19) & [ColumnSpec](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs#7-12))
Located in [configs/db_config/src/table.rs](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs).
- [TableSpec](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs#14-19): Represents the schema of a table. It links a human-readable `name` (like "customer") to its `file_id` on disk and a list of [ColumnSpec](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs#7-12)s.
- [ColumnSpec](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs#7-12): Represents a single column with its `column_name` and `data_type`. Data types can be `Int32`, `Int64`, `Float32`, `Float64`, or `String`. It also holds statistical data which will be vital for your Query Optimizer in Phase 3.

### C. Database Entry Point ([database/src/main.rs](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/database/src/main.rs))
The [db_main](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/database/src/main.rs#15-89) function is the lifecycle of your database processor.
1. **Load Context:** It parses CLI options and loads `DbContext` (which contains your [TableSpec](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/configs/db_config/src/table.rs#14-19)s for understanding table schemas).
2. **Setup Communication:** It creates standard UNIX pipes for standard I/O communication using `setup_disk_io` and `setup_monitor_io`. This gives you readers and writers connected directly to the `Disk Simulator` and `Monitor` without file-system access.
3. **Parse Initial Query:** It waits for a single line from the monitor, mapping it from JSON directly into the [Query](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#4-7) tree representation.
4. **Communicate with Disk:** It sends string commands (like `get block-size\n`) safely using `Write::write_all` to the disk process' `stdin` pipeline and parses the response. 
5. **Get Memory Boundaries:** Finally, it queries the `Monitor` for the `memory_limit` configured for the user. 
6. (Your Future Work): At the end, there is commented out boilerplate indicating where you'll validate and emit the processed tuples back to the Monitor.

---

## 3. Both Tasks: Verify Setup & Communication Flow

**Goal:** Run the provided Monitor process to simulate a simple [Scan](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/common/src/query.rs#64-67) query and watch how `database.rs` communicates with the disk.

**Execution Plan for Both:**
1. Ensure the P1 task finishes generating the binary dataset first.
2. The Database runs as a subprocess to the Monitor. You will invoke the monitor using this exact command:
```bash
cargo run -r --bin monitor -- --config ./scratch/runtimes/tpch/monitor_config.json 
```
3. Check [database/src/main.rs](file:///Users/abhinavgupta/Documents/IIT_DELHI/Shared/IITD/Sem2/COL7362/Assignment_3/DB55/my-awesome-db-master/database/src/main.rs). Notice the `println!` statements inside? They log parsing steps, showing exactly when the system parses a json query string, retrieves block 0, and receives the memory limit constraint.
4. **Observation:** Ensure you can visually track the initial handshake: Monitor -> Query JSON -> Database -> "get block-size" -> Disk -> "4096" -> Database -> "get memory limit" -> Monitor.

## Next Steps
Once you manually execute the generator (Task 1) and run the monitor against a test scan query (Task 3), you will be officially ready to write Rust for disk I/O handling on Day 3.
