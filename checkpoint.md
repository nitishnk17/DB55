# Assignment 3 — Our Progress Tracker
### Out-of-Core Query Execution

---

## Phase 0 — Get Everything Running

### Stage 0.1 — Build the project
First things first, make sure Rust is installed and working. Run the build and wait for it to finish cleanly. Once done, check that all five binaries got created in the target/release folder — database, disk, monitor, generator, and demo_query_printer. Also make sure sqlite3 is available since we'll need it later to generate expected outputs.

### Stage 0.2 — Generate the dataset
We already have the raw CSV and schema files from the tar extract sitting in scratch/datasets/tpch. Now we need to run the generator on them. This converts everything into the binary format the disk simulator understands, creates a sqlite database we can use for validation, and generates all the config files the monitor needs to wire things together. After it runs, scratch/compiled_datasets/tpch should have the bin files and sqlite.db, and scratch/runtimes/tpch should have the three JSON config files.

### Stage 0.3 — Check the pipeline works end to end
Write a dead simple scan query using demo_query_printer, grab the JSON it prints, and drop it into monitor_config.json as an enabled query. Generate what the correct output should look like using sqlite3. Then fire up the monitor and watch what happens. We're not expecting the database to produce correct results yet — we just want to see all three processes start, talk to each other, and not crash. If that works, Phase 0 is done.

---

## Phase 1 — Build the Foundation

### Stage 1.1 — Disk communication module
We need a dedicated place in the code that handles all communication with the disk simulator. Create disk.rs inside database/src. This file will have functions for everything we need to ask the disk: what is the block size, where does the scratch region start, where does a specific table begin, how many blocks does a table use, read some blocks, write some blocks to scratch. Important — every single command we send must end with a newline and be flushed right away, otherwise the disk simulator will just hang waiting.

### Stage 1.2 — Scratch space manager
The disk simulator gives us a massive anonymous region to use as scratch space during query execution. We need something to manage which block IDs we're using so different parts of the code don't accidentally write over each other. Create scratch_allocator.rs. It keeps a running counter and hands out block IDs when something needs space, and takes them back when that space is no longer needed. The sort operator especially will need this since it writes large amounts of intermediate data to disk.

### Stage 1.3 — Row parser
Table data on disk is just raw bytes — we need to decode them into actual values. Create row.rs. The way blocks work is: the last two bytes of every block tell you how many rows are in it, and then the rows themselves are packed from the very beginning of the block. We read that row count, then walk through the block decoding each column one by one based on the schema — integers are fixed width, strings go until a null byte. We also need to go the other direction and encode rows back into bytes for when we write them to scratch during sorting.

### Stage 1.4 — Buffer pool
This is important. Tables can be several gigabytes but we only have 64 MB. The buffer pool sits between our code and the disk and keeps a small number of blocks loaded in memory at a time. When we ask for a block that isn't loaded, it fetches it from disk. If memory is full, it kicks out the block that hasn't been used for the longest time to make room. We need to be careful not to use the entire 64 MB for the pool — we should leave some room, roughly 14 MB or so, for the sort and join operators to do their work.

---

## Phase 2 — Build the Five Operators

### Stage 2.1 — The common interface
Before writing any operator, define a shared interface that all of them follow. It's one method — next — that returns either the next row or signals that there are no more rows. This is what lets us chain operators together into a tree without each one needing to know what's above or below it.

### Stage 2.2 — Scan
This is the simplest operator. It reads every row from one table on disk and hands them up one at a time. It goes block by block, parses all the rows in each block, and returns them through next. One important thing: instead of reading one block at a time from the disk, read a bunch at once — 16 or so — because each disk call has overhead and batching them saves a lot of time.

### Stage 2.3 — Filter
This operator wraps around any other operator and only lets rows through that match a set of conditions. It keeps calling next on whatever is below it, checks each row, and passes it up only if every condition is satisfied. It needs to handle all six comparison types and work with integers, floats, strings, and also comparisons between two columns in the same row.

### Stage 2.4 — Project
This operator also wraps another operator. As each row comes through it picks out only the columns we want, puts them in the right order, and renames them if needed. That's it. Rows come out in the same order they went in, just with fewer or renamed columns.

### Stage 2.5 — Cross join
This operator takes two child operators and produces every combination of a row from the left with a row from the right. The naive approach of looping through all right rows for each left row works but is very slow because it re-reads the right table over and over. The smarter approach is to load as many left rows as we can fit into memory at once, then do one pass over the right side for that whole batch. Fewer right-side passes means much less disk I/O.

### Stage 2.6 — Sort
This one is the most involved. The data is too big to sort in memory so we use external merge sort. In the first phase we pull rows from the child in chunks that fit in memory, sort each chunk, and write it as a sorted run to scratch space on disk. Once all rows have been processed we have a bunch of sorted runs. In the second phase we merge them — we keep one buffer per run, use a priority queue to always pick the globally smallest row, and emit rows in order. When a buffer runs dry we load the next block from that run. After everything is merged we free the scratch blocks so they can be reused.

---

## Phase 3 — Wire It All Together

### Stage 3.1 — Query tree builder
Create engine.rs. This is the function that takes the JSON query the monitor sends us and builds the actual operator tree from it. It walks the query structure recursively — a Scan node becomes a ScanOp, a Filter node becomes a FilterOp wrapping its child, and so on. Before building anything, ask the monitor for the memory limit so we can decide how to split memory between the buffer pool and the sort and join operations.

### Stage 3.2 — Stream results back to monitor
Now update main.rs to do the full flow. Read the query from the monitor pipe. Ask for the memory limit. Build the operator tree using the engine. Tell the monitor output is starting. Then keep calling next on the root operator and send each row to the monitor formatted as pipe-separated values with a trailing pipe at the end of every row. After the last row send the done signal. Flush everything and exit cleanly.

### Stage 3.3 — Make sure everything validates
Run the monitor with queries that cover each operator type one by one — a plain scan, a filter, a project, a sort, a cross join, and then a complex query that chains all of them together. Every one should come back as a pass. If any fail, compare our output to what sqlite3 produces and find where they diverge.

---

## Phase 4 — Make It Fast

### Stage 4.1 — Read more blocks per disk call
Go back to the scan operator and make it fetch a larger batch of blocks in each disk call rather than one at a time. Do the same wherever the cross join reads the right side. The more data we get per call, the less time we waste on disk overhead.

### Stage 4.2 — Filter as early as possible
Right now filtering happens after rows have been read and passed up the tree. Move that evaluation to happen during the scan, as rows are being read off disk. Even better, if the column statistics tell us that a whole block's worth of data cannot possibly satisfy a filter condition, skip reading that block entirely.

### Stage 4.3 — Use the statistics
The db_config.json has statistics for every column that the generator computed. We should actually use them. If we know one table is much smaller than the other, put it on the left side of the cross join so the right side gets scanned fewer times. If a column is already physically sorted on disk, we can skip the whole external sort or at least reduce it to a single merge pass. If the range of values in a column makes a filter condition impossible to satisfy, skip those blocks.

### Stage 4.4 — Tune the sort
Make each sorted run as big as possible by filling as much memory as we can before writing it to disk — bigger runs means fewer total runs to merge. Merge as many runs at the same time as the buffer allows. Ideally the whole sort should be done in two passes. Make sure scratch blocks are actually being freed after the merge completes and not just piling up.

---

## Phase 5 — Polish and Submit

### Stage 5.1 — Test the tricky cases
Try an empty table and make sure nothing crashes and no rows come out. Try a cross join where one side is empty and confirm the result is also empty. Try sorting when lots of rows share the same value. Check that float values in our output look exactly like what sqlite3 produces for the same query — this needs to match precisely or validation will fail.

### Stage 5.2 — Stay within memory limits
The grading server enforces a hard 64 MB limit. Run our database against the biggest TPCH queries and confirm it never goes over. The buffer pool should be evicting old blocks and not growing forever. Scratch space should be getting freed and not accumulating across queries.

### Stage 5.3 — Clean things up
Go through the code and replace any unwrap calls that could panic if something unexpected happens. Make sure every write to the disk pipe and the monitor pipe is properly flushed — missing a flush is a really common cause of hangs. Do a clean release build and fix any warnings the compiler throws.

### Stage 5.4 — Submit
Do one final clean build to make sure everything compiles from scratch. Upload to the course portal. Check the leaderboard to confirm the entry is there.
