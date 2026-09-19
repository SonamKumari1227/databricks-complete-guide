# Performance Optimization: Interview Questions

```text
1. Execution Model
2. Data Layout
3. Joins and Skew
4. Caching and Runtime
5. Cluster and Cost
6. Scenario-Based
```

---

# 1. Execution Model

### Explain job, stage, and task.

An action creates a **job**. The job is split into **stages** at each shuffle
boundary. Each stage runs one **task** per partition, and each task occupies one
CPU core.

### Narrow vs wide transformations?

Narrow transformations (`filter`, `select`, `withColumn`) operate within a
partition with no data movement. Wide transformations (`groupBy`, `join`,
`distinct`, `orderBy`) require a shuffle.

### Why is a shuffle expensive?

Data is serialised, written to local disk, transferred over the network, and read
back. It also creates a stage boundary where skew and spill surface.

### What is lazy evaluation and why does it matter for debugging?

Transformations build a plan without executing. Work only happens at an action,
so the "slow line" is the action, not the transformation that caused it.

### How many partitions should you have?

Enough to keep every core busy across roughly 2 to 4 waves of tasks, with
partitions around 128 MB to 1 GB. Too few leaves cores idle; too many adds
scheduling overhead.

### repartition vs coalesce?

`repartition(n)` does a full shuffle, produces evenly sized partitions, and can
increase the count. `coalesce(n)` merges adjacent partitions with no shuffle, can
only reduce, and may leave uneven sizes.

### Why is `coalesce(1)` dangerous on large data?

It funnels the whole dataset through a single task, eliminating parallelism and
often causing OOM or extreme runtime.

### What causes driver OOM vs executor OOM?

Driver OOM typically from `collect()`, `toPandas()`, or an oversized broadcast.
Executor OOM from oversized partitions, skew, a large broadcast held per
executor, or excessive caching.

---

# 2. Data Layout

### How does Delta skip data?

Min/max/null statistics per file, stored in the transaction log for the first 32
columns, let the engine eliminate files without reading them.

### Why might a filter get no data skipping?

The column is beyond the first 32 (no statistics), a function wraps the column in
the predicate, or the data is not physically clustered so every file has an
overlapping value range.

### Liquid clustering vs Z-order vs partitioning?

| | Change keys | Skew | Small files |
|---|---|---|---|
| Partitioning | Full rewrite | Poor | High risk |
| Z-order | Re-run OPTIMIZE | Moderate | Moderate |
| Liquid clustering | `ALTER TABLE` | Good | Low |

Liquid clustering is the default for new tables.

### When is partitioning still correct?

Very large tables with a low-cardinality column most queries filter on, where
each partition holds at least roughly 1 GB.

### What goes wrong if you partition by a high-cardinality column?

Millions of directories each holding a tiny file, so listing metadata costs more
than reading the data, and every query is permanently slow.

### What does OPTIMIZE do, and how do you know you need it?

It compacts small files into larger ones. `DESCRIBE DETAIL` showing a small
average file size (`sizeInBytes / numFiles`) is the signal.

### How do you prevent small files rather than fixing them?

`delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`, longer
streaming trigger intervals, and predictive optimization on managed tables.

### What does VACUUM do and what is the risk?

Removes files no longer referenced by the current version. It destroys time
travel beyond the retention period, and too short a retention can break
concurrent long-running readers.

### What are deletion vectors?

Deleted rows are marked in a side file instead of rewriting whole data files,
making DELETE, UPDATE, and MERGE far faster on large tables.

---

# 3. Joins and Skew

### What join strategies exist?

Broadcast hash, sort-merge, shuffle hash, and broadcast nested loop (a fallback
that usually indicates a missing equality condition).

### When is a broadcast join used?

When one side is under the broadcast threshold, when AQE determines at runtime
that it is small enough, or when explicitly hinted.

### What is data skew and how do you detect it?

One partition holding far more data than the rest. Detect from the task duration
or shuffle-read distribution — a maximum far above the median — then confirm with
a row count per key.

### List skew fixes in the order you would try them.

```text
1. AQE skew join (on by default; tune factor and threshold)
2. Remove null and sentinel keys — the most common real cause
3. Pre-aggregate before joining
4. Salt only the hot keys
5. Change strategy / raise the broadcast threshold
```

### Explain salting.

Append a random suffix to a hot key so it spreads across partitions, and
replicate the other side once per salt value so matches still occur. It costs
replication of the smaller side.

### Why are NULL join keys a problem?

They all hash to the same partition, producing one enormous task, and in an inner
join they cannot match anything, so they should be filtered out first.

### What causes spill and how do you fix it?

A task needing more memory than available. Fix by reducing partition size, fixing
skew, giving more memory per core, and only then scaling the cluster.

### How do you make a MERGE fast?

Add a predicate on the clustering or partition column to the `ON` clause so files
can be skipped, deduplicate the source by key, enable deletion vectors, and keep
the target compacted.

---

# 4. Caching and Runtime

### What are the three kinds of caching?

Delta disk cache (automatic, Parquet on worker SSD), Spark cache (`df.cache()`,
explicit, in executor memory), and the SQL result cache (identical query with
unchanged data).

### When should you not use `df.cache()`?

When the DataFrame is used once, when it does not fit comfortably in memory, or
when the disk cache already makes the underlying read fast.

### What should you do instead of caching a huge intermediate result?

Write it as a Delta table — it survives restarts, is shareable, appears in
lineage, and can be clustered and optimised.

### What does AQE do?

Re-optimises at runtime using actual statistics: coalescing small shuffle
partitions, converting joins to broadcast, and splitting skewed partitions.

### What is Photon and where does it not help?

A vectorised C++ engine accelerating scans, joins, aggregations, and Delta
writes. It does not accelerate Python UDFs or RDD operations, which fall back to
the JVM.

### Why are Python UDFs slow, and what is the alternative?

Each row is serialised to a separate Python process, breaking vectorisation and
Photon. Prefer built-in functions; if custom logic is unavoidable, use a Pandas
UDF, which batches via Arrow.

### Why does `WHERE year(order_date) = 2026` perform badly?

Wrapping the column in a function prevents file-level pruning, so every file must
be read. Use a range predicate on the bare column.

---

# 5. Cluster and Cost

### How do you size a cluster?

Start small, measure runtime and CPU utilisation, double the workers and check
whether runtime roughly halves. When it stops halving, the job is not CPU-bound
and more nodes are wasted.

### When does adding workers not help?

With skew (one long task), I/O-bound work, too few partitions, or a single-writer
bottleneck.

### Fewer large nodes or many small ones?

Fewer large nodes for shuffle-heavy work (less network transfer, better cache
locality); many small nodes for parallel, I/O-bound work.

### How do spot instances affect a job?

Workers can be reclaimed and their tasks re-run. Keep the driver on-demand, use
them for fault-tolerant batch work, and avoid them for hard SLAs.

### What do cluster policies enforce?

Auto-termination, maximum size, allowed node types and runtimes, mandatory cost
tags, and defaults such as spot availability.

### Is Photon always worth it?

No. It roughly doubles the DBU rate and usually more than doubles speed for
SQL-style work, but adds little for UDF-heavy jobs. Compare DBUs consumed, not
wall-clock time alone.

### How do you attribute cost to teams?

`system.billing.usage` combined with cluster tags made mandatory by policy.

### What is usually the single largest cost saving?

Moving scheduled production workloads off all-purpose clusters, and shutting down
compute nobody owns.

---

# 6. Scenario-Based

### A nightly job went from 20 minutes to 3 hours with no code change. Diagnose.

```text
1. Spark UI → which stage grew?
2. Task max vs median → skew?
3. Count rows per join key → a new sentinel value from upstream?
4. DESCRIBE DETAIL → did the target fragment into small files?
5. ANALYZE TABLE → are statistics stale after a big load?
6. Check whether another job now shares the cluster
7. Check whether the data volume genuinely grew

Most common real answer: an upstream change introduced a sentinel key
(-1 or 'UNKNOWN') that now dominates one partition.
```

### A dashboard takes 3 minutes to load. Walk through the fix.

```text
1. Query profile: is it joining silver tables at query time?
   → pre-aggregate into a gold table or materialized view
2. Bytes scanned >> returned → add clustering, push date filters into SQL
3. Warehouse cold start → serverless
4. Queries queued not slow → raise max clusters, not warehouse size
5. Small files on gold → OPTIMIZE and enable auto compaction
6. Twenty tiles each with its own query → consolidate into shared datasets
```

### Your Databricks bill doubled. Investigate.

```text
1. system.billing.usage grouped by SKU, job, cluster, and team tag
2. All-purpose SKU in production? → move to job clusters
3. Autotermination disabled anywhere? → enforce by policy
4. An always-on stream that could run on a schedule? → availableNow
5. A cluster resized upward, or max_workers raised?
6. A DLT pipeline switched to continuous mode?
7. A materialized view that started recomputing fully?
8. Add a daily DBU alert so the next spike is caught in a day
```

### A 2 TB table is queried by date but scans everything. Fix it.

```text
1. Confirm with the query profile: bytes scanned vs returned
2. Check the filter form — year(date) = 2026 defeats pruning
3. Check whether the date column is within the first 32 columns
4. Check the physical layout: is the table clustered by date?
   ALTER TABLE ... CLUSTER BY (order_date, country); OPTIMIZE ...
5. Verify improvement in the profile, not by wall-clock alone
```

### A MERGE into a 5 TB table takes 4 hours. Improve it.

```text
1. Add a predicate on the clustering column to the ON clause so Delta
   can skip files — usually the single biggest win
2. Deduplicate the source by key before merging
3. Enable deletion vectors
4. OPTIMIZE the target if fragmented
5. Consider replaceWhere partition overwrite if each run owns one date
6. Check for skew on the merge key
```

### How would you approach optimising a pipeline you have never seen?

```text
1. Measure: job run history trend, which task dominates
2. Spark UI on the worst task: stage, task distribution, shuffle, spill
3. Check table health: DESCRIBE DETAIL on every table it touches
4. Check the layout: are clustering keys aligned with the filters used?
5. Check the query: filters late, UDFs, unnecessary sorts, SELECT *
6. Only then consider cluster changes
7. Change one thing, re-measure, document the result
```

---

## Rapid-Fire Recap

```text
Action → Job → Stages (split at shuffles) → Tasks (one per partition)
Narrow = no shuffle | Wide = shuffle = expensive

Layout: liquid clustering > Z-order > partitioning
        OPTIMIZE, optimizeWrite, autoCompact, VACUUM, deletion vectors
        stats on the first 32 columns only

Joins: broadcast small side; AQE converts at runtime
Skew:  task max >> median → nulls/sentinels first, then AQE, then salting
Spill: more shuffle partitions → fix skew → more memory per core → bigger cluster

Runtime: AQE + Photon on by default; Python UDFs break both
Caching: disk cache is automatic; df.cache() only if reused + expensive + fits
         otherwise materialize as a Delta table

Cluster: measure before scaling; job clusters in prod; spot for batch;
         policies for autotermination, size caps, and tags

Fix order: layout → query → skew → memory → cluster → architecture
```
