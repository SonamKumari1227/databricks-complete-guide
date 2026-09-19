# Structured Streaming: Interview Questions

```text
1. Fundamentals
2. Checkpoints and Fault Tolerance
3. Triggers and Latency
4. Windows and Watermarks
5. Joins
6. Production
7. Scenario-Based
```

---

# 1. Fundamentals

### What is Structured Streaming?

A stream processing engine on Spark SQL that models a stream as an unbounded
table, so the same DataFrame and SQL API works for both batch and streaming.

### How does micro-batch execution work?

Spark periodically collects newly available data into a small batch, runs the
query incrementally, and atomically commits the output together with progress
offsets.

### What is the only API difference from batch?

`spark.read` becomes `spark.readStream`, `df.write` becomes `df.writeStream`,
plus a mandatory checkpoint location.

### What are the output modes?

```text
append   → only new rows; aggregations need a watermark
update   → rows that changed this batch
complete → the entire result table every batch (small state only)
```

### What is `foreachBatch` and why does it matter?

A sink that gives you each micro-batch as an ordinary batch DataFrame, unlocking
MERGE, multi-table writes, and arbitrary sinks. It must be idempotent because a
batch may be retried with the same `batch_id`.

### Stateless vs stateful streaming?

Stateless operations (filter, select, cast) process rows independently with no
memory across batches. Stateful operations (aggregations, joins, deduplication)
keep a state store and require watermarks to stay bounded.

### Why can you not call `.count()` on a streaming DataFrame?

It is unbounded, so there is no final count. Use aggregations within the stream
or inspect `numInputRows` in the progress metrics.

---

# 2. Checkpoints and Fault Tolerance

### What is a checkpoint?

A durable directory holding read offsets, commit records, source metadata, and
operator state, enabling a stream to resume exactly where it stopped.

### How is exactly-once achieved?

Offsets are written before processing; commits after the sink write. On restart,
any batch with offsets but no commit is reprocessed. With a replayable source and
a transactional sink such as Delta, the end result is exactly-once.

### Can two streams share a checkpoint?

No. Each stream requires its own location; sharing corrupts offsets and state.

### Which changes require a new checkpoint?

Changes to stateful operators — grouping keys, join conditions, adding or
removing aggregations — and changes to the source type or the sink table.
Stateless changes such as filters are safe to resume.

### What happens if you delete a checkpoint?

All progress and state are lost. On restart the stream begins from the configured
starting position, which means reprocessing or skipping data.

### What is the state store, and when do you switch to RocksDB?

The store holding cross-batch state. Use the RocksDB provider beyond roughly a
few hundred thousand keys to move state off the JVM heap, and enable changelog
checkpointing to reduce batch duration.

### How do you replay a stream from the beginning?

Read with `startingVersion 0` (Delta) or `startingOffsets earliest` (Kafka),
truncate the target, and use a **new** checkpoint directory.

---

# 3. Triggers and Latency

### What does a trigger control?

How often a micro-batch runs, which determines latency, compute cost, and output
file size.

### List the trigger modes.

```text
processingTime="N"  fixed interval, always on
availableNow=True   process all available data, then stop
default             back-to-back batches
continuous="1s"     experimental, map-only, ~1 ms latency
```

### Why is `availableNow` so useful?

It gives streaming semantics — checkpoints, exactly-once, automatic change
tracking — with batch economics, since the cluster runs only while work exists.
For latency budgets of 5 minutes or more it is far cheaper than an always-on
stream.

### What happens if a batch takes longer than the trigger interval?

The next batch starts immediately after the current one ends. Batches never
overlap, but sustained overrun means the stream falls further behind.

### How do you prevent a huge first batch during a backfill?

Throttle with `maxFilesPerTrigger`, `maxBytesPerTrigger`, or
`maxOffsetsPerTrigger`.

### Why do streaming pipelines create small files?

Each micro-batch writes at least one file per partition. Fix with optimized
writes, auto compaction, scheduled `OPTIMIZE`, and longer trigger intervals.

---

# 4. Windows and Watermarks

### Event time vs processing time?

Event time is when the event occurred (a data column). Processing time is when
Spark handled it. Business aggregations must use event time.

### What is a watermark?

A threshold stating how late events may arrive and still be counted, computed as
the maximum observed event time minus a delay. It finalises windows and releases
state.

### Why does a streaming aggregation need a watermark?

Without one, Spark must keep every window forever in case a late event arrives,
so state grows unbounded, and in append mode it can never decide a window is
final.

### Tumbling vs sliding vs session windows?

Tumbling: fixed, non-overlapping, each event in one window. Sliding: fixed,
overlapping, each event in `size/slide` windows. Session: variable length,
defined by an inactivity gap.

### What happens to data later than the watermark?

It is dropped from the aggregation. Recover it with a periodic batch recompute
over a lookback window.

### How do you choose the watermark delay?

Measure the distribution of `ingested_at - event_time` historically and set it
near the p99, then reconcile the remainder in batch.

### Why might a windowed stream stop emitting results?

The watermark advances from observed data, so if data stops arriving the
watermark freezes and append-mode windows are never finalised.

### `dropDuplicates` vs `dropDuplicatesWithinWatermark`?

The former remembers every key forever, so state grows without bound. The latter
forgets keys once the watermark passes, keeping state bounded.

---

# 5. Joins

### Which streaming joins are supported?

Stream-to-static (stateless) and stream-to-stream inner and outer joins
(stateful, requiring watermarks and time-range conditions).

### Why do stream-stream joins need watermarks and a time constraint?

Matching rows may arrive later, so both sides must be buffered. The watermark and
the time range bound how long rows are retained and let outer joins decide when
to emit nulls.

### Why are outer stream-stream join results delayed?

An unmatched row can only be emitted once the watermark passes its expiry, so the
delay is roughly the watermark plus the join range.

### How do you reduce stream-stream join state?

Shorter watermarks, tighter join ranges, projecting only required columns before
the join, RocksDB with changelog checkpointing, and pre-aggregating one side.

### When is `foreachBatch` + MERGE better than a stream-stream join?

When the requirement is "update this record when a related event arrives". The
Delta table becomes the state store, eliminating watermark tuning and handling
arbitrarily late events.

### How do you join a stream to a dimension with history?

A point-in-time join against an SCD2 table where the event time falls between
`valid_from` and `valid_to`.

---

# 6. Production

### How should a production stream be deployed?

As a job: continuous for always-on streams (auto-restart with backoff), or
scheduled with `availableNow` when the latency budget allows. Never a notebook on
an all-purpose cluster.

### How do you detect a silently dead stream?

Log progress metrics to a Delta table via a `StreamingQueryListener` and alert
when a query stops reporting. Nothing else catches this.

### What metrics matter?

Input versus processed rate, batch duration trend, state row count, rows dropped
by watermark, restart frequency, and target table freshness.

### How do you stop one bad record from killing a stream?

Parse with `from_json` (returns null instead of throwing), quarantine
unparseable rows, and alert on quarantine growth rather than on stream failure.

### How do you survive a 10× traffic spike?

Throttle batch size so each batch stays processable, and enable autoscaling so
capacity grows while the backlog drains.

### How do you deploy a breaking change?

Blue-green: run the new version with a new checkpoint writing to a new target,
compare outputs, then switch consumers and retire the old stream.

### Why does `spark.sql.shuffle.partitions` matter more in streaming?

It is fixed for the life of the query and sets the number of state store
instances, so a bad value degrades every micro-batch permanently.

---

# 7. Scenario-Based

### Design a near-real-time order pipeline from Kafka.

```text
Bronze:  Kafka → Delta, keep raw payload + offsets, stateless,
         processingTime 30s or availableNow every 5 min
Silver:  parse with from_json, quarantine failures, deduplicate within
         the micro-batch, MERGE into silver by order_id
Gold:    windowed aggregation with a measured watermark, update mode + MERGE,
         plus a nightly batch recompute for the late tail
Ops:     continuous job, RocksDB, throttles, metrics logged to Delta, alerts
```

### The stream is falling further behind every hour. Diagnose.

```text
1. Compare inputRowsPerSecond and processedRowsPerSecond — confirm the gap
2. Check batchDuration trend — is it growing?
3. Check state size — unbounded growth means a missing or generous watermark
4. Check for skew in the Spark UI — one task dominating each batch
5. Check spark.sql.shuffle.partitions against cluster size
6. Check whether the sink is the bottleneck (many small files, MERGE on a
   poorly clustered target)
7. Fixes: scale out, RocksDB, tighten watermark, OPTIMIZE the target,
   throttle input to stabilise, or split into parallel streams by key range
```

### Numbers in the streaming dashboard disagree with the daily batch report.

```text
Likely causes:
- Late data dropped by the watermark (check numRowsDroppedByWatermark)
- Streaming uses update mode (non-final) while batch is complete
- Different business filters between the two implementations
- Duplicate events not deduplicated in the streaming path

Resolution:
- Declare the batch table authoritative for reporting
- Reconcile nightly: recompute a lookback window and MERGE into gold
- Document which table answers which question
```

### A stream must be reprocessed after a logic bug shipped three days ago.

```text
1. Stop the stream gracefully
2. DESCRIBE HISTORY on the target, find the last good version
3. RESTORE TABLE target TO VERSION AS OF <n>
4. Start with a NEW checkpoint and an explicit startingVersion/startingOffsets
   from three days ago
5. Use trigger(availableNow=True) so it catches up and stops
6. Verify counts against bronze, then resume the normal continuous job

This works because bronze retained everything and silver writes are idempotent.
```

### You need exactly-once delivery to an external REST API.

```text
Spark guarantees exactly-once only to replayable sources + idempotent sinks.
A REST API is not idempotent by default, so:

✔ Send a deterministic idempotency key per record (order_id + batch_id)
✔ Have the receiver deduplicate on that key
✔ Or write to Delta first, then have a separate consumer push with its own
  offset tracking

Without receiver-side deduplication, the honest answer is at-least-once.
```

### How would you decide between streaming and batch for a new pipeline?

```text
Ask: "what decision changes if this data is 10 minutes old instead of 1?"

Answer "nothing"  → scheduled batch or availableNow, far cheaper
Answer "a real one" → streaming, and size the watermark and trigger to that

Also weigh: operational burden, on-call coverage, and whether the team
can support an always-on system at 3 AM.
```

---

## Rapid-Fire Recap

```text
Stream = unbounded table | micro-batch | same API as batch

Checkpoint = offsets + commits + state → exactly-once with Delta
One checkpoint per stream, never shared, new one for stateful changes

Triggers: processingTime | availableNow (usually best) | default | continuous
Throttles: maxFilesPerTrigger | maxBytesPerTrigger | maxOffsetsPerTrigger

Event time, not processing time
Watermark = max(event_time) - delay → finalises windows, bounds state
withWatermark BEFORE groupBy

Joins: stream-static (free) | stream-stream (watermarks + time range)
Alternative: foreachBatch + MERGE, Delta as the state store

Production: continuous job | metrics to Delta | alert on silence
quarantine bad records | throttle spikes | idempotent writes | documented replay
```
