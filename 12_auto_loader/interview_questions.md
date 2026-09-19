# Auto Loader: Interview Questions

```text
1. Fundamentals
2. File Detection
3. Schema Handling
4. Options and Tuning
5. Production
6. Scenario-Based
```

---

# 1. Fundamentals

### What is Auto Loader?

A Structured Streaming source (`format("cloudFiles")`) that incrementally ingests
new files from cloud storage exactly once, with built-in schema inference,
evolution, and rescue.

### What problem does it solve?

Loading only new files without maintaining a processed-files control table,
without listing an entire directory each run, and without breaking when the
source schema changes.

### What are the required configuration pieces?

```text
format("cloudFiles")
cloudFiles.format             → file type
cloudFiles.schemaLocation     → where the schema is stored
load(path)                    → folder to watch
checkpointLocation            → where processed-file state lives
```

### Difference between `schemaLocation` and `checkpointLocation`?

`schemaLocation` stores the inferred and evolving schema with version history.
`checkpointLocation` stores streaming progress, including which files have been
processed. Both must be unique per loader.

### Can Auto Loader run as batch?

Yes — `trigger(availableNow=True)` processes everything available and stops,
giving exactly-once semantics with batch economics. This is the usual production
choice.

### Which file formats are supported?

json, csv, parquet, avro, orc, text, binaryFile, and xml.

### How does it achieve exactly-once?

Offsets recording which files a batch will process are written before processing,
and commits after the Delta write. A crash between the two causes reprocessing of
an uncommitted batch, and the atomic Delta commit prevents duplicates.

### What is the `_metadata` column?

A hidden struct on file sources exposing `file_path`, `file_name`, `file_size`,
and `file_modification_time` — the source of bronze audit columns.

---

# 2. File Detection

### How does Auto Loader discover new files?

Either incremental directory listing (default) or cloud file notifications
through a managed queue.

### How does incremental listing work?

Auto Loader uses lexical ordering of filenames to resume listing from where it
stopped, rather than listing the entire directory each run. Randomly named files
defeat this.

### When should you use notification mode?

When directory listing time becomes a visible part of batch duration — typically
tens of thousands of files per day, or millions of files in the path.

### What cloud resources does notification mode need?

SNS + SQS on AWS, Event Grid + Storage Queue on Azure, Pub/Sub on GCP. Databricks
can create them if permissions allow, or you can point at existing ones.

### What is `cloudFiles.backfillInterval` and why is it critical?

It runs a periodic directory listing even in notification mode, catching files
whose cloud events were lost. Without it, a dropped event means a file is never
ingested and nothing raises an error.

### Does the detection mode determine latency?

Usually not. The trigger dominates. Notification mode mainly reduces listing cost
at scale; it only reduces latency meaningfully with continuous triggers.

### How do you prevent reading partially uploaded files?

`pathGlobFilter` limited to completed extensions, and producers writing to a
temporary path then renaming atomically into the watched folder.

---

# 3. Schema Handling

### How does schema inference work?

On the first run Auto Loader samples files, infers a schema, and persists it in
`schemaLocation` as a versioned history that later runs reuse.

### What are the schema evolution modes?

| Mode | Behaviour |
|------|-----------|
| `addNewColumns` | Fails once, records the new column, resumes on restart |
| `rescue` | Unexpected data goes to `_rescued_data`, never fails |
| `failOnNewColumns` | Fails and blocks until a human updates the schema |
| `none` | Silently ignores new columns — loses data |

### What is `_rescued_data`?

A column capturing fields absent from the schema and values that do not match
their column type, preserving unexpected source changes instead of dropping them.

### Why does `addNewColumns` fail the stream?

Because the schema changed mid-query. The batch never commits, the schema version
is updated, and a restart reprocesses the file with the new column — so it
requires automatic restart to be usable.

### Why set `inferColumnTypes = false` for bronze?

Because evolution can add columns but cannot change an existing column's type.
String typing means a source format change never causes an unrecoverable
ingestion failure; casting happens in silver where failures can be quarantined.

### What are schema hints for?

Pinning specific column types — big integers for IDs, decimals for money,
timestamps — while letting inference handle the rest.

### What is the difference between `schemaEvolutionMode` and `mergeSchema`?

The first governs how the reader reacts to unexpected fields; the second allows
the Delta writer to add columns to the target table. Both are required for
evolution to work end to end.

---

# 4. Options and Tuning

### How do you protect against an enormous first batch?

`cloudFiles.maxFilesPerTrigger` and `cloudFiles.maxBytesPerTrigger`, set from what
one batch can comfortably process rather than the normal arrival rate.

### What does `includeExistingFiles = false` do?

Skips files already present when the stream first starts. It is evaluated only on
the first run of a checkpoint; changing it afterwards has no effect.

### What is the risk of `cloudFiles.maxFileAge`?

It bounds checkpoint state by forgetting old file paths — but a very late
delivery or an archive restore of a forgotten file would then be ingested again.

### Which CSV mode belongs in bronze?

`PERMISSIVE` with a corrupt record column. `DROPMALFORMED` silently discards
rows, violating the bronze guarantee.

### What does `cleanSource` do?

Moves or deletes processed source files after a retention period. `MOVE` is
preferred; `DELETE` is irreversible.

### Which table properties help the bronze target?

`delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`, because
frequent micro-batches otherwise create many small files.

---

# 5. Production

### How should Auto Loader be deployed?

As a scheduled job with `trigger(availableNow=True)` on a job cluster, defined in
Git via Asset Bundles, running as a service principal, with retries and failure
alerts — or triggered by file arrival for lower latency.

### How do you ingest hundreds of tables without hundreds of notebooks?

A control table describing each entity (path, format, hints, target), one generic
parameterised loader notebook, and a For Each task over the active entities.

### Which alerts matter for ingestion?

```text
1. Freshness      → ingestion stopped (job paused, queue broken, vendor silent)
2. _rescued_data  → the source schema changed
3. Volume anomaly → truncated file, duplicate delivery, partial upload
```

Job failure alerts catch none of these.

### How do you reprocess files Auto Loader has already handled?

It will not re-read them. Either read those paths as a batch and merge, or
restart with a new checkpoint and schema location after truncating or restoring
the target.

### How do you control ingestion cost?

Scheduled `availableNow` rather than always-on, small autoscaling clusters
(ingestion is I/O bound), one job covering many entities, notification mode only
when listing dominates, and archiving processed files.

### What makes surgical recovery possible?

`_source_file`, `_ingested_at`, and a batch identifier captured at ingestion, so
one bad delivery can be found and removed precisely.

---

# 6. Scenario-Based

### A vendor re-sends yesterday's file with a new name. What happens?

```text
Auto Loader tracks by FILE PATH, so the new name is treated as new data
and every row is ingested again.

Bronze: duplicates land — correct, bronze is append-only
Silver: MERGE on order_id deduplicates, so downstream is unaffected
Detection: the volume anomaly alert flags the doubled row count

This is precisely why silver must deduplicate by business key rather
than trusting the source.
```

### Ingestion has been running for two years and the checkpoint is huge.

```text
Cause: Auto Loader remembers every processed file path.

Options:
1. cloudFiles.maxFileAge = "1 year" — bounds state, but any file older
   than that reappearing will be re-ingested
2. Archive processed files out of the watched path (cleanSource MOVE)
3. Rotate to a new checkpoint at a clean cutover point, with
   includeExistingFiles=false and modifiedAfter as the boundary

Prefer options 2 and 3; option 1 trades a silent duplication risk for state size.
```

### Files stopped arriving three days ago and nobody noticed.

```text
Why nothing fired: the job ran successfully every 15 minutes and
processed zero files. Success is not the same as progress.

Fix:
✔ Freshness alert on max(_ingested_at) per entity
✔ Expected-delivery-window check per source (vendor SLA)
✔ Log rows_loaded per run to a control table and alert on a zero-run streak
```

### The source added three columns and downstream numbers look wrong.

```text
With rescue mode: the data landed in _rescued_data, nothing broke,
and the rescued-data alert fired.

Action:
1. Inspect what is being rescued
2. Decide deliberately whether the columns matter
3. Add them to the silver SELECT
4. Backfill from bronze — possible because bronze kept everything

With mode "none", the columns would have been dropped silently and
the backfill would be impossible. That is the whole argument for rescue.
```

### Design ingestion for a vendor that drops files at unpredictable times.

```text
Trigger:   file arrival trigger on the landing volume
           wait_after_last_change_seconds → avoid partial multi-file uploads
           min_time_between_triggers_seconds → debounce bursts
Loader:    Auto Loader with availableNow inside the job
Fallback:  a daily scheduled run at a fixed time that alerts if no file
           arrived within the SLA window
Filter:    pathGlobFilter to ignore .tmp and manifest files
Archive:   cleanSource MOVE after 30 days
```

### Compare Auto Loader with COPY INTO.

| | Auto Loader | COPY INTO |
|---|-------------|-----------|
| Model | Streaming source | SQL command |
| Scale | Millions of files | Thousands |
| Tracking | Checkpoint | Delta transaction log |
| Schema evolution | Rich (rescue, hints, versions) | Basic |
| Detection | Listing or notifications | Listing |
| Best for | Ongoing production ingestion | One-off loads, simple SQL pipelines |

```text
Rule: COPY INTO for a simple, occasional load in pure SQL.
Auto Loader for anything continuous or large.
```

---

## Rapid-Fire Recap

```text
format("cloudFiles") + cloudFiles.format + schemaLocation + checkpointLocation

Detection: directory listing (default) | notifications (+ backfillInterval!)
Tracking:  by FILE PATH → re-sent file with a new name = new data

Schema:
inferColumnTypes=false for bronze
schemaEvolutionMode: addNewColumns | rescue | failOnNewColumns | none
_rescued_data = unknown columns + type mismatches → alert on it
write side needs mergeSchema

Throttle: maxFilesPerTrigger | maxBytesPerTrigger
Filter:   pathGlobFilter | includeExistingFiles | partitionColumns
Cleanup:  cleanSource MOVE + retentionDuration

Production:
availableNow + schedule/file-arrival | metadata-driven For Each
alerts: freshness + rescued data + volume anomaly
capture _source_file and _ingested_at or recovery is impossible
```
