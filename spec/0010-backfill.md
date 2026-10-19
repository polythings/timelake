---
number: 10
title: Backfill
area: Writing
status: planned
updated: 2026-10-11
related: [9, 11, 15]
---

# 10. Backfill

**Planned** · updated 2026-10-11
· related: [spec 9](0009-write-path.md), [spec 11](0011-import.md), [spec 15](0015-maintenance.md)

## Summary

Load months or years of history into a live table with one command: written in the final layout, staged on an Iceberg branch, validated, published atomically, resumable, with rollups refreshed afterwards. Existing rows win on conflict by default.

## Design

Backfill means loading historical data (months to years) into a table, usually *while live ingestion continues*. Common IoT cases:
- Onboarding a site that already has a historian (PI, Ignition, …)
- Migrating from TimescaleDB, InfluxDB, or S3 Tables + Glue
- An edge gateway flushing days of buffered data after an outage
- Reprocessing a range after a calibration fix

**Goals:**
- One command.
- No small files.
- No disruption to live ingestion or queries.
- All-or-nothing visibility.
- Resumable.
- Rollups correct afterwards.
- No false alarms.

**How it works**
1. **Plan:** Read the source and map its columns onto the table contract (time column, series key, types, time zone). Split the work into units of *time chunk × series bucket*.
   - `--dry-run` reports row count, time range, overlap with existing data, rows that fall outside retention, and estimated cost.
2. **Write in the final layout:** Workers write files that are already sorted, sized, and partitioned at the granularity appropriate for the data's age.
   - This bypasses the WAL, the hot tier, and later compaction, so nothing gets written twice.
   - Work units are independent, so the job fans out across threads, processes, or Lambdas.
3. **Stage on an Iceberg branch** (`backfill/<job_id>`), the write-audit-publish pattern. Readers of `main` see nothing yet.
4. **Validate:** Row counts vs. the source, dedup-key conflicts, time bounds, null checks, and optional user SQL assertions.
5. **Publish:** Apply the branch's changes onto the current `main` in one atomic commit.
   - Live ingestion keeps committing meanwhile; append-only commits don't conflict.
   - Very large jobs can publish chunk by chunk (e.g. one month at a time) instead.
6. **Follow-up:** The published time ranges are marked as changed, so the rollups over those ranges are refreshed in bulk, in parallel by chunk.

**Overlap modes** (what happens when backfilled rows meet existing rows)

| Mode | Behavior | Typical use |
|------|----------|-------------|
| `merge` (default) | Dedup by `dedup_key`. Conflicts are resolved at write time. By default **existing rows win** (`prefer = existing`); override per job with `prefer = backfill`. | Migrations, gateway catch-up |
| `replace` | Atomically replaces the covered (series, time range) in one commit. Replaced rows are *logically* deleted ([spec 13](0013-deletes.md)), so it is reversible. | Reprocessing after calibration fixes |
| `fill_gaps` | Inserts only into (series, time range) spans that have no existing data | Patching outages from a secondary source |
| `append` | No overlap checks (fastest) | Loading into an empty table |

**Interactions with the rest of the system**
- **Rules and alarms:** Not evaluated by default.
  - `--evaluate-rules` runs them in historical mode. Resulting events are written with `source = 'backfill'` and are never sent to real-time sinks.
- **Retention:** Rows older than the table's retention are rejected in the dry run unless the job overrides retention.
- **Compaction:** Backfilled files are already compacted. The compactor skips partitions with an unpublished backfill on the same range.
- **Late bulk data from live sources:**
  - **v1:** Manual and opt-in. Users run a backfill for a gateway's buffered data, or mark an ingest stream / batch as `backfill`.
  - **Later:** `timelake-ingest` automatically routes large batches older than a threshold to the backfill writer, avoiding WAL floods and small files in old partitions.
- **Resumable and idempotent:** Job state (units done or pending) is stored in the catalog or object storage. Each work unit has an ID, so `timelake backfill resume <job>` is safe to re-run.

**Sources:** Any source supported by import ([spec 11](0011-import.md)).

**Interfaces:**
```bash
timelake backfill iot.sensor \
  --from 's3://old-historian/export/*.parquet' \
  --map ts=timestamp,device_id=tag_name --tz Europe/Berlin \
  --mode merge --dry-run
timelake backfill status <job>      # progress, throughput, ETA
timelake backfill resume <job>
```
```python
job = lake.table("iot.sensor").backfill(source=df, mode="replace",
                                         start="2024-01-01", end="2025-01-01")
job.wait(); print(job.report())
```
```sql
-- DuckDB extension
CALL timelake_backfill('iot.sensor',
  'SELECT * FROM read_csv(''s3://exports/*.csv'')', mode := 'merge');
```

## Decisions

| ID | Decision | Status |
|---|---|---|
| D9 | Backfill is a first-class operation: one command, final layout on write, staged on an Iceberg branch, atomic publish; existing rows win on conflict by default ([spec 10](0010-backfill.md)) | Accepted |

## Open questions and issues

None open.

## History

- 2026-10-11: D9 accepted (SPEC.md §8.4); Q14 and Q15 resolved
- 2026-10-19: moved from SPEC.md into its own spec.
