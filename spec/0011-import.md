---
number: 11
title: Import and migration
area: Writing
status: in-progress
updated: 2026-10-12
related: [10, 6]
---

# 11. Import and migration

**In progress** · updated 2026-10-12
· related: [spec 10](0010-backfill.md), [spec 6](0006-table-contract.md)

## Summary

`import` infers a table contract from a source, creates the table and loads it through the backfill engine: CSV, Parquet and Iceberg first, then TimescaleDB and Postgres with a `--sync` cutover, then the long tail through DuckDB. CSV and Parquet import with schema inference is built; the rest is planned.

## Design

**`import` = create (or adopt) a table + backfill.**
- `backfill` loads into a table that already exists.
- `import` also *infers the table contract* from the source and creates the table. It then reuses the backfill engine ([spec 10](0010-backfill.md)), so all its guarantees apply: final layout, branch staging, atomic publish, resumable, dry run.

**The simple path**
```bash
timelake import readings.csv --table iot.sensor
```
1. **Inspect:** Sample the source and infer column types, timestamp formats, and time zones.
2. **Propose a contract:**
   - Time column: from the type and column name (`ts`, `time`, `timestamp`, …).
   - Series key: low-cardinality string/int columns that repeat across timestamps.
   - Chunk interval: from data volume and time span.
3. **Show the proposal and an import plan:** row estimate, time range, target layout, and cost.
4. **Confirm**, or pass `--yes` for scripts. Edits can be made inline with flags.
5. **Save** the plan as `import.yaml`. It can be edited, version-controlled, and re-run (`timelake import --plan import.yaml`).

**Sources and their smart defaults**

| Source | How it's read | What gets auto-mapped |
|--------|---------------|-----------------------|
| **CSV / TSV** (local, URLs, object-store globs; gzip/zstd) | Streaming parser, parallel by file and byte range | Header → columns; timestamp format and time-zone detection; optional **unpivot** of wide historian exports (one column per tag → `tag, ts, value`) |
| Parquet, JSON/NDJSON, Arrow IPC | Native readers | Schema as-is |
| **Register in place** (Parquet already in the target layout) | No copy (Iceberg `add_files`) | Validated against the contract |
| **Iceberg tables** (S3 Tables, Glue, any REST catalog) | `iceberg-rust` | Schema, partitioning. Two options: copy + re-layout with `import`, or **in place** with `adopt` ([spec 6](0006-table-contract.md)) |
| Delta Lake, DuckLake, Hive-partitioned Parquet | Native or DuckDB-backed reader | Schema, partition columns |
| **TimescaleDB** | Parallel by **hypertable chunk** (natural time-range units), consistent snapshot, binary `COPY` → Arrow | Hypertable time dimension → `time_column`; compression `segmentby` → `series_key`; `chunk_time_interval` → `chunk_interval`; retention policy → retention. Continuous aggregate definitions are **translated to Timelake rollups** where the functions map; anything unmapped is reported. |
| PostgreSQL / MySQL / SQL Server | Parallel by time-range slices, ADBC / `COPY` | Time column from type; series key proposed |
| InfluxDB (v1/v2 export, v3 Flight SQL) | Per measurement | Measurement → table; tags → `series_key`; fields → columns |
| ClickHouse, QuestDB | Native protocol or HTTP/Arrow | Time and ordering key → contract |
| Prometheus | Remote-read API | Metric + labels → `series_key` |
| Industrial historians* (AVEVA PI, Ignition SQL historian, …) | **Premium** connectors ([spec 8](0008-license-and-open-core.md)); CSV exports work in open source | Tag → series, quality → `quality` |
| Anything DuckDB can read | A DuckDB SQL query as the source | Inferred from result schema |

\*A **historian** is the specialized time-series database that factories and utilities run on-site to record sensor and PLC (programmable logic controller) data. Examples: AVEVA PI (formerly OSIsoft PI), Ignition, Honeywell PHD, AspenTech IP.21. In industrial IoT, historians are the most common source of years of historical data, often exported as CSV.

**Migration with cutover (database sources):**
```bash
timelake import postgres://user@host/factory --hypertable public.sensor_data --table iot.sensor_data
timelake import postgres://user@host/factory --all-hypertables --namespace factory
timelake import ... --sync      # keep tailing new rows by time watermark until cutover
timelake import ... --cutover   # final catch-up, then stop
```
1. A bulk historical copy (parallel, resumable).
2. `--sync` keeps copying rows newer than the last watermark, so the source can stay live during migration. Late updates to old rows on the source are caught by a final range re-check before cutover.
3. Logical-replication CDC (exact row-level changes) is a later enhancement.

**Python:**
```python
lake.import_csv("s3://exports/*.csv", table="iot.sensor", time_column="ts")   # or omit for inference
lake.import_from("postgres://…/factory", hypertable="public.sensor_data", table="iot.sensor_data")
```

**Implementation note:**
- Priority sources get native Rust readers: CSV, Parquet, Iceberg, Postgres/TimescaleDB.
- The long tail goes through an optional DuckDB-backed source (DuckDB's scanners for MySQL, SQLite, Delta, Excel, …). It is an optional CLI feature, so the core keeps no C++ dependency (D3).

## Decisions

| ID | Decision | Status |
|---|---|---|
| D10 | `import` = contract inference + create/adopt + backfill, with smart defaults for CSV, lake formats, and databases (TimescaleDB first) ([spec 11](0011-import.md)) | Accepted |

## Open questions and issues

None open.

## History

- 2026-10-11: D10 accepted (SPEC.md §8.5); Q16 resolved
- 2026-10-12: `timelake import` of CSV and Parquet built
- 2026-10-19: moved from SPEC.md into its own spec.
