---
number: 2
title: Architecture
area: Foundations
status: in-progress
updated: 2026-10-17
related: [3, 4, 5, 9, 17]
---

# 2. Architecture

**In progress** · updated 2026-10-17
· related: [spec 3](0003-iceberg-and-commits.md), [spec 4](0004-catalog.md), [spec 5](0005-engine.md), [spec 9](0009-write-path.md), [spec 17](0017-read-paths.md)

## Summary

How the pieces fit: Parquet on object storage, Iceberg tables, Arrow in memory, DataFusion embedded as the engine. The components (core library, catalog, functions, reader, ingest, maintenance, CLI, Python, DuckDB extension) and which of them run where. No component is a mandatory always-on server.

## Design

| Layer | Technology | Role in Timelake |
|-------|------------|------------------|
| **Object storage** | S3, GCS, Azure Blob, MinIO, R2, local disk | Holds all data and metadata files. Accessed via the Rust `object_store` crate. |
| **File format** | **Apache Parquet** (later Vortex) | Columnar *on-disk* format. Compressed, with per-column statistics (min/max/null counts) and page indexes that let readers skip data. |
| **Table format** | **Apache Iceberg** | Turns a pile of Parquet files into a *table*: schema, partitioning, snapshots (time travel), atomic commits, and file-level statistics in manifests. This is what makes DuckDB/Spark/Trino able to read our data. |
| **Catalog** | **`timelake-catalog`** (ours, [spec 4](0004-catalog.md)) or any Iceberg REST catalog (Lakekeeper, Polaris, Glue, S3 Tables, Nessie, …) | Maps table names to the current metadata file and makes commits atomic. Every commit and every reader goes through it. |
| **In-memory format** | **Apache Arrow** | Columnar *in-memory* format and the lingua franca between systems. Rust, Python (PyArrow/Polars/pandas), and DuckDB can exchange Arrow data **zero-copy**. We use Arrow for every in-flight batch: ingestion buffers, WAL segments (Arrow IPC), query results, and the Python API. |
| **Query engine (embedded)** | **Apache DataFusion** | Rust library containing a SQL parser, planner, optimizer, and vectorized executor that operates on Arrow. It is *not* a server; we embed it. We extend it with custom table providers (our pruning + hot-tier merge), UDFs/UDAFs (time-series functions), and optimizer rules (route queries to rollups). It powers compaction, rollup refresh, in-stream rules, and the Timelake reader. |
| **Iceberg in Rust** | `iceberg-rust` (Apache) | Rust implementation of the Iceberg spec, including catalog clients and a DataFusion integration. Our base for reading and writing Iceberg metadata. |
| **External engines** | DuckDB, Spark, Trino, Snowflake, Athena | Read our Iceberg tables directly. DuckDB is the first-class "plain SQL" path (R1, R10). |

In short, **Parquet** is how data sits on disk, **Iceberg** is how files become tables, **Arrow** is how data moves in memory, and **DataFusion** is the engine we embed to compute on it.


### Architecture overview

```
  Devices / MQTT /     ┌───────────────────────────────────────────────┐
  Kafka / HTTP /  ───► │ timelake-ingest (stateless)                   │
  Arrow Flight         │ decode → validate → transform → rules/alarms  │
                       │ → buffer → WAL append → lake commit           │
                       └──────┬────────────────┬────────────────┬──────┘
                              │ events/alarms  │ recent data    │ Parquet + commit
                              ▼                ▼                ▼
                      sinks (webhook,    ┌───────────┐   ┌──────────────────────┐    ┌─────────────────────┐
                      Kafka, SNS) +      │ Hot tier  │   │ Iceberg tables on    │◄───┤ timelake backfill   │
                      events table       │ (optional)│   │ object storage       │    │ branch → publish    │
                                         │ S3 WAL /  │   │ raw / rollups /      │    │ ◄ files, DBs,       │
                                         │ Valkey /  │   │ events / metadata    │    │   Iceberg tables    │
                                         │ DuckDB/PG │   └──────────▲───────────┘    └─────────────────────┘
                                         └─────▲─────┘              │
                                               │         ┌──────────┴───────────────┐
                                               │         │ timelake-maintain (jobs) │
                                               │         │ compaction, rollups,     │
                                               │         │ retention, erasure, idx  │
                                               │         └──────────────────────────┘
         ┌─────────────────────────────────────┴─────────────────────────────────────┐
         │ Readers                                                                   │
         │  • timelake reader (Rust/Python): lake + hot-tier merge, Puffin index     │
         │    pruning, rollup routing, TS functions                                  │
         │  • DuckDB + timelake extension: the same, inside DuckDB                   │
         │  • Plain DuckDB / Spark / Trino: committed Iceberg data                   │
         └───────────────────────────────────────────────────────────────────────────┘
```

*Not drawn:* the **catalog** (`timelake-catalog` or third-party). Every commit and every reader's table lookup goes through it.

### Components (all Rust)
| Component | Form | Responsibility |
|-----------|------|----------------|
| `timelake-core` | library | Table conventions, writer (sorting, partitioning, file sizing), commit protocol, Puffin indexes, hot-tier trait |
| `timelake-catalog` | library + optional service | Iceberg REST catalog with commit queueing, metadata cache, scan planning, commit events ([spec 4](0004-catalog.md)) |
| `timelake-functions` | library | Engine-agnostic time-series kernels on Arrow ([spec 5](0005-engine.md)) |
| `timelake-query` | library | DataFusion integration: table provider, UDF adapters, rollup query rewrite, hot-tier merge, fast paths |
| `timelake-ingest` | binary / container | Firehose-style ingestion with in-stream rules ([spec 35](0035-ingest-and-rules.md)) |
| `timelake-maintain` | binary / job | Compaction, rollup refresh, retention, snapshot expiry. Runs as cron, k8s CronJob, Lambda, or on commit events |
| `timelake` CLI | binary | Create/adopt tables, backfill, query, inspect, erase, run maintenance |
| `timelake` Python | PyO3 wheel | Query → Arrow / Polars / pandas; write; rollup management |
| DuckDB extension | loadable extension | Primary user-facing SQL path: Timelake-aware scans (indexes, hot tier, dedup) and the function library in DuckDB |

## Decisions

| ID | Decision | Status |
|---|---|---|
| D4 | Any database-backed component (hot store, catalog DB) is an optional performance upgrade, never a requirement | Accepted |
| D14 | Rust for the core, even where we must implement Iceberg operations ourselves. Python is the client/API layer (PyO3), not the engine. | Accepted |

## Open questions and issues

None open.

## History

- 2026-10-11: primer and architecture overview (SPEC.md §4, §6)
- 2026-10-19: moved from SPEC.md into its own spec.
