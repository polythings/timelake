---
number: 1
title: Purpose and requirements
area: Foundations
status: in-progress
updated: 2026-10-18
related: [2, 31, 44]
---

# 1. Purpose and requirements

**In progress** · updated 2026-10-18
· related: [spec 2](0002-architecture.md), [spec 31](0031-v0.md), [spec 44](0044-real-data-beta.md)

## Summary

Timelake is an open-source, serverless time-series lake on Apache Iceberg: frequent small writes, late data, rollups and retention, with every byte readable by DuckDB, Spark, Trino or Snowflake. This spec holds the vision, the requirements R1–R27 as stated, and the goals and non-goals the other specs refer to.

## Design

**Timelake is an open-source, serverless time-series lake built on Apache Iceberg.**

Think "S3 Tables, but open source, runs on any object storage, and actually understands time series": frequent small writes, late and out-of-order data, materialized rollups, retention, and a rich time-series function library — while every byte stays as plain Iceberg + Parquet that DuckDB, Spark, Trino, Snowflake, etc. can read directly.

Primary target: **IoT / industrial telemetry.** Also supports observability metrics, financial ticks, and event streams.

The problem we replace: teams today run S3 Tables (or self-managed Iceberg) plus **custom Glue/Spark jobs** to compact, re-sort, downsample, and expire time-series data. Timelake makes that a library + a few stateless jobs with sane defaults.


### Requirements (as stated)

| ID | Requirement |
|----|-------------|
| R1 | Maximum compatibility with existing tools — e.g. query with plain DuckDB. |
| R2 | Build on Apache Iceberg. Open-source alternative to S3 Tables, works on any object storage, specialized for time series (frequent writes, late-arriving data, …). |
| R3 | Parquet as the first file format; keep the door open for Vortex once Iceberg supports it. |
| R4 | Pluggable in-memory cache/hot layer for recent data (Valkey/Redis, DuckDB). |
| R5 | Simple materialized rollups and a rich time-series function library. |
| R6 | Willing to build a custom table format *if justified* — to be decided (see [spec 3](0003-iceberg-and-commits.md)). |
| R7 | Easier to use and faster for time series than DuckLake, S3 Tables, Lance, etc. |
| R8 | Support all time-series shapes; IoT is the primary target. |
| R9 | Explain how Apache Arrow and DataFusion fit (see [spec 2](0002-architecture.md)). |
| R10 | Core/application layer in Rust; Python library for retrieval; plain SQL via DuckDB. |
| R11 | Firehose-like ingestion service with in-stream compute: irregular-data detection, alarm events, etc. |
| R12 | Good documentation with use-case examples (multitenancy, …). |
| R13 | No mandatory server — scale cheaply; replace "S3 Tables + custom Glue jobs". |
| R14 | Provide a retrieval library faster than DuckDB where possible. |
| R15 | Instant data access; batch and continuous ingestion; an optional small managed DB is acceptable, and users can opt out of it in exchange for documented limitations. |
| R16 | Backfilling historical data must be easy. |
| R17 | Deletes are immutable and audited, *and* GDPR-compliant erasure is possible. |
| R18 | Simple import from CSV files, existing stores (Iceberg/S3 Tables, Parquet, …), and databases such as TimescaleDB. |
| R19 | Easy read access to the data for LLM agents, with permission sets built in ([spec 36](0036-agent-access.md)). |
| R20 | Pre-made static read models on the blob store, based on the frontend's Zod schemas, invalidated and regenerated automatically on ingestion, so dashboards for a time range of a device or asset are near instant ([spec 37](0037-read-models.md)). |
| R21 | Data → events: produce any type of event into any event stream system, based on incoming data ([spec 38](0038-data-to-events.md)). |
| R22 | Later: a simple compute model. Users define a Python function and the result it should produce (for example a computed metric), and Timelake applies it automatically, the way it does rollups ([spec 39](0039-computed-metrics.md)). |
| R23 | Connect to Snowflake: Timelake as an optimized time-series store that Snowflake queries in place ([spec 40](0040-snowflake.md)). |
| R24 | Auto sync: IoT devices write to their own local storage and Timelake syncs it to the lake behind the scenes, so devices don't handle how data travels ([spec 41](0041-device-sync.md)). |
| R25 | Edge endpoints that devices send metrics, blob uploads, logs and IoT data to, so devices don't need to know how to reach the cloud; the edge also serves data from a local cache ([spec 42](0042-edge-gateway.md)). |
| R26 | Modular enough that Timelake can also run as a database: a long-lived server on local disk, with the bucket as the durable copy, as an alternative to a managed time-series database. Experiment first ([spec 43](0043-server-mode.md)). |
| R27 | Series cardinality: a typical deployment has about 20,000 devices with 5–20 metrics each (100k–400k series); the top end is millions of devices with hundreds of metrics each (hundreds of millions of series). Everything that grows with the number of series (the last-value index, rollups, join filters, planning) must hold up at both ([spec 26](0026-series-cardinality.md)). |


### Goals and non-goals

### Goals
- **G1 — Open by default:** Data is valid Iceberg (v2, targeting v3). No Timelake software is needed to *read* committed data.
- **G2 — Time-series native:** Time-aware partitioning, sort order, compaction, retention, rollups, late-data handling, dedup.
- **G3 — Serverless and cheap:** All durable state lives in object storage + an Iceberg catalog. Every Timelake component is stateless or embeddable and can run as a library, a cron/k8s job, a Lambda, or a sidecar.
- **G4 — Fresh:** Seconds-level visibility through Timelake readers when a hot tier is enabled; minutes-level when it is not.
- **G5 — Fast on time-series query shapes:** Faster than generic engines on the query shapes that matter (last value, range scan for a few series, bucketed aggregates), because we exploit layout, indexes, and rollups.
- **G6 — Batteries included:** Ingestion, stream rules/alarms, rollups, a Python client, and docs with real-world recipes.

### Non-goals (for now)
- An OLTP database or a general-purpose warehouse.
- Full-text log search or vector search.
- A new query language. SQL is the interface.
- Making *any* always-on service mandatory. `timelake-catalog` is recommended but optional, and Timelake works with third-party Iceberg catalogs at reduced capability ([spec 4](0004-catalog.md)).

## Open questions and issues

None open.

## History

- 2026-10-11: vision, requirements R1–R18, goals (SPEC.md §1–3)
- 2026-10-13: R19–R26 with the planned features
- 2026-10-18: R27, series cardinality
- 2026-10-19: moved from SPEC.md into its own spec.
