---
title: Landscape and positioning
updated: 2026-10-15
---

# Landscape and positioning

This section compares products. What we learned from them, and how we credit it, is in [PRIOR_ART.md](../PRIOR_ART.md).

## Table formats

| | Iceberg (plain) | Delta Lake | Apache Hudi | Apache Paimon | DuckLake | Lance | **Timelake** |
|---|---|---|---|---|---|---|---|
| Steward | Apache | Linux Foundation (Databricks) | Apache (Onehouse) | Apache (Alibaba) | DuckDB Labs | LanceDB | — |
| Core idea | Spec-first open table format | Transaction log on object storage | Streaming upserts, record indexes | LSM tree per bucket; streaming-first | Metadata in a SQL database | Random access for ML / vectors | Iceberg + time-series conventions + optional hot tier |
| Read support (DuckDB / Spark / Trino / warehouses) | Broadest | Broad | Spark/Flink-centric | Flink/Spark-centric; can emit Iceberg metadata | DuckDB-centric | Limited for analytics | Same as Iceberg |
| Frequent small writes | Weak (per-commit metadata cost) | Moderate | Good | **Strong** (LSM) | **Strong** (inlining in catalog DB) | Moderate | Strong *with* hot tier / batched commits |
| Upserts / dedup | Merge-on-read deletes | MERGE, deletion vectors | Native | Native (merge engines) | Yes | Yes | Dedup by `dedup_key` ([spec 12](0012-late-data-and-dedup.md)) |
| Time-series aware layout / compaction | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Materialized rollups | ✗ | ✗ | ✗ | Aggregation merge engine (on write) | ✗ | ✗ | ✓ incremental, late-data aware |
| Needs extra infrastructure | Catalog | Optional catalog | Spark/Flink jobs | Flink/Spark jobs | SQL database | ✗ | Catalog (ours or any); hot store optional |

## Managed catalogs and maintenance services

| | AWS S3 Tables | Cloudflare R2 Data Catalog | Apache Amoro | Lakekeeper / Polaris / Nessie | **Timelake** |
|---|---|---|---|---|---|
| What it is | Managed Iceberg + table buckets | Managed Iceberg catalog on R2 | Open-source "self-optimizing" lakehouse manager (JVM server) | Open-source Iceberg REST catalogs | Open-source library + optional catalog + jobs |
| Storage | AWS only | R2 only | Any | Any | Any |
| Compaction | Generic, automatic (billed) | Generic, automatic | Generic: compaction, dedup, sort | ✗ | **Time-series aware**, tiered partitions |
| Snapshot expiry / cleanup | ✓ | ✓ | ✓ | ✗ | ✓ |
| Late data, rollups, retention by time, GDPR erasure, backfill, ingestion rules | ✗ | ✗ | ✗ | ✗ | ✓ |
| Runs serverless / scale to zero | Managed | Managed | ✗ (resident server) | Server | ✓ |

**Also watch:** RustFS 1.0 (Apache-2.0, Rust, GA 2026-09) is an S3-compatible object store that ships an **Iceberg REST catalog / S3 Tables-style API**. It's a likely integration target (storage + catalog in one binary) and a potential competitor for the "open-source S3 Tables" position.

**Also watch:** IceGate (Apache-2.0, Rust, prototype) is an OpenTelemetry observability lake on Iceberg + Parquet with a DataFusion engine and an object-store Parquet WAL. It shares our stack, not our use case; notes and ideas to adopt are in PRIOR_ART.md.

## Time-series databases (baselines, not lakes)
TimescaleDB, InfluxDB 3, QuestDB, TDengine, GreptimeDB. They are fast and feature-rich, but they own the storage. Long retention is expensive, and the data is not an open table that other engines can read. They are the **performance and ergonomics bar** Timelake is measured against ([spec 44](0044-timebench.md)).

**TimescaleDB on Tiger Cloud is the closest managed alternative.**
- **Tiered storage** (`timescaledb_osm`) moves older chunks to S3, and they stay queryable through the database. It has to be enabled per service in the Console; it can't be enabled over SQL.
- **`timescaledb_lake`**, Tiger's Iceberg integration, is available on Tiger Cloud services (seen at version 1.0.1, 2026-10). It is to be evaluated.

Both keep the database as the entry point, which is the trade-off Timelake avoids. The benchmark of tiered storage was `bench/v0/tsdb_cloud.py` (in git history at `b99aae9`; results in `bench/results/TIMESCALE.md`).

*All Timelake columns are design targets until measured by `timebench` ([spec 44](0044-timebench.md)).*
