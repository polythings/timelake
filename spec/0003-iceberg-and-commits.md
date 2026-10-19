---
number: 3
title: Iceberg tables and the commit layer
area: Foundations
status: done
updated: 2026-10-13
related: [4, 6, 18]
issues: [21, 24, 25, 26, 39, 47, 48, 51]
---

# 3. Iceberg tables and the commit layer

**Done** · updated 2026-10-13
· related: [spec 4](0004-catalog.md), [spec 6](0006-table-contract.md), [spec 18](0018-table-head.md)

## Summary

Iceberg is the source of truth; Timelake adds time-series conventions only through Iceberg's extension points (table properties, snapshot summaries, sidecar files), so other engines keep reading the tables. Timelake owns its commit layer on iceberg-rust's public types, and commits in two steps: everything uploaded in parallel, then one conditional write.

## Design

**Iceberg is the source of truth. We add "Timelake conventions" on top using only Iceberg's extension points.**

Iceberg already provides the mechanisms we need to add time-series intelligence *without breaking other readers*:

| Extension point | What we store there | Other engines |
|-----------------|--------------------|---------------|
| Table properties (`timelake.*`) | Table kind (raw / rollup / events), time column, series key, dedup policy, retention, rollup definitions | Ignore |
| Snapshot summary properties | Ingest watermarks (WAL offsets), changed time ranges per commit, idempotency/batch IDs | Ignore |
| Puffin statistics files | Series→file index, per-series time ranges, "last value" index, sketches | Ignore unknown blobs (*to verify per engine*) |
| Sort order + partition spec | `(series_key…, time_column)` sort; time + bucket partitioning | Use for pruning |
| Sidecar Iceberg tables | Series/device metadata, rollups, events/alarms | Fully readable |

### Why not a custom format (yet)
- **Ecosystem:** Iceberg is readable by DuckDB, Spark, Trino, Flink, Snowflake, BigQuery, Athena, and Databricks. A custom format would forfeit R1.
- The known Iceberg weaknesses for time series are **commit overhead** (each commit writes metadata.json + a manifest list + manifests) and **commit throughput** (optimistic concurrency at the catalog). Both are addressable *around* Iceberg:
  - Batch commits: a single committer per table, committing every N seconds or N MB.
  - A hot tier for sub-commit freshness ([spec 9](0009-write-path.md)).
  - Aggressive manifest compaction and snapshot expiration.
  - Adopting Iceberg v4 improvements (proposed single-file commits / adaptive metadata tree) when they land.
- DuckLake chose a SQL database as its catalog largely to solve small commits and fast metadata. Our hot tier plays the same role *optionally*, and keeps the lake itself pure Iceberg.

### When we would revisit
We revisit if benchmarks show that Iceberg metadata planning dominates latency for typical queries even after Puffin indexes and metadata caching. Even then, the fallback is a **Timelake index layer that accelerates Iceberg**, not a replacement for it.

### Two-step commit (P4, built 2026-10-13)

A commit uploads everything it already knows (data files, manifest, last-value index, manifest list) in parallel, then
makes the one conditional write of `vN+1.metadata.json`. The version hint is updated afterwards, in the background.
Commit p50 went 483 → 243 ms on S3 from a laptop, and the whole v0 ingest 1,307 → 795 s
([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md)).

### iceberg-rust's write support (Q7, answered 2026-10-11)

Append and read are solid and DuckDB-compatible on v2 and v3. Replace, row-delta, delete writers, branch commits,
manifest merging and file cleanup are missing and can't be added from outside the crate, so Timelake implements its
commit layer on iceberg-rust's public types (D13). Details: `spikes/iceberg-write/README.md`.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D1 | Build on Apache Iceberg; no custom table format ([spec 3](0003-iceberg-and-commits.md)) | Accepted |
| D13 | Timelake owns its commit layer (append, replace, row-delta, overwrite, branch commits, rewrite-manifests) on iceberg-rust's public types. Forking and maintaining our own iceberg-rust is acceptable if needed. Upstreaming is welcome but low priority. Iceberg Java, iceberg-cpp, and iceberg-go serve as **reference implementations** to learn from (never to copy) and as **conformance oracles** in tests (Phase 0 finding, `spikes/iceberg-write`) | Accepted |

## Open questions and issues

- [#21](https://github.com/polythings/timelake/issues/21) Commit: an uncertain conditional PUT can delete a committed version's manifests
- [#24](https://github.com/polythings/timelake/issues/24) Tests: upgrade fixtures written by each release
- [#25](https://github.com/polythings/timelake/issues/25) CI: declare compatibility on changes to persisted formats
- [#26](https://github.com/polythings/timelake/issues/26) Tests: crash at every step of a commit
- [#39](https://github.com/polythings/timelake/issues/39) Commit: specify how each pair of concurrent operations resolves
- [#47](https://github.com/polythings/timelake/issues/47) Commit: carry Timelake state forward on every commit, compaction included
- [#48](https://github.com/polythings/timelake/issues/48) Commit: batch-id idempotency ends when snapshots expire
- [#51](https://github.com/polythings/timelake/issues/51) Lake: a dropped and recreated table can take a commit from a stale writer

## History

- 2026-10-11: D1 accepted (SPEC.md §5.1)
- 2026-10-12: D13, own commit layer
- 2026-10-13: two-step commit (P4) built
- 2026-10-19: moved from SPEC.md into its own spec.
