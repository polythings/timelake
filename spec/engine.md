---
title: Embedded engine: DataFusion
updated: 2026-10-20
---

# Embedded engine: DataFusion

## Summary

Compaction, rollup refresh, ingest rules and the reader all run queries inside Timelake's own Rust processes, so Timelake embeds a query engine: Apache DataFusion, extended with table providers, optimizer rules and functions. DuckDB stays the main external SQL path; a DuckDB extension is planned.

## Design

**Why we need an embedded engine at all.** Several Timelake components must execute queries *inside our own Rust processes*:
- **Compaction:** k-way merge of sorted files, dedup, re-sort.
- **Rollup refresh:** `GROUP BY time_bucket(…)` over changed ranges.
- **Ingest transforms and rules:** SQL over streaming batches.
- **Server-side scan planning** in the catalog.
- **The Timelake reader** and Python library.

Writing all of that by hand would mean reimplementing a query engine.

**Options considered**

| Option | Pros | Cons |
|--------|------|------|
| **DataFusion** | Native Rust. Arrow-native (no conversions). Extensible at every layer: custom table providers (our pruning + hot-tier merge), optimizer rules (rollup routing), custom physical operators (sorted merge/dedup), UDAFs with state. Streaming execution with memory limits (good for compaction in small Lambdas). Integrates with `iceberg-rust`. Proven in time-series systems (InfluxDB 3, GreptimeDB). Apache governance. | Fast-moving API (major releases are frequent, so we pin versions). Comparable to DuckDB on many benchmarks, not uniformly faster. |
| **Embedded DuckDB** (`duckdb-rs`) | Excellent performance. One engine everywhere. Users already know it. | C++ dependency complicates cross-compiling for edge/ARM/Lambda. Deep customization (scan pruning, optimizer, operators) must happen through C++ extensions. Data crosses a Rust↔C++ boundary. |
| **Polars** | Fast DataFrame engine | DataFrame-first; limited SQL planner extensibility; weak fit for custom scans and streaming SQL rules |
| **Hand-written only** | Maximum speed for specific shapes | Reinventing a query engine |

**Proposal**
1. **DataFusion** is the internal engine for compaction, rollups, rules, the catalog planner, and the Timelake reader.
2. **Hand-written fast paths** (directly on `arrow-rs`/`parquet`) cover the hottest query shapes where we want to beat any general engine: last value, single-series range reads, rollup point lookups.
3. **The DuckDB extension is the primary user-facing SQL path.** Users keep DuckDB; we make it Timelake-aware.
4. **One function implementation, two engines:** time-series kernels live in an engine-agnostic crate, `timelake-functions`, that operates on Arrow arrays. Thin adapters expose them as DataFusion UDFs/UDAFs and as DuckDB extension functions (Rust via the DuckDB C extension API; *verify aggregate-function coverage*).

**Phase 0 spike to confirm:** Implement compaction (merge + dedup) and one rollup refresh in both DataFusion and embedded DuckDB. Compare throughput, memory, binary size, and build complexity.

**Our own table provider, not `iceberg-datafusion`.** iceberg-rust ships a DataFusion integration, `iceberg-datafusion`. As of 0.10.1 its scan:
- is one DataFusion partition, so a table decodes as one serial stream;
- reads through iceberg-rust's `TableScan::to_arrow` and its `FileIO`, not DataFusion's `ParquetSource` over `object_store`, so it gets none of DataFusion's row-group and page pruning, its footer cache, byte-range splitting, statistics or declared sort order, and it bypasses the block cache ([spec 22](0022-block-cache.md));
- prunes only with the filters it can turn into Iceberg predicates.

The reader also needs a provider of its own for dedup on read ([spec 12](0012-late-data-and-dedup.md)), the last-value index rewrite ([spec 6](0006-last-value-index.md)), newest-first and series order ([spec 25](0025-newest-first.md), [spec 26](0026-series-order.md)), IN-list access plans ([spec 24](0024-in-list-pruning.md)), footers taken from the table head ([spec 7](0007-table-head.md)), and scan partitions sized for object storage. Built on `iceberg-datafusion`, we would replace its scan and filter conversion and keep only catalog glue, which Timelake already has (D13). So `TimelakeProvider` (`crates/timelake/src/query/provider.rs`) reads Timelake's manifests and plans DataFusion `ParquetSource` scans itself, and `iceberg-datafusion` is not a dependency.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D3 | DataFusion as the embedded engine; shared function kernels; DuckDB extension as the user-facing SQL path ([embedded engine](engine.md)) | Proposed, pending a Phase 0 spike |
| D20 | v0 pins **DataFusion 54.1**, the newest release on Arrow 58, which iceberg-rust 0.10 requires. Revisit when iceberg-rust moves to Arrow 59+. | Accepted (v0) |
| D24 | The reader uses its own DataFusion `TableProvider` over `ParquetSource` and `object_store`, not `iceberg-datafusion`, whose scan is one partition through iceberg-rust's reader. Revisit if `iceberg-datafusion` plans per-file `ParquetSource` scans over `object_store`. | Accepted (v0) |

## Open questions and issues

None open.

## History

- 2026-10-11: proposed (SPEC.md §5.3)
- 2026-10-12: v0 built on DataFusion 54.1 (D20); Q12 settled by building on it
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 5; a background page since the specs were regrouped one per feature.
- 2026-10-20: D24 records why the reader has its own table provider instead of `iceberg-datafusion` (a v0 choice).
