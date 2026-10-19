# Specs

Timelake's design, one numbered spec per part, in the spirit of Rust's RFCs and Python's PEPs. Each spec says what a
part of Timelake does and why, what was measured to decide it, what is still open, and how far it is built. Start
here, people and agents alike: find the spec, read its summary, follow its links.

## Statuses

| Status | Meaning |
|---|---|
| Done | Built and in effect; the spec describes how it works now. |
| In progress | Partly built: the spec says which parts are, and the open parts have issues. |
| Planned | Agreed or proposed, not built yet. |
| Dropped | Measured or discussed and not done; kept so the reasons stay on record. |

## Writing and changing a spec

- **When:** anything beyond a small fix gets a spec, or a change to one, agreed before it is built.
- **New spec:** copy [TEMPLATE.md](TEMPLATE.md) to `NNNN-short-name.md` with the next free number (numbers are never
  reused; a replaced spec is marked Dropped and links its successor).
- **Front matter:** `number`, `title`, `area`, `status` (`done`, `in-progress`, `planned`, `dropped`), `updated` (the
  date of the last change of substance), and optionally `requires`, `related` (spec numbers) and `issues` (issue
  numbers). The table below and the website's spec pages are built from it.
- **Sections:** Summary (a few sentences), Design, Decisions (rows with the next free `D` number, where there are
  any), Open questions and issues, History (dated, newest last).
- **Links:** cite another spec as "spec 21", linked to its file. Measurements go in
  [`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md) and A/B runs in `bench/results/experiments/`;
  the spec states the result and links them.
- **Open questions** are GitHub issues (label `question`), listed under `issues`, not paragraphs in the spec. Tasks
  that aren't designs (bugs, chores, small improvements) are issues too.
- **Same commit:** when work lands, its spec's status, design and history change with the code, the docs and the
  changelog. Then run `uv run python scripts/spec_index.py` (a pre-commit hook checks it).

## Index

<!-- index:start -->

44 specs: 13 done, 15 in progress, 16 planned.

### Foundations

| # | Spec | Status | Updated |
|---:|---|---|---|
| 1 | [Purpose and requirements](0001-purpose.md) | In progress | 2026-10-18 |
| 2 | [Architecture](0002-architecture.md) | In progress | 2026-10-17 |
| 3 | [Iceberg tables and the commit layer](0003-iceberg-and-commits.md) | Done | 2026-10-13 |
| 4 | [Catalog](0004-catalog.md) | In progress | 2026-10-12 |
| 5 | [Embedded engine: DataFusion](0005-engine.md) | Done | 2026-10-12 |
| 6 | [Time-series table contract and layout](0006-table-contract.md) | In progress | 2026-10-19 |
| 7 | [Supplemental timestamps](0007-supplemental-timestamps.md) | Planned | 2026-10-16 |
| 8 | [License and open core](0008-license-and-open-core.md) | In progress | 2026-10-12 |

### Writing

| # | Spec | Status | Updated |
|---:|---|---|---|
| 9 | [Write path, freshness and the hot tier](0009-write-path.md) | In progress | 2026-10-12 |
| 10 | [Backfill](0010-backfill.md) | Planned | 2026-10-11 |
| 11 | [Import and migration](0011-import.md) | In progress | 2026-10-12 |
| 12 | [Late data and dedup](0012-late-data-and-dedup.md) | Done | 2026-10-16 |
| 13 | [Deletes](0013-deletes.md) | Planned | 2026-10-11 |
| 14 | [GDPR erasure](0014-gdpr-erasure.md) | Planned | 2026-10-11 |
| 15 | [Table maintenance and streaming compaction](0015-maintenance.md) | Done | 2026-10-17 |
| 16 | [Writes from WebAssembly](0016-wasm-writes.md) | Done | 2026-10-17 |

### Reading

| # | Spec | Status | Updated |
|---:|---|---|---|
| 17 | [Read paths](0017-read-paths.md) | In progress | 2026-10-17 |
| 18 | [One-request table state and version checks](0018-table-head.md) | Done | 2026-10-17 |
| 19 | [Block cache](0019-block-cache.md) | Done | 2026-10-18 |
| 20 | [Scan tuning](0020-scan-tuning.md) | In progress | 2026-10-19 |
| 21 | [Materialized rollups](0021-rollups.md) | Done | 2026-10-18 |
| 22 | [Signal aggregates](0022-signal-aggregates.md) | Done | 2026-10-19 |
| 23 | [Downsampling with m4](0023-m4.md) | Done | 2026-10-18 |
| 24 | [Function library](0024-function-library.md) | In progress | 2026-10-19 |
| 25 | [Settings](0025-settings.md) | Done | 2026-10-19 |

### Performance and scale

| # | Spec | Status | Updated |
|---:|---|---|---|
| 26 | [Series cardinality](0026-series-cardinality.md) | In progress | 2026-10-19 |
| 27 | [Performance plan](0027-performance-plan.md) | In progress | 2026-10-19 |
| 28 | [Deployment and cost model](0028-deployment-and-cost.md) | In progress | 2026-10-11 |
| 29 | [Multitenancy](0029-multitenancy.md) | Planned | 2026-10-16 |
| 30 | [Versions and releases](0030-versions-and-releases.md) | Done | 2026-10-14 |
| 31 | [v0: a maintenance-free time-series table on S3](0031-v0.md) | Done | 2026-10-12 |

### Benchmarks

| # | Spec | Status | Updated |
|---:|---|---|---|
| 32 | [Benchmark harness: timebench](0032-timebench.md) | In progress | 2026-10-19 |
| 33 | [Industrial IoT benchmark](0033-iiot-benchmark.md) | In progress | 2026-10-15 |
| 34 | [Agent benchmark](0034-agent-benchmark.md) | Planned | 2026-10-15 |

### Services and planned features

| # | Spec | Status | Updated |
|---:|---|---|---|
| 35 | [Ingestion and rules](0035-ingest-and-rules.md) | Planned | 2026-10-11 |
| 36 | [Agent access with permission sets](0036-agent-access.md) | Planned | 2026-10-13 |
| 37 | [Static read models](0037-read-models.md) | Planned | 2026-10-13 |
| 38 | [Data to events](0038-data-to-events.md) | Planned | 2026-10-13 |
| 39 | [Computed metrics](0039-computed-metrics.md) | Planned | 2026-10-13 |
| 40 | [Snowflake connection](0040-snowflake.md) | Planned | 2026-10-13 |
| 41 | [Device sync](0041-device-sync.md) | Planned | 2026-10-13 |
| 42 | [Edge gateway](0042-edge-gateway.md) | Planned | 2026-10-13 |
| 43 | [Server mode](0043-server-mode.md) | Planned | 2026-10-15 |
| 44 | [Real-data beta](0044-real-data-beta.md) | Planned | 2026-10-18 |

<!-- index:end -->

## Decisions

Every decision has an ID and lives in the spec it shapes.

| ID | Decision | Spec |
|---|---|---|
| D1 | Build on Apache Iceberg; no custom table format | [3. Iceberg tables and the commit layer](0003-iceberg-and-commits.md) |
| D2 | Our own Iceberg REST catalog, optional and able to scale to zero | [4. Catalog](0004-catalog.md) |
| D3 | DataFusion as the embedded engine; DuckDB as the main external SQL path | [5. Embedded engine: DataFusion](0005-engine.md) |
| D4 | Any database-backed component is an optional upgrade, never a requirement | [2. Architecture](0002-architecture.md) |
| D5 | User-defined schemas with a time-series table contract | [6. Time-series table contract and layout](0006-table-contract.md) |
| D6 | Deletes are immutable: logical, audited and reversible; purge is separate and explicit | [13. Deletes](0013-deletes.md) |
| D7 | Apache License 2.0 for everything in this repository | [8. License and open core](0008-license-and-open-core.md) |
| D8 | GDPR erasure: an immediate logical delete, then a batched physical purge of every copy | [14. GDPR erasure](0014-gdpr-erasure.md) |
| D9 | Backfill as one command: final layout, staged on a branch, published atomically | [10. Backfill](0010-backfill.md) |
| D10 | `import` = contract inference + create or adopt + backfill | [11. Import and migration](0011-import.md) |
| D11 | Ingestion as a library-first pipeline, deployable embedded, on Lambda, as a container or at the edge | [35. Ingestion and rules](0035-ingest-and-rules.md) |
| D12 | Open core: the lake is open source; historian connectors and managed operations may be premium | [8. License and open core](0008-license-and-open-core.md) |
| D13 | Timelake owns its commit layer on iceberg-rust's public types | [3. Iceberg tables and the commit layer](0003-iceberg-and-commits.md) |
| D14 | Rust for the core; Python as the client layer | [2. Architecture](0002-architecture.md) |
| D15 | timebench: TSBS IoT plus lake extensions, against every alternative | [32. Benchmark harness: timebench](0032-timebench.md) |
| D16 | Parquet stays the source of truth; Vortex only where Timelake owns the read path | [6. Time-series table contract and layout](0006-table-contract.md) |
| D17 | The v0 catalog is Hadoop-catalog compatible: put-if-absent metadata and a version hint | [4. Catalog](0004-catalog.md) |
| D18 | The last-value index is a Parquet sidecar per snapshot, readable by any engine | [6. Time-series table contract and layout](0006-table-contract.md) |
| D19 | Inline maintenance compacts size-tiered; `maintain` rewrites each partition into one file | [15. Table maintenance and streaming compaction](0015-maintenance.md) |
| D20 | DataFusion pinned to 54.1, the newest release on iceberg-rust's Arrow | [5. Embedded engine: DataFusion](0005-engine.md) |
| D21 | Benchmarks include every opponent, publish losses, and report cost and growth | [32. Benchmark harness: timebench](0032-timebench.md) |
| D22 | Descriptive function names with pandas aliases; continuous aggregates are materialized rollups | [24. Function library](0024-function-library.md) |

## Roadmap phases

The phases of the original plan (the website's roadmap is the current view, `dashboard/lib/roadmap.ts`). The planned
features F1–F10 are specs 36 (F1), 37 (F2), 38 (F3), 39 (F4), 40 (F5), 41 (F6), 42 (F7), 43 (F8), 25 (F9) and 16
(F10); the performance steps P1–P10 are listed in spec 27.

| Phase | Deliverable |
|-------|-------------|
| 0 | The spec (then one file, SPEC.md). `timebench` skeleton: TSBS IoT generator → Parquet, adapters for DuckDB+Iceberg and TimescaleDB as first baselines. Spikes: ✅ `iceberg-rust` write maturity (Q7); `replace` + deletion-vector commits via our own commit layer (D13); DataFusion vs. embedded DuckDB (Q12); Lakekeeper evaluation (Q2); Iceberg v3 engine-support survey (Q1). Benchmark harness. |
| 1 | `timelake-core` writer (contract, sorting, partitioning, batching, commits); `timelake-catalog` (object-store backend, REST spec); CLI create/adopt/query; **import** (CSV, Parquet, Iceberg/S3 Tables) and **backfill** (plan, final-layout write, branch staging, publish, resume); DuckDB reads committed tables |
| 2 | `timelake-maintain`: compaction, re-sort, dedup, retention, snapshot expiry, triggered by catalog events. Immutable deletes + audit table; GDPR erasure workflow. **TimescaleDB/Postgres import** with `--sync` cutover. Snowflake connection (F5), first through metadata-file registration, then through the REST catalog. |
| 3 | Rollups (incremental, hierarchical) + `timelake-functions` core (`time_bucket`, `time_buckets`, `forward_fill`, `time_weighted_mean`, `counter_diff`, quantiles) |
| 4 | DuckDB extension (functions, dedup-on-read, index-aware scans) |
| 5 | Hot tier (profile WAL vs. Valkey; ship both behind `HotStore`) + Timelake reader with merge, Puffin indexes, fast paths |
| 6 | `timelake-ingest` with MQTT/HTTP/Kafka and the rule engine + events table; data → events with pluggable sinks and commit-driven triggers (F3); edge gateway (F7) and device sync (F6) |
| 7 | Python package (PyO3 + maturin), docs site with recipes, published benchmarks; Vortex when Iceberg supports it |
| 8 | Agent access with permission sets (F1) and static read models for dashboards (F2) |
| Later | Computed metrics from user-defined Python functions (F4) |

## Background

- [Landscape and positioning](landscape.md): table formats, managed catalogs and time-series databases compared.
- [`PRIOR_ART.md`](../PRIOR_ART.md): what we learned from other projects, and how we credit it.

Until 2026-10-19 all of this was one file, `SPEC.md`; its history is in git.
