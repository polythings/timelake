# Specs

Timelake's design, in the spirit of Rust's RFCs and Python's PEPs. Start here, people and agents alike.

- **Specs** (`NNNN-name.md`): one feature each, with the same template. A spec says what the feature does and why, what
  was measured to decide it, what is still open (as GitHub issues), how far it is built, and the releases that changed
  it.
- **Milestones** ([`milestones/`](milestones/README.md)): goals made of specs, such as v0. The roadmap, here and on
  the website, is the list of milestones; a spec can be part of more than one.
- **Background** (`name.md`, listed below the index): what isn't a feature: purpose and requirements, architecture,
  license, releases, deployment and cost, and the landscape.

The website shows all of it: specs at [/spec](https://timelake.dev/spec), milestones at
[/roadmap](https://timelake.dev/roadmap).

## Statuses

| Status | Meaning |
|---|---|
| Done | Built and in effect; the spec describes how it works now. |
| In progress | Partly built: the spec says which parts are, and the open parts have issues. |
| Planned | Agreed or proposed, not built yet. |
| Dropped | Measured or discussed and not done; kept so the reasons stay on record. |

Milestones are done, in progress, planned, or exploring (an idea being looked into, which may not happen).

## Writing and changing a spec

- **When:** anything beyond a small fix gets a spec, or a change to one, agreed before it is built. A spec is one
  feature someone could ask for by name; a theme or a version is a milestone.
- **New spec:** copy [TEMPLATE.md](TEMPLATE.md) to `NNNN-short-name.md` with the next free number (numbers are never
  reused; a replaced spec is marked Dropped and links its successor), and add it to a milestone.
- **Front matter:** `number`, `title`, `area` (one of the groups below), `status` (`done`, `in-progress`, `planned`,
  `dropped`), `updated` (the date of the last change of substance), `versions` (the releases that changed it, oldest
  first; `unreleased` until the release script names the version), and optionally `related` (spec numbers) and
  `issues` (issue numbers).
- **Generated:** each file's title and status line, the index below, the roadmap, and the website's lists come from
  the front matter: run `uv run python scripts/spec_index.py` (a pre-commit hook checks it).
- **Sections:** Summary (a few sentences), Design, Decisions (rows with the next free `D` number, where there are
  any), Open questions and issues, History (dated, newest last).
- **Links:** cite another spec as "spec 21", linked to its file. Measurements go in
  [`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md) and A/B runs in `bench/results/experiments/`;
  the spec states the result and links them.
- **Open questions** are GitHub issues (label `question`), listed under `issues`, not paragraphs in the spec. Tasks
  that aren't designs (bugs, chores, small improvements) are issues too.
- **Same commit:** when work lands, its spec's status, versions, design and history change with the code, the docs and
  the changelog, and its milestone's status when that changes.

## Index

<!-- index:start -->

46 specs: 19 done, 9 in progress, 18 planned. Milestones group them on the [roadmap](milestones/README.md).

### Tables and storage

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 1 | [Iceberg tables](0001-iceberg-tables.md) | Done | 0.1.0 | 2026-10-13 |
| 2 | [Commits](0002-commits.md) | Done | 0.1.0, 0.2.0, unreleased | 2026-10-20 |
| 3 | [Object-store catalog](0003-object-store-catalog.md) | Done | 0.1.0 | 2026-10-12 |
| 4 | [Time-series table contract](0004-table-contract.md) | In progress | 0.1.0, 0.5.0 | 2026-10-18 |
| 5 | [File layout and encodings](0005-file-layout.md) | In progress | 0.1.0, 0.6.0, 0.7.0, unreleased | 2026-10-19 |
| 6 | [Last-value index](0006-last-value-index.md) | In progress | 0.1.0, 0.5.0, unreleased | 2026-10-19 |
| 7 | [One-request table state and version checks](0007-table-head.md) | Done | 0.3.0, 0.6.0, 0.7.0 | 2026-10-17 |
| 8 | [Table format versions and upgrades](0008-format-versions.md) | Done | 0.3.0, 0.4.0, 0.5.0 | 2026-10-14 |
| 9 | [Settings](0009-settings.md) | Done | 0.5.0 | 2026-10-19 |
| 10 | [Supplemental timestamps](0010-supplemental-timestamps.md) | Planned | – | 2026-10-16 |

### Writing

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 11 | [Appends](0011-appends.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased | 2026-10-20 |
| 12 | [Late data and dedup on read](0012-late-data-and-dedup.md) | Done | 0.5.0 | 2026-10-16 |
| 13 | [Writes from WebAssembly](0013-wasm-writes.md) | Done | 0.6.0, unreleased | 2026-10-21 |
| 14 | [Hot tier and seconds-fresh reads](0014-hot-tier.md) | Planned | – | 2026-10-12 |
| 15 | [Backfill](0015-backfill.md) | Planned | – | 2026-10-11 |
| 16 | [Import and migration](0016-import.md) | In progress | 0.1.0 | 2026-10-12 |
| 17 | [Deletes](0017-deletes.md) | Planned | – | 2026-10-11 |
| 18 | [GDPR erasure](0018-gdpr-erasure.md) | Planned | – | 2026-10-11 |

### Maintenance

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 19 | [Compaction](0019-compaction.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0 | 2026-10-17 |
| 20 | [Maintenance runs, retention and expiry](0020-maintenance.md) | In progress | 0.1.0, 0.3.0, 0.5.0, unreleased | 2026-10-20 |

### Reading

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 21 | [Timelake reader](0021-reader.md) | In progress | 0.1.0 | 2026-10-17 |
| 22 | [Block cache](0022-block-cache.md) | Done | 0.5.0, 0.7.0 | 2026-10-18 |
| 23 | [Filter pushdown](0023-filter-pushdown.md) | Done | 0.7.0 | 2026-10-17 |
| 24 | [Pruning with long IN lists](0024-in-list-pruning.md) | Done | 0.7.0 | 2026-10-18 |
| 25 | [Newest-first scans](0025-newest-first.md) | Done | 0.7.0, unreleased | 2026-10-19 |
| 26 | [Scans in series order](0026-series-order.md) | Done | 0.7.0, unreleased | 2026-10-19 |
| 27 | [Materialized rollups](0027-rollups.md) | Done | 0.5.0, 0.7.0, unreleased | 2026-10-20 |

### SQL functions

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 28 | [Function library](0028-function-library.md) | In progress | 0.1.0, 0.7.0, unreleased | 2026-10-20 |
| 29 | [Time buckets and gap filling](0029-gap-filling.md) | Done | 0.1.0, unreleased | 2026-10-19 |
| 30 | [Signal aggregates](0030-signal-aggregates.md) | Done | 0.7.0, unreleased | 2026-10-19 |
| 31 | [Downsampling with m4](0031-m4.md) | Done | 0.7.0 | 2026-10-18 |

### Integrations

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 32 | [REST catalog](0032-rest-catalog.md) | Planned | – | 2026-10-12 |
| 33 | [DuckDB extension](0033-duckdb-extension.md) | Planned | – | 2026-10-12 |
| 34 | [Snowflake connection](0034-snowflake.md) | Planned | – | 2026-10-13 |
| 35 | [Multitenancy](0035-multitenancy.md) | Planned | – | 2026-10-16 |

### Services

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 36 | [Ingestion and rules](0036-ingest-and-rules.md) | Planned | – | 2026-10-11 |
| 37 | [Data to events](0037-data-to-events.md) | Planned | – | 2026-10-13 |
| 38 | [Static read models](0038-read-models.md) | Planned | – | 2026-10-13 |
| 39 | [Computed metrics](0039-computed-metrics.md) | Planned | – | 2026-10-13 |
| 40 | [Agent access with permission sets](0040-agent-access.md) | Planned | – | 2026-10-13 |
| 41 | [Device sync](0041-device-sync.md) | Planned | – | 2026-10-13 |
| 42 | [Edge gateway](0042-edge-gateway.md) | Planned | – | 2026-10-13 |
| 43 | [Server mode](0043-server-mode.md) | Planned | – | 2026-10-15 |

### Benchmarks

| # | Spec | Status | Versions | Updated |
|---:|---|---|---|---|
| 44 | [Benchmark harness: timebench](0044-timebench.md) | In progress | – | 2026-10-20 |
| 45 | [Industrial IoT benchmark](0045-iiot-benchmark.md) | In progress | – | 2026-10-20 |
| 46 | [Agent benchmark](0046-agent-benchmark.md) | Planned | – | 2026-10-15 |

### Background

- [Architecture](architecture.md): How the pieces fit: Parquet on object storage, Iceberg tables, Arrow in memory, DataFusion embedded as the engine. The components (core library, catalog, functions, reader, ingest, maintenance, CLI, Python, DuckDB extension) and which of them run where. No component is a mandatory always-on server.
- [Deployment and cost model](deployment-and-cost.md): The minimum deployment is a bucket, a catalog and the library in the user's app, plus scheduled maintenance; no always-on Timelake server. Cost comes down through fewer, bigger files, rollups and tiered retention.
- [Embedded engine: DataFusion](engine.md): Compaction, rollup refresh, ingest rules and the reader all run queries inside Timelake's own Rust processes, so Timelake embeds a query engine: Apache DataFusion, extended with table providers, optimizer rules and functions. DuckDB stays the main external SQL path; a DuckDB extension is planned.
- [Landscape and positioning](landscape.md)
- [License and open core](license.md): Everything in this repository is Apache 2.0 (decided). What is open source and what could be premium (industrial historian connectors, a managed control plane, enterprise features) is a draft.
- [Purpose and requirements](purpose.md): Timelake is an open-source, serverless time-series lake on Apache Iceberg: frequent small writes, late data, rollups and retention, with every byte readable by DuckDB, Spark, Trino or Snowflake. This page holds the vision, the requirements R1–R27 as stated, and the goals and non-goals the other specs refer to.
- [Releases](releases.md): One version for every package, following Cargo's semver rules, cut by a script with a changelog section and a tag, and published by tag to crates.io, PyPI and npm with trusted publishing. Table format versions and `timelake upgrade` are a feature of their own ([spec 8](0008-format-versions.md)).

<!-- index:end -->

## Decisions

Every decision has an ID and lives in the spec or page it shapes.

| ID | Decision | Where |
|---|---|---|
| D1 | Build on Apache Iceberg; no custom table format | [1. Iceberg tables](0001-iceberg-tables.md) |
| D2 | Our own Iceberg REST catalog, optional and able to scale to zero | [32. REST catalog](0032-rest-catalog.md) |
| D3 | DataFusion as the embedded engine; DuckDB as the main external SQL path | [Embedded engine](engine.md) |
| D4 | Any database-backed component is an optional upgrade, never a requirement | [Architecture](architecture.md) |
| D5 | User-defined schemas with a time-series table contract | [4. Time-series table contract](0004-table-contract.md) |
| D6 | Deletes are immutable: logical, audited and reversible; purge is separate and explicit | [17. Deletes](0017-deletes.md) |
| D7 | Apache License 2.0 for everything in this repository | [License and open core](license.md) |
| D8 | GDPR erasure: an immediate logical delete, then a batched physical purge of every copy | [18. GDPR erasure](0018-gdpr-erasure.md) |
| D9 | Backfill as one command: final layout, staged on a branch, published atomically | [15. Backfill](0015-backfill.md) |
| D10 | `import` = contract inference + create or adopt + backfill | [16. Import and migration](0016-import.md) |
| D11 | Ingestion as a library-first pipeline, deployable embedded, on Lambda, as a container or at the edge | [36. Ingestion and rules](0036-ingest-and-rules.md) |
| D12 | Open core: the lake is open source; historian connectors and managed operations may be premium | [License and open core](license.md) |
| D13 | Timelake owns its commit layer on iceberg-rust's public types | [2. Commits](0002-commits.md) |
| D14 | Rust for the core; Python as the client layer | [Architecture](architecture.md) |
| D15 | timebench: TSBS IoT plus lake extensions, against every alternative | [44. Benchmark harness: timebench](0044-timebench.md) |
| D16 | Parquet stays the source of truth; Vortex only where Timelake owns the read path | [5. File layout and encodings](0005-file-layout.md) |
| D17 | The v0 catalog is Hadoop-catalog compatible: put-if-absent metadata and a version hint | [3. Object-store catalog](0003-object-store-catalog.md) |
| D18 | The last-value index is a Parquet sidecar per snapshot, readable by any engine | [6. Last-value index](0006-last-value-index.md) |
| D19 | Inline maintenance compacts size-tiered; `maintain` rewrites each partition into one file | [19. Compaction](0019-compaction.md) |
| D20 | DataFusion pinned to 54.1, the newest release on iceberg-rust's Arrow | [Embedded engine](engine.md) |
| D21 | Benchmarks include every opponent, publish losses, and report cost and growth | [44. Benchmark harness: timebench](0044-timebench.md) |
| D22 | Descriptive function names with pandas aliases; continuous aggregates are materialized rollups | [28. Function library](0028-function-library.md) |
| D23 | Every benchmarked system runs default (as installed) and tuned (its docs' recommendations, precomputation and tuned queries included) | [44. Benchmark harness: timebench](0044-timebench.md) |

Until 2026-10-19 all of this was one file, `SPEC.md`, with the original plan's phases; on 2026-10-19 the specs were
regrouped one per feature, with milestones in place of the hand-written roadmap. Both are in git history.
