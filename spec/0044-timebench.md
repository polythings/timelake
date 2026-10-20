---
number: 44
title: Benchmark harness: timebench
area: Benchmarks
status: in-progress
updated: 2026-10-20
versions: []
related: [45, 46]
issues: [64, 81, 90, 94, 132, 137, 138, 139, 140, 147, 148, 149, 150, 151, 152, 153]
---

# 44. Benchmark harness: timebench

**In progress** · updated 2026-10-20 · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Benchmarks against every alternative](milestones/benchmarks.md)
· related: [spec 45](0045-iiot-benchmark.md), [spec 46](0046-agent-benchmark.md)

## Summary

One reproducible suite run against Timelake and every alternative with each system's own tooling: TSBS IoT and DevOps plus lake-specific workloads (streaming ingest, late data, maintenance, cost), local and on object storage, every run kept with its commit, and a weekly regression run.

## Design

One reproducible suite, run against Timelake **and every alternative** in [landscape](landscape.md), using each system's idiomatic tooling.

### Base workload: TSBS IoT, adopted
We adopt the **TSBS IoT use case** (Timescale's Time Series Benchmark Suite) rather than inventing our own. It is the de-facto TSDB benchmark with an IoT scenario, it has published results for TimescaleDB, InfluxDB, QuestDB, and TDengine, and it already models what IoT data looks like:
- **Scenario:** A fleet of trucks.
  - `readings`: 7 metrics + 8 tags
  - `diagnostics`: 3 metrics + 8 tags
  - Nanosecond timestamps, one record per ~10 s
  - **Includes out-of-order and missing data**
- **Scale:** The number of trucks (we use 1k / 10k / 100k).
- **The 12 IoT queries:**

| TSBS query | Shape it exercises |
|---|---|
| `last-loc` | Last value per series |
| `low-fuel`, `high-load` | Latest-state filter across the fleet |
| `stationary-trucks` | Recent-window aggregate per series |
| `long-driving-sessions`, `long-daily-sessions` | Session/state analysis over a window (≈ `state_duration`) |
| `avg-vs-projected-fuel-consumption`, `avg-load` | Group-by aggregate over tags |
| `avg-daily-driving-duration`, `avg-daily-driving-session` | Daily bucketing + sessions |
| `daily-activity` | Uptime/downtime per day (≈ `uptime`) |
| `breakdown-frequency` | Event detection over history |

**Data generation:** Use the TSBS generator (`tsbs_generate_data --use-case=iot`), then convert it once to Parquet as the common input for every system. The data is the same as in published TSBS results, but the method isn't: TSBS loads in bulk with parallel workers and reports throughput, while timebench streams commits and times single queries from fresh processes. Numbers that sit next to published ones need TSBS's own loader and query runner ([spec 44](0044-timebench.md)).

### Lake extensions (what TSBS doesn't cover)
TSBS assumes a database. A lake also has to be judged on these, borrowing ideas from IoTDB-Benchmark and TPCx-IoT (out-of-order ingestion, downsampling):

| Workload | Measures |
|---|---|
| **Streaming ingest** in small batches (1 s / 10 s / 60 s) | Throughput; **freshness** (write → visible to a query); files and metadata created per hour |
| **Bulk load / backfill** of 1 year | Time, cost, final file count, behavior under concurrent live ingest |
| **Late data** at 1% / 10% / 30%, up to 30 days late | Query correctness, small-file growth, rollup correctness after refresh |
| **Maintenance** | Compaction time/cost; query speed before vs. after; snapshot/metadata size over 30 days of commits |
| **Rollups** | Rollup refresh cost; TSBS daily queries answered from rollups vs. raw |
| **Deletes** | Logical delete (device + range) latency; GDPR purge time and bytes rewritten |
| **Extra query shapes** | Time-weighted average with irregular sampling; gap-filled `time_bucket`; ASOF join of two series; raw range read for 1 / 100 series over 1 h / 1 d / 30 d |
| **Storage** | Bytes on disk per raw row (compression), object count |

### Systems under test
| Category | Systems | Writer / engine used |
|---|---|---|
| Timelake | Lake-only, object-store WAL, Valkey hot store (Q5 ([#107](https://github.com/polythings/timelake/issues/107))) | Timelake writer; Timelake reader **and** plain DuckDB |
| Plain Iceberg | Iceberg + REST catalog | PyIceberg / Spark write; DuckDB, Trino read |
| Managed Iceberg | S3 Tables; R2 Data Catalog | Spark/PyIceberg write; **Firehose → S3 Tables** (AWS's recommended streaming path; planned, needs an AWS Paid plan); Athena / DuckDB read |
| Other formats | Delta Lake, Hudi, Paimon, DuckLake, Lance | delta-rs, Spark, Flink, DuckDB, lance |
| No table format | Plain Parquet files per commit, by day (what a Firehose or Kafka Connect S3 sink writes) | pyarrow; DuckDB and DataFusion over a listing |
| TSDB baselines | TimescaleDB, InfluxDB 3 Core, QuestDB, ClickHouse | Their native TSBS loaders; in timebench, each one's recommended client ([spec 45](0045-iiot-benchmark.md)) |

PyIceberg appends are the naive baseline (what v0 compares against today). Published results must also include Iceberg written the way an expert team would (planned): a Flink or Spark streaming sink that batches commits, with scheduled `rewrite_data_files` and sorting; the Kafka Connect Iceberg sink; iceberg-rust; and DuckDB's Iceberg writer. Beating only the naive writer proves too little.

### Default and tuned (decided 2026-10-20, D23)

Every system runs twice, so results neither flatter a system with expert tuning nobody does nor punish it for
defaults nobody keeps:

- **Default:** installed and loaded with its defaults, the way a team's first afternoon with it looks: tables as its
  library or loader creates them, no sort order or indexes chosen, its server settings as shipped.
- **Tuned:** configured as its own docs recommend for the workload, using anything the system offers: layout, sort
  order, indexes, compaction, server settings, and precomputation (Timelake rollups, TimescaleDB continuous
  aggregates, ClickHouse materialized views and projections, QuestDB materialized views, InfluxDB's caches). Where a
  system's precomputation is read by different SQL, it gets a tuned version of the queries, written as its docs show;
  every answer is still checked against the reference.

| System | Default | Tuned |
|---|---|---|
| Timelake | the contract only (time column, series key), default settings | rollups for the workload's bucketed queries, settings for the workload; queries unchanged (routing) |
| Iceberg, S3 Tables | PyIceberg appends, day partitions | a sort order on (series key, time) and sorting compaction; later the expert writer below (#138) |
| Lance | appends | compaction and scalar indexes on the series key and time |
| Parquet files | a file per commit | one sorted file per day |
| TimescaleDB | a hypertable, row store | columnstore (segment by the series key, order by time) and continuous aggregates, read by its tuned queries |
| ClickHouse | MergeTree ordered by time | ordered by (series key, time), monthly partitions, ReplacingMergeTree, materialized views and projections, read by its tuned queries |
| QuestDB | tables as ingestion creates them | indexed SYMBOL keys, dedup keys, materialized views, read by its tuned queries |
| InfluxDB 3 Core | as shipped (a query reads at most 432 files) | the file limit raised, the last value and distinct value caches, read by its tuned queries |

Runs record their tuning; the weekly schedule runs both every week, over more days of the instance. The site shows
one tuning at a time with a switch per section, entries keeping their plain names. The lake systems have both
since 2026-10-20 (a tuned run runs `bench/tunings/<workload>/<system>.sql` after creating the tables, and reads a
query's `[<reader> tuned]` version where it has one); Timelake's tuned IIoT tables have an hourly rollup per tag. The
database servers have both since 2026-10-20 too: TimescaleDB's continuous aggregates, QuestDB's materialized views,
ClickHouse's materialized views (TSBS only: IIoT's rows repeat, and an insert-triggered view would count them twice),
InfluxDB's last value cache. Where rows repeat, every system removes them in both tunings, since the answers are wrong
otherwise. Measured on 7 days of IIoT (bench/results/experiments/2026-10-20), tuned against default after maintenance:
TimescaleDB 378 → 59 ms, QuestDB 376 → 82 ms, ClickHouse 267 → 199 ms; InfluxDB as shipped fails 6 of 11 queries.
TimescaleDB's continuous aggregates miss rows that arrive late into hours already materialized until the next
refresh, so its tuned answers right after ingest are wrong where they read them. Work: #147 (harness), #148 (site), #149 (lake systems), #150 to #153 (TimescaleDB, ClickHouse,
QuestDB, InfluxDB).

### Methodology
- **Same hardware and same object store** for every lake system:
  - Locally: RustFS (or SeaweedFS), with injected latency (e.g. toxiproxy) to approximate S3's per-request latency.
  - Published runs: real S3 in-region, plus S3 Express One Zone and R2. Local object stores don't reproduce S3's request latency, so they are for repeatability only. MinIO's community edition is no longer maintained.
  - **Planned: one published run per popular object store**, each with latency, request counts and $ per month at that store's prices: AWS S3 (Standard, Express One Zone), Cloudflare R2, Tigris, Wasabi, Backblaze B2, Azure Blob, Google Cloud Storage. Stores without request fees (R2, Wasabi) or with different latency (Tigris, Express) change the cost and speed picture most.
- **Object-store compatibility matrix (CI):** S3, GCS, Azure Blob, R2, Tigris, Wasabi, Backblaze B2, RustFS, SeaweedFS, Garage. Each is tested for the features Timelake relies on, especially **conditional writes** (`If-None-Match` / `If-Match`), which the object-store-only catalog and WAL require ([spec 3](0003-object-store-catalog.md), [spec 14](0014-hot-tier.md)).
- **Store notes (measured 2026-10-13 from a laptop, warm connections, [bench/results/PERFORMANCE.md](../bench/results/PERFORMANCE.md)):**
  - **Tigris** answers GET, HEAD, PUT and LIST faster than S3 us-east-2 (small GET 37 vs 67 ms, PUT 80 vs 110 ms), but a request for a key that doesn't exist (HEAD or GET 404) takes about 285 ms, against 55 ms on S3. Timelake's reader asks for `vN+1.metadata.json` on every table load, so on Tigris each cold load paid it. A LIST of that exact name answers the same question in about 40 ms; it is the default check on Tigris since P1 ([spec 7](0007-table-head.md)).
  - **Tigris global buckets** are strongly consistent within a region and eventually consistent across regions; a conditional write is decided by the bucket's leader only with `X-Tigris-Consistent: true`. Writers to one table from different regions need that header or a single-region bucket. To verify in the compatibility matrix.
- **Cold and warm runs reported separately:** cold means no local caches.
- **Results include:** p50/p99 latency, bytes read, **object-store request counts**, and **estimated $** (per query, per GB ingested, per month of storage).
- **Configurations:** Every system's configuration and version is committed, in a default and a tuned form (above), and vendors are invited to review.
- **Harness (built 2026-10-15):** `timebench` in `bench/timebench`, named on its own because it may move to its own repository: only its Timelake adapter knows Timelake. The website explains its method at /benchmarks/timebench. Our own benchmark system, extended over time with new workloads and systems: public benchmarks miss most of what a time-series lake has to do in practice (streaming commits, late data, maintenance, cost), and it isn't limited to Iceberg.
  - A **workload** is data at a named scale, the order its rows arrive in (in stages, each optionally ending at a checkpoint), and its queries (written once with dialect placeholders; a reader-specific version only where its SQL lacks a construct). Now: TSBS IoT (`small`, `standard`, `large`), TSBS DevOps cpu-only (`small`, `standard` = 4,000 hosts) and IIoT ([spec 45](0045-iiot-benchmark.md)).
  - A **system** is one adapter: create tables, append a commit, run its own maintenance, open its readers. Now: Timelake, Iceberg (PyIceberg), S3 Tables, Lance, plain Parquet files, TimescaleDB (any server: a container, the benchmark instance, Tiger Cloud), ClickHouse, QuestDB, InfluxDB 3 Core, and `reference` (the right answers, [spec 45](0045-iiot-benchmark.md)).
  - A **run** streams the workload into each system, then runs every query in a fresh process on every reader at each checkpoint (right after ingest, after maintenance), and checks that row counts match across readers.
  - **Under load (2026-10-15):** fresh readers probe each system while it ingests (query latency under writes, and whether they see every finished commit); then 1, 4 and 16 dashboard clients query at once (queries per second, p50/p95/p99). On AWS, every record carries its cost at S3 list prices; machines are not priced yet.
  - **History:** every run is one file in `bench/results/runs/` with the commit, machine, versions and all measurements. The website charts it per target. Runs from before the harness (v0, Tiger Cloud) were converted.
  - `timebench run tsbs-iot --scale small` reproduces a run; `report` and `history` read them back.
- **Weekly regression run (decided 2026-10-15):** `bench/profiles/weekly.toml` every Monday (and `iiot.toml`, industrial IoT, every Thursday, so each fits in a day: every system on local disk, then the lake systems on S3, since 2026-10-20) on one fixed instance in us-east-2 (c8gd.2xlarge, local NVMe), started by a GitHub Actions workflow through a role limited to that instance. The instance stops itself when done; an hourly job commits finished runs, so a run may take longer than a GitHub job. Same workloads, scales and pinned competitor versions each week, so changes in the history come from Timelake. No benchmark runs in CI.
- **Spike: a CI gate on request and byte counts.** Those counts don't depend on the machine, so they could catch regressions (an extra metadata read, a lost pruning step) on every push. **Measured (2026-10-15, `spikes/request-counts`):** across three identical runs, query request counts were exact or within one or two requests, bytes within 0.5%, and Timelake's ingest within 0.1%. A gate, Timelake only at `small` scale (3–4 minutes), could require exact counts after maintenance (identical in every run) and allow 5% and 3 requests elsewhere. Not built yet: weekly runs only, for now.

### Phase 0 baseline (2026-10-11)
Full results: [`bench/results/BASELINE.md`](../bench/results/BASELINE.md).
- **Coverage:** TSBS IoT (500 trucks × 3 days), TPC-H SF10, and ClickBench (10% subset), on DuckDB, DataFusion, Polars, DuckDB over Iceberg, and TimescaleDB. Local disk, M1 Pro.
- **No engine dominates.** Polars won TSBS and TPC-H, DataFusion won ClickBench, and DuckDB was close behind on all three. DataFusion is weakest on join-heavy TPC-H, which is not Timelake's workload.
- **TimescaleDB vs. lake:**
  - TimescaleDB is **~100× faster on last-value lookups** (2–3 ms vs. 34–317 ms). → The last-value index / hot store is mandatory.
  - The lake engines are **4–44× faster on analytics** and use **14× less storage** (vs. *uncompressed* TimescaleDB).
- **Sorted, day-partitioned Iceberg beat unsorted Parquet** in DuckDB (`last-loc` 2× faster). This validates time-aware layout and compaction.
- **DuckDB pitfall:** `time_bucket(INTERVAL '1 day', timestamptz)` is ~130× slower than `INTERVAL '24 hours'` ([spec 28](0028-function-library.md)).
- **Object storage (RustFS + 20 ms per request, [`OBJECT_STORAGE.md`](../bench/results/OBJECT_STORAGE.md)):**
  - First runs are 2.7–6.4× slower than local disk.
  - Repeat speed depends on in-process caching: DuckDB caches, DataFusion/Polars don't. → The reader cache is a priority.
  - Iceberg metadata adds round trips. → Catalog-side planning matters.
  - The latest-value query downloads ~165 MB.
- **Parquet vs Vortex ([`VORTEX.md`](../bench/results/VORTEX.md)):** Vortex is faster locally (2.4–4× on single-device queries), at parity to slower on object storage (more requests), and not smaller in our layout. → D16.
- **DataFusion (Rust) vs DuckDB on object storage:** DataFusion was faster on first runs (TSBS 0.29 s vs 0.35 s; single-device 0.12 s vs 0.22 s) but slower on repeats (DuckDB caches file data). → D3 plus a Timelake data cache.
- **Parquet filter pushdown** in DataFusion is 1.2–1.6× *slower* on object storage (extra range requests) and slightly faster locally. Enable it per query shape.
- **Real S3 (2026-10-13, laptop → us-east-2, [`S3.md`](../bench/results/S3.md)):**
  - Right after 1,728 five-minute commits, TSBS queries take **1.5 s on Timelake** vs 3.3 s on Lance and 20–22 s on naive Iceberg and **real S3 Tables**, with 65 requests per query vs 1,500–2,800.
  - S3 Tables' managed compaction ran 2.4–2.6 h after ingest. After it, Timelake was still faster: 1.2 s vs 2.4 s on TSBS, 1.0 s vs 6.7 s on single-truck queries.
  - Timelake commits fastest: 0.48 s median, vs 0.99 s for Lance, 1.46 s for S3 Tables and 2.41 s for Iceberg.
  - Every engine is 9–24× slower on S3 than on local disk for first runs. → The reader cache matters.
  - Lance reads stale versions on S3 Express One Zone (unsorted listings); Timelake doesn't.

### Public benchmarks (reviewed 2026-10-15)
We run the public benchmarks that fit a time-series lake, so results compare with published numbers, and say why for the others. The website lists the same, with a methodology page for each benchmark we run or plan (`dashboard/lib/benchmark-catalog.ts`).

| Benchmark | Status | Why |
|---|---|---|
| TSBS IoT | Running | The base workload ([spec 44](0044-timebench.md)). |
| TSBS DevOps / cpu-only | Running (2026-10-15) | The TSBS use case most databases publish; same generator. 4,000 hosts, streamed and as a bulk load (rows per second). |
| TSBS with its own tools | Planned, after F8 (decided 2026-10-15) | timebench uses TSBS's generator and queries but its own method ([spec 44](0044-timebench.md)). For numbers comparable with published TSBS results, run TSBS's loader and query runner against Timelake's server mode through the Postgres wire protocol (F8, [spec 43](0043-server-mode.md)), as for TimescaleDB; or add a Timelake target to TSBS. |
| ClickBench | Planned | A 10% subset runs in development. Comparable numbers need the full dataset on its reference machine (c6a.4xlarge). An official entry waits until the system is more refined (issue #90). Its SQL is CC BY-NC-SA, so it stays out of the repository. |
| RTABench | Planned | Fresh data, selective lookups and joins. Its pre-aggregate queries wait for rollups ([spec 27](0027-rollups.md)). |
| TSM-Bench | Planned | Seven query templates, queries while ingesting, compression. Its repository has no license: ask the authors first. |
| IoT-benchmark (Apache IoTDB) | Planned | Batch size, writer concurrency, out-of-order data and read/write mixes. Its Java adapters need a server endpoint: F8 ([spec 43](0043-server-mode.md)). |
| LST-Bench | Method only | Rounds of appends, compaction and expiry on our own data, charting how queries degrade; not its TPC-DS workload. |
| TS-Benchmark (ICDE 2021) | Not planned | Unmaintained since 2022; its five queries are covered above. |
| TSDBBench / YCSB-TS | Not planned | Unmaintained since 2019. |
| TPCx-IoT | Not planned | TPC rules allow only audited results; 1 KB key-value records don't suit columnar files. |
| STAC-M3 | Not planned | Closed, member-only, paid audits. |
| TSRBench | Not planned | Measures LLM reasoning with the series pasted into the prompt, so storage plays no part. See [spec 46](0046-agent-benchmark.md). |

## Decisions

| ID | Decision | Status |
|---|---|---|
| D15 | `timebench`: TSBS IoT as the base workload plus lake extensions, run against every alternative in [landscape](landscape.md) ([spec 44](0044-timebench.md)) | Accepted |
| D23 | Every system runs in two tunings: default (as installed) and tuned (its docs' recommendations, precomputation included, with tuned queries where the system needs them, answers checked against the reference); the site switches between them per section | Accepted |
| D21 | Benchmarks compare against every opponent (InfluxDB 3, TimescaleDB, ClickHouse, QuestDB, naive Iceberg, Lance), publish where Timelake is worse as well as better, and always report cost per month and behavior as data grows next to latency ([spec 45](0045-iiot-benchmark.md)). | Accepted |

## Open questions and issues

- [#64](https://github.com/polythings/timelake/issues/64) Bench: report write amplification and requests per GB ingested
- [#81](https://github.com/polythings/timelake/issues/81) Bench: a mixed read workload with p50/p90/p99 and hits per cache tier
- [#90](https://github.com/polythings/timelake/issues/90) Submit an official ClickBench entry
- [#94](https://github.com/polythings/timelake/issues/94) Bench: time moving data out of Tiger Cloud and Snowflake
- [#132](https://github.com/polythings/timelake/issues/132) Point queries' first run slower than on 2026-10-16 (one-truck-latest 4 → 8 ms)
- [#137](https://github.com/polythings/timelake/issues/137) Bench: the public benchmarks still to run
- [#138](https://github.com/polythings/timelake/issues/138) Bench: Iceberg written the way an expert team would
- [#139](https://github.com/polythings/timelake/issues/139) Bench: one published run per popular object store, and a compatibility matrix in CI
- [#140](https://github.com/polythings/timelake/issues/140) CI: a gate on request and byte counts
- [#147](https://github.com/polythings/timelake/issues/147) timebench: run every system default and tuned
- [#148](https://github.com/polythings/timelake/issues/148) Site: a Default / Tuned switch on benchmark sections
- [#149](https://github.com/polythings/timelake/issues/149) Bench: default and tuned lake systems (Timelake, Iceberg, S3 Tables, Lance, Parquet)
- [#150](https://github.com/polythings/timelake/issues/150) Bench: default and tuned TimescaleDB, with its tuned queries
- [#151](https://github.com/polythings/timelake/issues/151) Bench: default and tuned ClickHouse, with its tuned queries
- [#152](https://github.com/polythings/timelake/issues/152) Bench: default and tuned QuestDB, with its tuned queries
- [#153](https://github.com/polythings/timelake/issues/153) Bench: default and tuned InfluxDB 3 Core, with its tuned queries

## History

- 2026-10-11: Phase 0 baseline (SPEC.md §17.5)
- 2026-10-15: timebench built; public benchmarks reviewed
- 2026-10-19: every A/B experiment archived with an index
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 32; specs regrouped one per feature, with milestones on the roadmap.
- 2026-10-20: the weekly industrial IoT run also runs the lake systems on S3.
- 2026-10-20: default and tuned runs of every system decided (D23); work in #147–#153.
- 2026-10-20: default and tuned lake systems (#149): tuning statements and tuned queries in the harness.
- 2026-10-20: default and tuned database servers (#150–#153), measured on IIoT.
