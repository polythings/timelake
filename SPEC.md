# Timelake — Specification

> **Status:** Draft v0.8 — v0 implementation in progress (§19.0)
> **Last updated:** 2026-10-15
> **Scope of this document:** product requirements, architecture, and the key design decisions (with open questions). Not an implementation guide yet.

### Decision log
| ID | Decision | Status |
|----|----------|--------|
| D1 | Build on Apache Iceberg; no custom table format (§5.1) | **Accepted** |
| D2 | Ship our own Iceberg REST catalog, `timelake-catalog`, that is optional and scales to zero (§5.2) | **Accepted** |
| D3 | DataFusion as the embedded engine; shared function kernels; DuckDB extension as the user-facing SQL path (§5.3) | Proposed, pending a Phase 0 spike |
| D4 | Any database-backed component (hot store, catalog DB) is an optional performance upgrade, never a requirement | **Accepted** |
| D5 | User-defined schemas with a time-series table contract (§7) | **Accepted** |
| D6 | Deletes are immutable: logical, audited, and reversible; physical purge is a separate explicit operation (§9.2) | **Accepted** |
| D7 | License: **Apache License 2.0** for everything in this repository. Apache 2.0's explicit patent grant suits infrastructure that companies embed, and it matches Iceberg, Arrow, DataFusion and Parquet. The repository is public (2026-10-12). AGPLv3 + CLA and FSL were considered to keep cloud vendors from hosting it, and rejected: an open-format, no-lock-in project wins by adoption (the PostgreSQL/Iceberg/DuckDB path), late restrictions are answered by forks (Valkey, OpenSearch), and an API can be reimplemented regardless of license (DocumentDB). Protection comes from the trademark, being the reference implementation, and a hosted service plus premium add-ons (§20). | **Accepted** |
| D8 | GDPR erasure is supported: immediate logical delete, then a batched physical purge (daily by default, `--now` on demand, alert past 7 days), covering every copy of the data (§9.3). No legal hold / WORM in v1. | **Accepted** |
| D9 | Backfill is a first-class operation: one command, final layout on write, staged on an Iceberg branch, atomic publish; existing rows win on conflict by default (§8.4) | **Accepted** |
| D10 | `import` = contract inference + create/adopt + backfill, with smart defaults for CSV, lake formats, and databases (TimescaleDB first) (§8.5) | **Accepted** |
| D11 | Ingestion is a library-first pipeline (ingest + rules) deployable embedded, as AWS Lambda, as a container, or at the edge (§13) | **Accepted** |
| D12 | Open core: the lake, ingestion, rollups, import of open formats/DBs, and the DuckDB extension are open source. Industrial historian connectors are a **premium** offering (§20). | Proposed |
| D13 | Timelake owns its commit layer (append, replace, row-delta, overwrite, branch commits, rewrite-manifests) on iceberg-rust's public types. Forking and maintaining our own iceberg-rust is acceptable if needed. Upstreaming is welcome but low priority. Iceberg Java, iceberg-cpp, and iceberg-go serve as **reference implementations** to learn from (never to copy) and as **conformance oracles** in tests (Phase 0 finding, `spikes/iceberg-write`) | **Accepted** |
| D14 | Rust for the core, even where we must implement Iceberg operations ourselves. Python is the client/API layer (PyO3), not the engine. | **Accepted** |
| D15 | `timebench`: TSBS IoT as the base workload plus lake extensions, run against every alternative in §16 (§17) | **Accepted** |
| D16 | Parquet stays the lake's source of truth (Iceberg-compatible). Vortex is used where Timelake owns the read path and data is local: reader SSD cache, hot tier, edge. Re-evaluate when Iceberg supports Vortex files or Vortex object-store I/O matures (`bench/results/VORTEX.md`). | Proposed |
| D17 | v0 catalog layout is Hadoop-catalog compatible: `<table>/metadata/vN.metadata.json` committed with put-if-absent, plus `version-hint.text`. Plain DuckDB `iceberg_scan('<table dir>')` and Spark's HadoopCatalog read tables with no catalog service. A REST endpoint can serve the same layout later (§5.2). | **Accepted (v0)** |
| D18 | The last-value index is a per-snapshot **Parquet sidecar** referenced from the snapshot summary (`timelake.last-value-index`), not a Puffin blob: any engine can read it, and it is rewritten incrementally on each append (previous index ⊕ newest rows of the batch). | **Accepted (v0)** |
| D19 | Inline maintenance uses **size-tiered compaction** (merge ≥ N files of similar size within a partition) to keep write amplification logarithmic under micro-batches. Explicit `maintain` rewrites each partition into one sorted, deduplicated file. | **Accepted (v0)** |
| D20 | v0 pins **DataFusion 54.1**, the newest release on Arrow 58, which iceberg-rust 0.10 requires. Revisit when iceberg-rust moves to Arrow 59+. | **Accepted (v0)** |
| D21 | Benchmarks compare against every opponent (InfluxDB 3, TimescaleDB, ClickHouse, QuestDB, naive Iceberg, Lance), publish where Timelake is worse as well as better, and always report cost per month and behavior as data grows next to latency (§17.6). | **Accepted** |
| D22 | Every time-series function has a descriptive name, and its pandas name as an alias where that differs (`forward_fill` / `ffill`, `value_at` / `asof`); "continuous aggregates" are called **materialized rollups** (§11) | **Accepted** |

---

## 1. Vision

**Timelake is an open-source, serverless time-series lake built on Apache Iceberg.**

Think "S3 Tables, but open source, runs on any object storage, and actually understands time series": frequent small writes, late and out-of-order data, materialized rollups, retention, and a rich time-series function library — while every byte stays as plain Iceberg + Parquet that DuckDB, Spark, Trino, Snowflake, etc. can read directly.

Primary target: **IoT / industrial telemetry.** Also supports observability metrics, financial ticks, and event streams.

The problem we replace: teams today run S3 Tables (or self-managed Iceberg) plus **custom Glue/Spark jobs** to compact, re-sort, downsample, and expire time-series data. Timelake makes that a library + a few stateless jobs with sane defaults.

---

## 2. Requirements (as stated)

| ID | Requirement |
|----|-------------|
| R1 | Maximum compatibility with existing tools — e.g. query with plain DuckDB. |
| R2 | Build on Apache Iceberg. Open-source alternative to S3 Tables, works on any object storage, specialized for time series (frequent writes, late-arriving data, …). |
| R3 | Parquet as the first file format; keep the door open for Vortex once Iceberg supports it. |
| R4 | Pluggable in-memory cache/hot layer for recent data (Valkey/Redis, DuckDB). |
| R5 | Simple materialized rollups and a rich time-series function library. |
| R6 | Willing to build a custom table format *if justified* — to be decided (see §5). |
| R7 | Easier to use and faster for time series than DuckLake, S3 Tables, Lance, etc. |
| R8 | Support all time-series shapes; IoT is the primary target. |
| R9 | Explain how Apache Arrow and DataFusion fit (see §4). |
| R10 | Core/application layer in Rust; Python library for retrieval; plain SQL via DuckDB. |
| R11 | Firehose-like ingestion service with in-stream compute: irregular-data detection, alarm events, etc. |
| R12 | Good documentation with use-case examples (multitenancy, …). |
| R13 | No mandatory server — scale cheaply; replace "S3 Tables + custom Glue jobs". |
| R14 | Provide a retrieval library faster than DuckDB where possible. |
| R15 | Instant data access; batch and continuous ingestion; an optional small managed DB is acceptable, and users can opt out of it in exchange for documented limitations. |
| R16 | Backfilling historical data must be easy. |
| R17 | Deletes are immutable and audited, *and* GDPR-compliant erasure is possible. |
| R18 | Simple import from CSV files, existing stores (Iceberg/S3 Tables, Parquet, …), and databases such as TimescaleDB. |
| R19 | Easy read access to the data for LLM agents, with permission sets built in (§21.1). |
| R20 | Pre-made static read models on the blob store, based on the frontend's Zod schemas, invalidated and regenerated automatically on ingestion, so dashboards for a time range of a device or asset are near instant (§21.2). |
| R21 | Data → events: produce any type of event into any event stream system, based on incoming data (§21.3). |
| R22 | Later: a simple compute model. Users define a Python function and the result it should produce (for example a computed metric), and Timelake applies it automatically, the way it does rollups (§21.4). |
| R23 | Connect to Snowflake: Timelake as an optimized time-series store that Snowflake queries in place (§21.5). |
| R24 | Auto sync: IoT devices write to their own local storage and Timelake syncs it to the lake behind the scenes, so devices don't handle how data travels (§21.6). |
| R25 | Edge endpoints that devices send metrics, blob uploads, logs and IoT data to, so devices don't need to know how to reach the cloud; the edge also serves data from a local cache (§21.7). |
| R26 | Modular enough that Timelake can also run as a database: a long-lived server on local disk, with the bucket as the durable copy, as an alternative to a managed time-series database. Experiment first (§21.8). |
| R27 | Series cardinality: a typical deployment has about 20,000 devices with 5–20 metrics each (100k–400k series); the top end is millions of devices with hundreds of metrics each (hundreds of millions of series). Everything that grows with the number of series (the last-value index, rollups, join filters, planning) must hold up at both (§19.1 P10). |

---

## 3. Goals and non-goals

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
- Making *any* always-on service mandatory. `timelake-catalog` is recommended but optional, and Timelake works with third-party Iceberg catalogs at reduced capability (§5.2).

---

## 4. Primer: how the pieces fit (R9)

| Layer | Technology | Role in Timelake |
|-------|------------|------------------|
| **Object storage** | S3, GCS, Azure Blob, MinIO, R2, local disk | Holds all data and metadata files. Accessed via the Rust `object_store` crate. |
| **File format** | **Apache Parquet** (later Vortex) | Columnar *on-disk* format. Compressed, with per-column statistics (min/max/null counts) and page indexes that let readers skip data. |
| **Table format** | **Apache Iceberg** | Turns a pile of Parquet files into a *table*: schema, partitioning, snapshots (time travel), atomic commits, and file-level statistics in manifests. This is what makes DuckDB/Spark/Trino able to read our data. |
| **Catalog** | **`timelake-catalog`** (ours, §5.2) or any Iceberg REST catalog (Lakekeeper, Polaris, Glue, S3 Tables, Nessie, …) | Maps table names to the current metadata file and makes commits atomic. Every commit and every reader goes through it. |
| **In-memory format** | **Apache Arrow** | Columnar *in-memory* format and the lingua franca between systems. Rust, Python (PyArrow/Polars/pandas), and DuckDB can exchange Arrow data **zero-copy**. We use Arrow for every in-flight batch: ingestion buffers, WAL segments (Arrow IPC), query results, and the Python API. |
| **Query engine (embedded)** | **Apache DataFusion** | Rust library containing a SQL parser, planner, optimizer, and vectorized executor that operates on Arrow. It is *not* a server; we embed it. We extend it with custom table providers (our pruning + hot-tier merge), UDFs/UDAFs (time-series functions), and optimizer rules (route queries to rollups). It powers compaction, rollup refresh, in-stream rules, and the Timelake reader. |
| **Iceberg in Rust** | `iceberg-rust` (Apache) | Rust implementation of the Iceberg spec, including catalog clients and a DataFusion integration. Our base for reading and writing Iceberg metadata. |
| **External engines** | DuckDB, Spark, Trino, Snowflake, Athena | Read our Iceberg tables directly. DuckDB is the first-class "plain SQL" path (R1, R10). |

In short, **Parquet** is how data sits on disk, **Iceberg** is how files become tables, **Arrow** is how data moves in memory, and **DataFusion** is the engine we embed to compute on it.

---

## 5. Key decisions

## 5.1 D1: Iceberg vs. a custom table format (R2, R6) — **Accepted: Iceberg**

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
  - A hot tier for sub-commit freshness (§8).
  - Aggressive manifest compaction and snapshot expiration.
  - Adopting Iceberg v4 improvements (proposed single-file commits / adaptive metadata tree) when they land.
- DuckLake chose a SQL database as its catalog largely to solve small commits and fast metadata. Our hot tier plays the same role *optionally*, and keeps the lake itself pure Iceberg.

### When we would revisit
We revisit if benchmarks show that Iceberg metadata planning dominates latency for typical queries even after Puffin indexes and metadata caching. Even then, the fallback is a **Timelake index layer that accelerates Iceberg**, not a replacement for it.

## 5.2 D2: Our own catalog, `timelake-catalog` — **Accepted**

A catalog alone doesn't make data faster to scan: file layout, compaction, and indexes do that, and they work with any catalog. What a catalog we control *does* optimize is everything around those:

| Lever | What `timelake-catalog` does | Without it (third-party catalog) |
|-------|------------------------------|----------------------------------|
| **Commit throughput** | Queues commits per table server-side and can coalesce concurrent append-only commits into one snapshot. No retry storms from optimistic concurrency (OCC) conflicts. | Writers retry on conflict; need a single committer per table |
| **Metadata latency** | Caches table metadata, manifests, and Puffin indexes in memory | Every reader re-fetches from object storage |
| **Server-side scan planning** | Implements the Iceberg REST scan-planning endpoints using our series/time indexes, so any engine that delegates planning gets Timelake pruning | Engines only use min/max statistics |
| **Maintenance triggers** | Emits commit events that drive compaction and rollup refresh as work arrives, with no polling or schedules | Cron schedules |
| **Timelake state** | Stores hot-tier watermarks, rollup registry, retention policies, delete audit log, tenant configuration | Stored in table properties / snapshot summaries (works, but slower to query) |
| **Security / multitenancy** | AuthN/Z, scoped credential vending (STS / presigned URLs), per-tenant quotas | Whatever the third-party catalog offers |

**Design constraints (to stay true to R13):**
- **Implements the standard Iceberg REST spec.** DuckDB, Spark, Trino, PyIceberg, and Snowflake attach unchanged.
- **Stateless process:**
  - Default backend is *object storage only*: the current-metadata pointer is swapped with conditional writes, so no database is needed.
  - Postgres is an optional backend for very high commit rates or many tables.
- **Scale-to-zero friendly:** Deployable as a Lambda / Cloud Run function, a container, or **in-process as a library** for single-node and edge setups.
- **Optional:** All Timelake features degrade gracefully on S3 Tables, Glue, Polaris, or Lakekeeper (right-hand column above).

**Build vs. extend:** Lakekeeper is an Apache-2.0 Iceberg REST catalog written in Rust. In Phase 0 we evaluate extending or forking it versus building a focused catalog on `iceberg-rust`. *Open question Q2.*

## 5.3 D3: Why an embedded engine, and why DataFusion — **Proposed**

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

---

## 6. Architecture overview

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
| `timelake-catalog` | library + optional service | Iceberg REST catalog with commit queueing, metadata cache, scan planning, commit events (§5.2) |
| `timelake-functions` | library | Engine-agnostic time-series kernels on Arrow (§5.3) |
| `timelake-query` | library | DataFusion integration: table provider, UDF adapters, rollup query rewrite, hot-tier merge, fast paths |
| `timelake-ingest` | binary / container | Firehose-style ingestion with in-stream rules (§13) |
| `timelake-maintain` | binary / job | Compaction, rollup refresh, retention, snapshot expiry. Runs as cron, k8s CronJob, Lambda, or on commit events |
| `timelake` CLI | binary | Create/adopt tables, backfill, query, inspect, erase, run maintenance |
| `timelake` Python | PyO3 wheel | Query → Arrow / Polars / pandas; write; rollup management |
| DuckDB extension | loadable extension | Primary user-facing SQL path: Timelake-aware scans (indexes, hot tier, dedup) and the function library in DuckDB |

---

## 7. Data model and table layout (IoT-first)

### 7.1 Timelake tables: a time-series table contract (D5)
Users define their own schema. A table becomes a **Timelake table** when it meets this contract:

| Element | Required? | Meaning |
|---------|-----------|---------|
| `time_column` | **Required**, exactly one | `timestamp` / `timestamptz` (µs; ns on Iceberg v3). Drives partitioning, retention, rollups, late-data detection. |
| `series_key` | Optional (strongly recommended) | One or more columns identifying a series, e.g. `(device_id, metric)`. Drives sort order, dedup, series indexes, bucket partitioning. If absent, the table is treated as a single stream (e.g. an event log). |
| `dedup_key` | Optional | Defaults to `series_key + time_column`. Set to none to disable dedup. |
| `chunk_interval` | Optional | Time partition granularity. Auto-chosen from expected volume if omitted. |
| `_ingest_ts` | Added automatically (opt-out) | Insertion time, set by the writer per append (built 2026-10-16, table format 0.3.0, §9.1). Decides which copy of a row is the latest write; powers late-data detection and freshness metrics. |
| Supplemental time columns | Optional | Other times a sample carries (server, gateway, sent, corrected), declared so Timelake understands them (§7.4, proposed). |
| All other columns | Free-form | Any Iceberg type, including `variant` on v3. Evolves via Iceberg schema evolution (add / rename / widen / drop). |

**Creating a table:**
```sql
CREATE TABLE iot.sensor (
  device_id STRING, metric STRING, ts TIMESTAMPTZ,
  value DOUBLE, quality SMALLINT
) WITH (time_column = 'ts', series_key = ('device_id', 'metric'), chunk_interval = '1 day');
```
```python
lake.create_table("iot.sensor", schema=arrow_schema,
                  time_column="ts", series_key=["device_id", "metric"])
```

**Adopting existing tables:** `timelake adopt <table>` turns an existing Iceberg table (e.g. one in S3 Tables) into a Timelake table. It validates the contract, sets `timelake.*` properties, and lets compaction gradually re-lay out the data. This is the main migration path from "S3 Tables + Glue".

**Internal series ID:** Timelake computes a stable 64-bit hash of `series_key` for indexing and bucketing, kept in indexes only, not as a column (Q4, decided 2026-10-18).

### 7.2 Documented schema templates (not mandatory)
| Template | Example schema | Best for |
|----------|----------------|----------|
| **Narrow** | `device_id, metric, ts, value, quality` | Heterogeneous devices, thousands of metric types, sparse data |
| **Wide** | `device_id, ts, temp, pressure, rpm, …` | Homogeneous fleets, metrics sampled together |
| **Event** | `device_id, ts, event_type, payload (variant/JSON)` | Alarms, state changes, logs |
| **Series metadata** (dimension) | `device_id, site, asset_type, unit, firmware, …` | Tags kept out of the hot table and joined at query time |

IoT tip: include a `quality` column (OPC-UA-style good/uncertain/bad flag). It is common in industrial data and supported by the function library.

### 7.3 Physical layout defaults (derived from the contract; all overridable)
- **Partitioning:** `<chunk_interval>(time_column)` plus optional `bucket(N, series_id)` or `identity(tenant)`, chosen automatically from expected volume.
- **Tiered partition granularity:** Recent data is partitioned by `hour(ts)`. After compaction, older data moves to `day(ts)` or `month(ts)`. This uses Iceberg partition evolution plus a rewrite.
- **Sort order:** `(series_key…, time_column)` within each file. This gives excellent compression (delta-encoded timestamps, similar adjacent values) and tight row-group statistics for series pruning.
- **File sizing:** Target 128–512 MB files with row groups sized for selective reads. Tunable per table.
- **Encodings:** Parquet `DELTA_BINARY_PACKED` for the time column, dictionaries for the other columns (floats falling back to `BYTE_STREAM_SPLIT` once the dictionary outgrows its page; setting `float-encoding`), and ZSTD. Measured 2026-10-19 against `BYTE_STREAM_SPLIT` for floats: IIoT readings a third of the size (366 → 119 MB for 5 days) at equal or better speed, TSBS DevOps 38% larger but scans up to twice as fast (`high-cpu-all` 274 → 136 ms), random doubles +5% size and +6% scan time.


### 7.4 Supplemental timestamps (proposed 2026-10-16, for review)

**The need.** A sample often carries more than one time. In industrial data the source (a PLC or sensor) stamps when it measured; a server or gateway stamps when it received the value (OPC UA sends both: `SourceTimestamp` and `ServerTimestamp`); a device stamps when it sent a store-and-forward batch; a correction carries when it was made. Timelake adds its own insertion time (`_ingest_ts`, §9.1). Users want to keep these times, query them, measure delays between them, and sometimes decide by one of them which copy of a reading is current.

**Today** they can already be ordinary timestamp columns: stored, queried, and pruned on (every column has Iceberg min/max bounds). What Timelake doesn't know is what they mean.

**Proposal: declare them in the contract.**
- **Declaring:** `timelake.supplemental-time-columns = server_ts, sent_ts` at creation (`supplemental_time_columns=[…]` in Python, `--time-columns server_ts,sent_ts` on the CLI). Each must be a timestamp; it is stored as µs UTC like the time column. Adding one later waits for schema evolution.
- **Which copy wins** (setting `latest-by`, §21.9): by default the latest `_ingest_ts`, then the later commit. A table can instead name a column, for example `latest-by = server_ts`, so a resent old reading never overrides a newer correction however late it arrives; `_ingest_ts` and commit order still break ties. Dedup on read, compaction, the last-value index and in-append dedup all use the same order. A numeric version column would work the same way; whether to allow non-timestamp columns is a question below.
- **What arrived when, from metadata:** each commit records the min and max of every supplemental time and of `_ingest_ts` in its snapshot summary, next to the time ranges it touched (§9.1). "Which partitions got data the server stamped after 10:00" and freshness dashboards then need no scan.
- **Delays:** per commit, the largest gap between the time column and `_ingest_ts` (and each supplemental time) goes in the snapshot summary, so store-and-forward lateness is visible per gateway or table without a query; SQL can compute it per row (`_ingest_ts - time`).
- **As of an insertion time:** `SET timelake.as_of_inserted = '2026-01-05 10:00'` answers "what did we know at 10:00": rows inserted later are left out before dedup, so a reading corrected at 10:30 shows its 10:00 value. It reads only `_ingest_ts`, or the column `latest-by` names.

**Questions for review:**
- **Names:** `supplemental-time-columns` and `latest-by`, or role names (`server-time-column`, `sent-time-column`) that tools could rely on, OPC UA style?
- **Non-timestamp `latest-by`:** allow a version number or sequence column, or timestamps only?
- **As-of:** a session setting (switchable, works in any SQL), or also a table function (`as_of_inserted('iot.readings', TIMESTAMP …)`) for one table in a join?
- **Writers that don't fill them:** a supplemental column may be null (the source didn't send it); `latest-by` then falls back to `_ingest_ts` for those rows. Or require it?

---

## 8. Write path, freshness, and the hot tier (R4, R13, R15)

### 8.1 Ingestion modes
- **Continuous:** Records stream into `timelake-ingest` or the Rust/Python writer. They are buffered and committed to Iceberg every *T* seconds or *S* MB (defaults: 60 s / 128 MB).
- **Batch / backfill:** Bulk loads of historical data go through the backfill path (§8.4).
- **Idempotency:** Each batch carries an ID that is recorded in the snapshot summary, so retried commits are no-ops.

### 8.2 Freshness tiers (users choose; the default will be decided by profiling B vs. C — Q5)

| Mode | Extra infra | Freshness (Timelake reader) | Freshness (plain DuckDB) | Notes |
|------|-------------|-----------------------------|--------------------------|-------|
| **A. Lake-only** | none | commit interval (~1 min) | commit interval | Simplest and cheapest. No moving parts. |
| **B. Object-store WAL** | none (uses the same bucket) | ~1 s | commit interval | Arrow IPC segments appended to object storage, coordinated with conditional writes (S3 `If-None-Match`/`If-Match`; GCS/Azure preconditions). Optionally on S3 Express One Zone for lower latency. Still serverless. |
| **C. Managed hot store** | Valkey/Redis, Postgres, or DuckDB | sub-second | commit interval* | Also serves a "latest value per series" cache and sub-ms point lookups. |

\*Plain DuckDB can see hot data via a generated view that unions the Iceberg scan with the hot store (e.g. DuckDB's Postgres scanner), or via the future Timelake DuckDB extension.

### 8.3 Lake/hot-tier consistency
- Each lake commit records the **WAL high-water mark** (per ingest partition) in its snapshot summary.
- A Timelake reader reads a snapshot and then reads from the hot tier only *after* that mark. This avoids gaps and double counting, with no coordination service.
- The hot tier is a **cache of uncommitted and recent data**, never the source of truth. It can be wiped and rebuilt from the WAL or the lake.
- The hot tier is accessed through a `HotStore` trait so backends are pluggable (R4).

### 8.4 Backfill (R16, D9)

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
| `replace` | Atomically replaces the covered (series, time range) in one commit. Replaced rows are *logically* deleted (§9.2), so it is reversible. | Reprocessing after calibration fixes |
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

**Sources:** Any source supported by import (§8.5).

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

### 8.5 Import and migration (R18, D10)

**`import` = create (or adopt) a table + backfill.**
- `backfill` loads into a table that already exists.
- `import` also *infers the table contract* from the source and creates the table. It then reuses the backfill engine (§8.4), so all its guarantees apply: final layout, branch staging, atomic publish, resumable, dry run.

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
| **Iceberg tables** (S3 Tables, Glue, any REST catalog) | `iceberg-rust` | Schema, partitioning. Two options: copy + re-layout with `import`, or **in place** with `adopt` (§7.1) |
| Delta Lake, DuckLake, Hive-partitioned Parquet | Native or DuckDB-backed reader | Schema, partition columns |
| **TimescaleDB** | Parallel by **hypertable chunk** (natural time-range units), consistent snapshot, binary `COPY` → Arrow | Hypertable time dimension → `time_column`; compression `segmentby` → `series_key`; `chunk_time_interval` → `chunk_interval`; retention policy → retention. Continuous aggregate definitions are **translated to Timelake rollups** where the functions map; anything unmapped is reported. |
| PostgreSQL / MySQL / SQL Server | Parallel by time-range slices, ADBC / `COPY` | Time column from type; series key proposed |
| InfluxDB (v1/v2 export, v3 Flight SQL) | Per measurement | Measurement → table; tags → `series_key`; fields → columns |
| ClickHouse, QuestDB | Native protocol or HTTP/Arrow | Time and ordering key → contract |
| Prometheus | Remote-read API | Metric + labels → `series_key` |
| Industrial historians* (AVEVA PI, Ignition SQL historian, …) | **Premium** connectors (§20); CSV exports work in open source | Tag → series, quality → `quality` |
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

---

## 9. Late-arriving data, dedup, and deletes

### 9.1 Late and out-of-order data, dedup

- **Late data is normal**, not an error. It is written to its correct time partition. Each commit records the **time ranges it touched** (per partition, optionally per series bucket) in the snapshot summary.
- **Small-file control:** Late data scattered across old partitions creates small files. The compactor prioritizes partitions by *small-file count × query heat*.
- **Rollup invalidation:** The rollup refresher diffs snapshots since its last processed snapshot and recomputes only the affected buckets (§11). No separate change log is needed.
- **Duplicates and corrections:** IoT devices resend data. Per-table policy:
  - `append` (default for raw): Duplicates are removed at compaction and at read time by the Timelake reader, using `dedup_key` and keeping the latest `_ingest_ts`.
  - `upsert`: Same as `append`, but exposed as a first-class semantic for corrections.
- **Trade-off:** Plain DuckDB may see duplicates in recently written, not-yet-compacted partitions. The Timelake DuckDB extension removes them on read. *Open question Q3.*

**Built so far (2026-10-16).** The key is `(series…, time)`. The latest write wins: the higher Iceberg data sequence number (commit order), and within one append the later row. An append keeps only the last of equal keys, so every file is free of duplicates on its own. The Timelake reader removes copies on read (*merge* below); compaction merges only runs of files next to each other in commit order, and a merged file keeps the newest data sequence number it replaces. The first version cost 1–4.6× on not-yet-compacted IIoT data (§17.6), because there nearly every file of a day overlaps every other; see the measurements below for where it stands now.

**Insertion time, `_ingest_ts` (built 2026-10-16, table format 0.3.0).**
- Tables created with a time column get a `_ingest_ts` column (timestamp with time zone, µs, not null) unless created with `ingest_time = false` (`--no-ingest-time`). The table records it in `timelake.ingest-time-column`.
- The writer sets it once per append, to the time the append started, for every row. An input that already has the column (an import keeping the original arrival times) keeps its values.
- The latest write is the highest `(_ingest_ts, commit order)`; tables without the column use commit order alone. Writers with skewed clocks can therefore order copies by their clocks rather than by commit; commit order breaks ties.
- Other engines can dedup with it: `QUALIFY row_number() OVER (PARTITION BY <series…>, <time> ORDER BY _ingest_ts DESC) = 1`. Arrival delay (`_ingest_ts - time`) measures store-and-forward lateness per row.
- It costs little storage (one value per append, run-length encoded). Adding it needs table format 0.3.0: older Timelakes can't open such tables. Existing tables can't gain the column until schema evolution exists.

**Dedup on read strategies** (setting `read-dedup-strategy`, §21.9):
- **merge** (built): files that may share a key are read with their commit rank, split by a hash of the key, merged in key order per slice, and the last row of each key kept. Only filters on key columns may apply before the merge, so filters on other columns can't prune those files.
- **keys-first** (built 2026-10-16): pass 1 reads only the key columns (and `_ingest_ts` or the rank) of the files that may share keys, restricted by key filters, merges them and lists the older copies as `(file rank, key)`. Pass 2 is the normal parallel scan with every filter pushed down, anti-joined against that list. Pass 1 reads two or three columns instead of every projected one; pass 2 prunes as freely as a scan without dedup, because the list was built from all rows.
- **Measured (2026-10-16, IIoT 50 machines × 9 days, before maintenance, laptop):** keys-first was slower on every query (1.3–6×; pass 2's anti-join probe costs more than merging the value columns), so merge is the default and keys-first stays a setting.
- **What made merge cheap:** splitting by the series hash inside the dedup operator (DataFusion otherwise re-sorts each slice), keeping each series whole in one slice so windows and groups by series need no new sort, filtering rows by key filters and a join's runtime filters before the merge, and comparing neighbouring keys column by column instead of row by row. Window queries (`gaps`, `time-above-limit`) then cost the same with and without dedup; scans of every row (`daily-stats`) still cost 2.6×, which cold-partition compaction removes (1.1×), leaving only the newest day to merge.

**Cold partitions compact sooner (built 2026-10-16, setting `compact-cold-after`).** Inline maintenance rewrites a partition into one sorted, duplicate-free file once its time range ended longer ago than the setting (default 1 hour) and it has more than one file. Backfill into a cold partition triggers the same rewrite after `compact-min-files` new files, not after every append. Dedup on read then only touches the current partitions, and DuckDB sees copies for a shorter time.

### 9.2 Deletes: immutable by design (D6)

**Principle:** A delete never mutates or destroys existing data. It is a new, audited, immutable fact layered on top. Iceberg already works this way: data files are never modified, and deletes are recorded as *new* delete files / deletion vectors in a new snapshot.

| Operation | Mechanism | Visible to plain DuckDB? | Reversible? |
|-----------|-----------|--------------------------|-------------|
| **Logical delete** (default) | Predicate → Iceberg v3 deletion vectors (v2: position deletes). Never rewrites data files. | Yes (if the engine supports the delete-file type; *verify DuckDB v3 DV support*) | **Yes**: `UNDELETE <delete_id>` commits a snapshot that removes those delete vectors. Original rows were never touched. |
| **Retention drop** | Whole-partition removal by policy | Yes | Until the snapshot expires |
| **Purge** (hard delete, for compliance) | Explicit, privileged operation: rewrites affected files without the rows, expires snapshots that still reference them, removes orphans | Yes | **No**, by design. Requires a reason and is audited. |

**Time-series-shaped deletes are first-class:**
```sql
DELETE FROM iot.sensor
WHERE device_id = 'pump-7' AND ts BETWEEN '2026-10-01' AND '2026-10-02'
WITH (reason = 'sensor miscalibrated', ticket = 'OPS-123');
```

**Audit trail:**
- Every delete gets a `delete_id` and records actor, timestamp, predicate, reason, and affected row count.
- These records go in the snapshot summary **and** in an append-only `_timelake_audit` Iceberg table, so audits are a SQL query.

**Consistency with rollups:** A delete, undelete, or purge marks its time ranges as changed, so the affected rollup buckets are recomputed (§11). Rollups never silently include deleted data.

**Corrections vs. deletes:** Bad values usually need *correcting* rather than deleting. An `upsert` writes the corrected row, and dedup keeps the latest `_ingest_ts`. The original stays in older snapshots, so history is preserved without a delete.

### 9.3 GDPR erasure (R17, D8)

A logical delete hides data but is not erasure: the bytes still exist in the original files and older snapshots. GDPR requires physical removal from **every copy**.

**Identifying the data subject.** In IoT, personal data is usually *indirect*: a person is linked to a vehicle, wearable, smart meter, or badge, not to individual rows. Tables can therefore declare a `subject_key`:
- **Direct:** a column on the table itself, e.g. `user_id`.
- **Indirect:** a mapping through a dimension table, e.g. `devices.owner_id`. Erasing a person resolves to "these series, over these time ranges of ownership."

**Erasure workflow**
1. **Request:** `timelake erase --subject <id>` (or an explicit predicate) creates an erasure request with an ID and a deadline.
2. **Immediately:** A logical delete is applied across every affected table, and the hot tier is evicted. The data is invisible within seconds.
3. **Physical purge** rewrites files, which is the expensive part, so it is *batched* by default:
   - `purge_schedule = daily` (default): All erasure requests from the day are purged together in the next maintenance run, so a file shared by several requests is rewritten once.
   - `timelake erase … --now`: Purge immediately. This takes minutes; duration scales with the amount of data touched, not the table size.
   - `max_purge_delay` (default 7 days): A hard upper bound. If a purge hasn't completed by then (e.g. jobs failing), Timelake alerts. GDPR expects erasure "without undue delay", generally within one month.
   - The purge job:
     - Rewrites affected files without the rows. The series-sorted layout keeps the number of touched files small.
     - Expires every snapshot that references the old files and deletes the orphaned files.
4. **Verify and certify:** A verification query confirms zero matching rows remain. The request is closed with a record of files rewritten, snapshots expired, and objects deleted.

**Coverage checklist (what purge must reach)**

| Location | Action |
|----------|--------|
| Raw tables, events table | Rewrite files; expire snapshots; delete orphans |
| Rollups | Recompute affected buckets; delete per-subject rollup rows (aggregates across many subjects are generally not personal data) |
| Hot tier | Delete/rewrite WAL segments; evict Valkey/DB keys |
| Puffin indexes, bloom filters, sketches | Rebuild for rewritten files |
| Catalog and reader caches | Invalidate; local caches have a bounded TTL |
| Ingest state checkpoints | Remove per-series state |
| **Bucket versioning / replication** | Purge must also delete non-current object versions and replicas, or the job fails loudly. |
| User backups outside Timelake | Out of scope; documented |

**Audit without leaking personal data:** Erasure audit records store the request ID and a *salted hash* of the subject identifier, never the raw identifier or predicate values. (Ordinary logical-delete audits, §9.2, store the predicate as-is.)

**Time-travel trade-off:** Erasure shortens time travel for affected files, because snapshots older than the purge that reference purged files must be expired. This is documented and surfaced in the erasure report.

**Tenant offboarding: crypto-shredding (optional):**
- With a warehouse or bucket per tenant (§14), each tenant can have its own KMS encryption key.
- Destroying that key makes all of the tenant's data unreadable instantly, followed by asynchronous deletion of the objects.
- This doesn't work for individual subjects inside a shared table; those use the purge workflow above.

**Legal hold / WORM:** Not in v1 (Q11 resolved). Purge must always be possible for GDPR. Per-table legal hold may come later, with explicit rules for how it interacts with erasure requests.

---

## 10. Table maintenance: the "Glue jobs" we replace (R13)

`timelake-maintain` runs as a set of idempotent, independently schedulable tasks:

| Task | What it does |
|------|-------------|
| Compaction | Merges small files, re-sorts by `(series_key…, time_column)`, applies dedup, coarsens partitions for old data |
| Rollup refresh | Incremental, snapshot-diff based (§11) |
| Retention | Per-table TTL (e.g. raw 90 d, 1-min rollup 2 y, 1-h rollup forever). Drops whole partitions (metadata-only when aligned) |
| Snapshot expiry and orphan cleanup | Keeps metadata small and storage costs down |
| Manifest rewrite | Clusters manifests by time so planning stays fast as tables grow |
| Index build | Writes Puffin blobs (series index, last-value index, sketches) |

These tasks can be triggered by a schedule, by commit events (catalog events, S3 notifications), or inline from the writer for small deployments. The core is concurrency-safe via Iceberg optimistic commits and conflict-aware retries.

### 10.1 Streaming compaction (decided 2026-10-17, built in the engine 2026-10-17)

**Status:** the engine compacts by streaming merge (`timelake_format::merge`, `maintain.rs`). Full compaction of TSBS IoT, 72 hourly files into 3 days: 3.65 → 2.37 s, peak memory 1.95 → 0.53 GB, identical output ([`bench/results/PERFORMANCE.md`](bench/results/PERFORMANCE.md)). Per open input about 10 MB (its row group). Writing was 80% of the time on one thread; the shared file writer (`timelake_format::file`) now encodes columns in parallel: 2.37 → 1.17 s, byte-identical files. The shared maintenance (WASM) adds the byte cap per run, page-window reads by the offset index, and at most 24 files per merge (§21.10).

**Why.** Compaction read every file of a run into memory, concatenated, sorted, and deduplicated it, so its memory grew with the partition: fine on a server, too much for a 128 MB Cloudflare Worker (F10), and wasteful everywhere.

**Design.** Every data file Timelake writes is sorted by the write order `(series…, time, insertion time)` and free of duplicates on its own (§9.1). Compaction becomes a k-way merge of its input files:
- Each input is read as a stream of small batches; the merge keeps the next batch of each input and emits rows in write order. Inputs are taken in commit order, so among rows with the same `(series…, time)` the later commit wins, then the later insertion time, as before.
- Duplicates are dropped as they meet: the last row of each `(series…, time)` is kept when dedup is on.
- Memory is one batch per input, the row group being written, and one upload part, whatever the partition's size. Outputs larger than an upload part go up as multipart uploads.
- An input not sorted in the table's order (written by another engine, or before the table declared one) is sorted on its own first: memory is then bounded by the largest such file, not the partition.
- The merge is a state machine with no I/O (it says which input needs its next batch, and takes batches as they come), so the engine (async, object store) and the WASM writer (JS store) run the same code.

**Optimizations to try, kept if measured faster:**
- Runs, not rows: where the current batch of one input ends before the next key of every other input, the whole run is emitted as a slice, without comparing rows one by one (found by binary search on row-encoded keys). Time-series files mostly cover different series or times, so most of a merge should be runs.
- Copy without decoding: a whole row group, or a whole file, whose key range overlaps no other input is copied to the output as encoded column chunks. This needs each file's (or row group's) first and last composite key; the per-column bounds in Iceberg metadata are too weak for a multi-column series key. If it pays off, the format gains them (below).
- Parallel decoding of inputs on native targets.
- Smaller read batches and row groups where memory is tight (WASM), as settings.

**Format extensions considered** (added to the table format only if a measurement needs them; readers that don't know them ignore them):
- In each data file's Parquet key-value metadata: `timelake.sort` (the write order) and `timelake.unique = true` (no two rows share `(series…, time)`), so compaction and dedup on read can trust a file without the table's history.
- Per data file in the head (and per row group in the footer metadata): the first and last composite key, for copying non-overlapping inputs without decoding.

**Limits per run.** A setting caps the bytes one maintenance run rewrites (needed on Workers, where a request has at most 5 minutes of CPU; useful anywhere inline maintenance shouldn't hold up appends). A run that stops at the cap commits what it rewrote; the next run continues.

---

## 11. Materialized rollups and the function library (R5)

### 11.1 Materialized rollups
A materialized rollup is a table of aggregates per time bucket (for example, per device per minute) that Timelake
keeps up to date as data lands, so dashboards read a small table instead of the raw data.
- Defined declaratively in SQL:
  ```sql
  CREATE MATERIALIZED ROLLUP sensor_1m AS
  SELECT device_id, metric,
         time_bucket('1 minute', ts) AS bucket,
         avg(value), min(value), max(value),
         time_weighted_mean(ts, value, 'linear') AS twa,
         count(*)
  FROM iot.sensor
  GROUP BY 1, 2, 3
  WITH (refresh_lag = '2 minutes', retention = '2 years');
  ```
- **Every rollup is itself an Iceberg table**, so DuckDB and others can query rollups directly.
- **Incremental refresh:** Only buckets touched since the last refresh are recomputed (from snapshot diffs). This handles late data correctly.
- **Hierarchical rollups:** 1 min → 1 h → 1 d, each built from the previous level.
- **Real-time aggregation (Timelake reader):** Combines materialized buckets with on-the-fly aggregation of the not-yet-materialized tail and hot-tier data.
- **Automatic query routing:** The Timelake reader rewrites `GROUP BY time_bucket(...)` queries to use the best matching rollup.
- **Mergeable state:** Aggregates that can't be re-merged from final values (percentiles, time-weighted averages) store intermediate **state** (sketches, partial sums).

#### 11.1.1 First version (built 2026-10-16)
The first version builds one level, from the raw table, with aggregates that a bucket's rows give exactly. What it
leaves out is listed at the end.

**Definition.**
```sql
CREATE MATERIALIZED ROLLUP [IF NOT EXISTS] iot.readings_1m AS
SELECT device, time_bucket(INTERVAL '1 minute', time) AS bucket,
       avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value, count(*) AS n
FROM iot.readings
GROUP BY device, bucket
[WITH (retention = '2y', chunk_interval = 'month', refresh_interval = '5m')];

REFRESH MATERIALIZED ROLLUP iot.readings_1m;   -- bring it up to date now
DROP MATERIALIZED ROLLUP [IF EXISTS] iot.readings_1m;
```
- The query reads one Timelake time-series table, with no `WHERE`, join, or subquery. It groups by one
  `time_bucket` (or `date_bin`) of the table's time column, with a fixed width (no months) and the default origin, and
  by any of the table's series-key columns, each of which must be selected.
- Aggregates: `count(*)`, `count(x)`, `sum`, `min`, `max`, `avg`, and `first_value` / `last_value` ordered by the
  time column. Unaliased outputs get names like `avg_value` and `bucket`.
- Grouping only by series-key columns keeps a row in the same group when a newer write of it replaces it (§9.1), so
  appending recomputed buckets is enough: nothing ever has to be taken out of a rollup.
- `WITH` takes the rollup table's `retention`, `chunk_interval` (default: day for buckets under an hour, else month)
  and `refresh_interval`, and any other table setting.

**Storage.** A rollup is an ordinary Timelake table: time column `bucket`, series key the grouping columns, dedup on,
no insertion-time column. Its properties hold the definition, which Timelake manages:
`timelake.rollup-source` (the raw table), `timelake.rollup-bucket` (the width) and `timelake.rollup-aggregates`
(JSON: output name, function, column). The raw table lists its rollups in `timelake.rollups`. Every snapshot of the
rollup records in its summary the raw table snapshot it reflects: `timelake.rollup-source-snapshot` and
`timelake.rollup-source-sequence` (that snapshot's sequence number), carried forward by the rollup's own compaction.
No format change: older Timelake versions read a rollup as a plain table and don't refresh it.

**Refresh.** The changed buckets are those covered by the time bounds of the raw table's live data files with a data
sequence number above the recorded one. Compaction keeps the newest data sequence number of the files it merges
(§10), so rewritten files whose rows the rollup already reflects don't count as changed; retention's deletes don't
either, so a rollup keeps history the raw table has dropped. The changed buckets are recomputed from the raw table
(read with dedup on), a window of at least a day at a time, and appended to the rollup, about a million rows per
commit, the last commit recording the new raw snapshot. Dedup on read keeps the newest copy of each bucket, and the
rollup's own compaction removes the rest. A refresh that fails halfway leaves the old record in place, so the next
one recomputes the same buckets.
- `timelake maintain` and `REFRESH MATERIALIZED ROLLUP` refresh every time. Inline maintenance after an append
  refreshes a rollup once its last refresh is older than `timelake.rollup-refresh-interval` (default 1 minute, `off`
  for never), so a busy table doesn't commit to its rollups on every append.
- Two refreshes at once are safe: the later commit wins each bucket, and its record names the raw snapshot those rows
  reflect, so any bucket changed after that snapshot counts as changed again.
- If the recorded raw snapshot is no longer in the history of the raw table's current snapshot (a rollback past it),
  the rollup may hold rows the raw table no longer has. Routing skips it, and the next refresh removes its files,
  records that it reflects nothing, and computes it again.

**Routing.** The Timelake reader answers `SELECT … GROUP BY time_bucket(w, time), keys…` on a raw table from a
rollup when the grouping is the rollup's (same width, same key columns), every aggregate is one the rollup stores,
and the filters are on the time column (comparisons with constants, `now()` included) or on the grouping columns.
The answer is exact, not a stale rollup:
- Buckets that are inside the query's time range, unchanged since the refresh, and within both tables' retention come
  from the rollup.
- Every other row (the changed buckets, the partial buckets at the edges of the time range) is aggregated from the raw
  table as usual. The two parts share no bucket, so they are combined with `UNION ALL`.
- `timelake.rollup-routing` (table and session; default on) turns it off. A session that reads without dedup
  (`timelake.read_dedup = false`) is not routed, since rollups count each reading once.

**Not in the first version:** rollups of rollups, aggregates that need stored state (percentiles, time-weighted
averages), answering a coarser query (hourly from minutes) or fewer keys from a rollup, calendar buckets (months),
filters in the definition, routing in plain DuckDB, and recomputing buckets after a delete (§9.2).

#### 11.1.4 Signal aggregates: time-weighted means, integrals, counters (built 2026-10-18)
`time_weighted_mean(time, value [, method])` and `integral(time, value [, method])` (value × seconds), method
`forward_fill` (default, alias `ffill`) or `linear`; `counter_diff(time, value)` and `counter_rate(time, value)` (per
second), where a drop is a reset to zero, so the reading after it counts whole. All over each group's first to last
reading; readings at one time count once with the largest value; one reading time gives its value as the mean, 0 as
the integral and growth, and no rate. Decided with the user on 2026-10-18 (exact always, both methods, first to last
reading).
- **State:** a list of segments, runs of a group's readings in time order: first and last reading, the values of the
  readings next to them, the time from the first to the next and into the last, the area and the counter's growth.
  Segments that follow each other join exactly (the gap's area and growth added; a shared time takes the larger value,
  corrected through the stored neighbours); partial results emit their runs unjoined, since another partition may
  hold readings between them. Overlapping segments are an error. The four aggregates share the state and differ in
  what they return from it. Layout measured on IIoT hourly means: neighbours as `Option<f64>` (96-byte segments)
  2.30 s, as plain `f64`s (80 bytes) 2.17 s, the same as the mean alone before; the open segment kept inline in each
  group instead of behind the vector's pointer 2.95 s (the group array doubles and DataFusion moves it as it grows).
- **Series order:** no partition may have readings of a series inside another partition's run of it. Scans under a
  signal aggregate over readings give that: files that must be merged (copies with dedup on read; without, files that
  overlap on `(series…, time)` or aren't sorted) are merged in series order with every file of the time they span
  (closed under overlap; the dedup operator, keeping every row without dedup), and the files before and after that
  time are read as they are, files in time order, each partition a contiguous stretch (DataFusion 54's work stealing
  between file partitions is turned off for these scans with `preserve_order`). Setting `series-order` (named
  `time-weighted-mean` in 0.7.0): `auto`, `segments` (copies only; overlaps are an error), `merge` (every file).
  0.7.0 spread files over partitions by size and merged only tables with dedup off, so a partition's run spanned days
  other partitions held, and `GROUP BY tag` over a month failed. Measured alternative: rewriting to windows (`lead`
  per group in time order): hourly means of 370 M IIoT readings 550 s and 20 GB peak, against 2.0 s with segments.
- **Rollups** store the state (`time_weighted_state`); routing merges it with the query's aggregate
  (`integral(state [, method])`), at the rollup's width too. One stored state answers all four for its column and
  method; the counters from either method's state.
- **Cost:** IIoT small, hourly per tag over 30 days: `avg` 1.98 s; each signal aggregate 2.18–2.20 s; all four in one
  query 3.47 s with a state each, 2.11 s sharing one: an optimizer rule after routing replaces the signal aggregates
  of the same readings and method (the counters join either method's) with one `time_weighted_state` and computes
  each result from it in a projection (`integral_of_state(state)`); setting `shared-signal-state`. Against the window form of the IIoT queries over 30 days: energy
  per machine (`lag`) 1.66 → 1.09 s, time-weighted means of a line's temperatures (`lead`) 166 → 87 ms.

#### 11.1.3 `m4` (built 2026-10-18, #73)
`m4(time, value)` is an aggregate returning a group's first, smallest, largest and last points as a list of
`{time, value}` structs in time order, repeats removed (after Apache IoTDB's time-window `M4`). Ties are broken by fixed
orders (time then value for first and last, value then time for the extremes), so the result doesn't depend on row
order. The points merge: `m4` over several groups' points equals `m4` over their rows, so the list is also the
aggregate's state, `m4(points)` merges lists, a rollup stores `m4` as a list column, and routing answers wider buckets
by merging stored lists. Columnar group accumulator. IIoT small, hourly points per tag over 30 days: 2.03 s from the
readings (3.5 s with a per-group accumulator), 0.51 s from a 1-minute rollup; `min`, `max`, `first_value` and
`last_value` alone (no times for the extremes) take 1.76 s.

#### 11.1.2 Coarser queries (built 2026-10-18)
Routing also answers a query whose bucket is a whole multiple of the rollup's and whose keys are a subset of the
rollup's. The rollup's rows in the query's buckets are aggregated again: `count` and `sum` by `sum`, `min` and `max` by
themselves, `first_value` and `last_value` ordered by the bucket, and `avg` as `sum(sum) / sum(count)`, or
`sum(avg × count) / sum(count)` when the rollup stores the average and a count of the column (`count(*)` where the
column can't be null). A query bucket comes from the rollup only if every rollup bucket in it is unchanged since the
refresh; the rest come from the raw table as before. A query at the rollup's own width and keys reads its rows as
they are. Measured on IIoT small (30 days, 1,000 tags, 1-minute rollup of 30 M rows): daily statistics per tag 2.07 →
0.25 s, 6-hourly totals over all tags 1.10 → 0.11 s, hourly for 20 tags 144 → 66 ms; answers equal the raw table's
to floating-point rounding (relative difference at most 9 × 10⁻¹³).

**Rollups of rollups** come from the same routing: every rollup is defined on the raw table, and its refresh query
(a `GROUP BY time_bucket` over the changed buckets) is routed like any query to the table's other rollups, leaving out
the one being refreshed. Refreshes go from the narrowest bucket to the widest, so a daily rollup is computed from an
up-to-date hourly or minute one. IIoT small: creating a daily rollup 5.2 s from the raw table, 1.8 s with a 1-minute
rollup.

### 11.2 Function library
Every function has a plain, descriptive name. Where pandas uses a different name, that name works too, as an alias:
`forward_fill` and `ffill` are the same function (D22). Polars mostly uses the descriptive names, so they match it.

| Category | Functions (pandas alias in brackets, where it differs) |
|----------|-----------|
| Bucketing | `time_bucket` (with origin, offset, timezone, months); `time_buckets(width, start, end [, origin])`, every bucket in a range, including empty ones, to join against (`date_range`; built 2026-10-19, fixed widths) |
| Gap filling | `forward_fill` (`ffill`), `backward_fill` (`bfill`), `interpolate` (linear or by time); built 2026-10-19 as window functions over a partition at once: `forward_fill` over 10 M rows 0.26 s against DataFusion's `last_value(x) IGNORE NULLS` 3.6 s, `backward_fill` 0.27 s against `first_value(x) IGNORE NULLS` 0.37 s |
| Selection | `first`, `last`, `value_at`, the latest value at or before a time (`asof`); ASOF JOIN |
| Statistics | `mean`, `stddev` (`std`), `variance` (`var`), `skewness` (`skew`), `kurtosis` (`kurt`), `corr`, `covariance` (`cov`), `regr_slope`, `regr_intercept`, `regr_r2`, `histogram` |
| Quantiles | `quantile`, `approx_quantile` (mergeable sketch state, so it rolls up across buckets) |
| Changes | `diff`, `percent_change` (`pct_change`), `rate` (per second); `counter_diff` and `counter_rate` also handle counter resets (§11.1.4, built) |
| Time-weighted | `time_weighted_mean` (`forward_fill` or `linear`), `integral` (§11.1.4, built). **Critical for IoT** because sampling is irregular |
| States and uptime | Time in a state and uptime: `integral(time, condition)` and `time_weighted_mean(time, condition)`, a condition counting 1 while true (built 2026-10-19 instead of `state_duration` and `uptime`; IIoT `time-above-limit` 334 → 191 ms against the `lead()` window form); `gaps` (silences longer than a threshold) |
| Finance | `ohlc` (open, high, low, close; volume is a `sum`) |
| Downsampling | `m4(time, value)`, a group's first, smallest, largest and last points (§11.1.3, built); `lttb` |
| Rolling and anomalies | `rolling_mean`, `rolling_std` (`rolling(…).mean()`), `zscore`, flatline (stuck sensor) detection |

**Availability by engine:**
- **Timelake reader (DataFusion):** Full library as Rust UDFs/UDAFs.
- **Plain DuckDB:** Functions DuckDB has natively (`time_bucket`, `arg_max`, ASOF JOIN, `approx_quantile`) plus a published **SQL macro pack**. The macros must use fixed-width intervals (e.g. `'24 hours'`, not `'1 day'`) or `date_trunc` for UTC data. Calendar-unit buckets on `timestamptz` take a ~130× slower path in DuckDB (measured in §17.5).
- **DuckDB extension (later):** Full parity.

---

## 12. Read path (R1, R10, R14)

### 12.1 Plain DuckDB (zero Timelake software)
```sql
INSTALL iceberg; LOAD iceberg;
ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'https://catalog…');
SELECT time_bucket(INTERVAL '1 hour', bucket) AS h, avg(avg_value)
FROM lake.iot.sensor_1m WHERE device_id = 'pump-7' AND bucket > now() - INTERVAL 7 DAY
GROUP BY 1 ORDER BY 1;
```
This path sees committed data only, and does not get hot-tier merge, dedup-on-read, or the full function library.

### 12.2 Timelake reader (Rust + Python): "faster than DuckDB" on time-series shapes
We don't try to beat DuckDB as a general engine. We win by using knowledge a generic engine doesn't have:
1. **Series-level pruning** from Puffin indexes, which goes beyond min/max statistics: open only the files and row groups containing the requested series.
2. **Last-value index:** "Current state of every device" without scanning.
3. **Automatic rollup routing.**
4. **Sort-order awareness:** No re-sorting for per-series windows and ASOF joins.
5. **Hot-tier merge** for instant freshness.
6. **I/O tuned for object storage:** Coalesced range reads, parallel GETs, and a metadata and footer cache (in memory or on local disk).

**Python API sketch:**
```python
import timelake as tl
lake = tl.connect("s3://bucket/warehouse", catalog="rest://…")
tbl = lake.table("iot.sensor")
df = tbl.query(series={"site": "plant-3", "metric": "temp"},
               start="-24h", bucket="5m", agg=["avg", "max"]).to_polars()
lake.sql("SELECT … time_weighted_mean(…) …").to_arrow()   # full SQL via DataFusion
tbl.to_duckdb()                                     # hand off an Arrow stream to DuckDB zero-copy
```

---

## 13. Ingestion: `timelake-ingest` (R11, D11)

A pipeline that does **ingestion and in-stream analytics in one place**. It is a **library first**: the same pipeline code runs in every deployment form, and the binary, container, and Lambda handler are thin wrappers around it.

### 13.1 Deployment forms
| Form | Triggered by | Best for |
|------|--------------|----------|
| **Embedded library** (Rust; Python via bindings) | User code calls `pipeline.push(batch)` | Apps that already receive data |
| **AWS Lambda** (and other FaaS) | Kinesis, SQS, IoT Core rules, API Gateway, S3 object-created events, EventBridge schedule | Bursty or low-volume fleets; scale to zero; no ops |
| **Container / microservice** | Long-lived consumers: MQTT, Kafka/Redpanda, NATS, HTTP, Arrow Flight | Steady high volume; persistent MQTT connections |
| **Edge binary** | Same as container, on a gateway | Factories and sites with unreliable uplinks: buffers locally, then catches up |

All forms share a single config file (`inputs → processors → rules → outputs`), so a pipeline can move from Lambda to a container without changes.

### 13.2 Pipeline
- **Inputs:** MQTT (IoT first-class), HTTP (JSON/NDJSON), Arrow Flight / Arrow IPC, Kafka/Redpanda, Kinesis, NATS, InfluxDB line protocol, OTLP metrics. OPC-UA and Sparkplug B are candidates.
- **Stages:** decode → schema validation and evolution → enrichment (join with series metadata) → transforms (DataFusion SQL) → **rules** → buffer → WAL / hot tier → lake commit.

### 13.3 Rules
- **Built-in rules (configurable per series or group):**
  - Threshold and range (with hysteresis)
  - Rate of change / spike
  - Flatline or stuck sensor
  - Missing data / heartbeat timeout
  - Out-of-order or late beyond tolerance
  - Unit and schema anomalies
  - Statistical anomalies (rolling z-score, EWMA)
- **Outputs of rules:** Rows in an **`events` Iceberg table** (queryable history of alarms) plus real-time sinks (webhook, Kafka, SNS/SQS, MQTT topic). Generalized to any event type and any event stream in F3 (§21.3).

### 13.4 Rule state without a server (draft — Q17)
| Rule type | State needed | How it works serverless |
|-----------|--------------|-------------------------|
| Stateless (threshold, range, schema) | None | Runs anywhere, any instance |
| Short windows (rate of change, flatline, z-score, hysteresis) | Small per-series state (last values, running stats) | Affinity via the source's partitioning (Kinesis shard / Kafka partition keyed by series), so one consumer sees a series in order. State is checkpointed with the source offset to object storage or the hot store, and restored on cold start. |
| Absence of data (heartbeat, missing data) | "Last seen" per series | Can't be triggered by data that never arrives. A **scheduled sweep** (e.g. EventBridge every minute, or a timer in the container) checks the last-value index / hot store for overdue series. |

### 13.5 Scaling
- Horizontal.
- Stateless stages scale freely.
- Stateful rules scale with the number of source partitions.
- Containers use consistent hashing on the series key when the source has no partitioning (e.g. MQTT).

---

## 14. Multitenancy (R12)

Documented patterns with trade-offs and examples:
| Pattern | Isolation | Cost / ops | When |
|---------|-----------|-----------|------|
| Shared table, `tenant` partition column | Row-level (enforced by catalog/engine policies) | Cheapest | Many small tenants |
| Namespace per tenant | Catalog-level ACLs | Low | Tens to thousands of tenants |
| Warehouse/bucket per tenant | IAM + per-tenant KMS keys | Higher | Regulated / enterprise tenants |

Plus examples for credential vending via the REST catalog, per-tenant retention and rollups, and per-tenant ingestion quotas.

**Planned setup guides, one per object store:** AWS S3 (and S3 Express One Zone), Cloudflare R2, Tigris, Wasabi, Backblaze B2, Azure Blob, Google Cloud Storage, and self-hosted RustFS. Each covers the bucket, credentials, endpoint and region settings, conditional-write support, and the cost to expect. Ingestion quick starts already cover Local, AWS, Azure, Google Cloud, Cloudflare, Fly.io, Vercel, and Tigris, with several designs each (written 2026-10-16).

**Other planned doc recipes:** factory sensor monitoring, fleet/GPS tracking, energy metering (counters → `counter_diff`), building management, observability metrics, crypto/stock ticks, migrating from S3 Tables + Glue, and migrating from TimescaleDB/InfluxDB.

---

## 15. Deployment and cost model (R13)

- **Minimum deployment:** One bucket, one Iceberg catalog, and the Timelake library inside the user's app or one ingest container, plus a scheduled `timelake-maintain`. There is no always-on Timelake server.
- **Catalog options:**
  - **`timelake-catalog`** (recommended), in one of three forms:
    - In-process library (single node / edge)
    - Scale-to-zero function (Lambda / Cloud Run)
    - Container
    Object-storage backend by default; Postgres optional.
  - Bring your own: S3 Tables, Glue, Lakekeeper, Polaris, Nessie, Unity, with the reduced capabilities listed in §5.2.
- **Cost levers:** Fewer, bigger files (lower GET/PUT costs); rollups instead of raw scans; tiered retention; optional cold storage classes for old partitions.

---

## 16. Landscape and positioning (R7)

This section compares products. What we learned from them, and how we credit it, is in [PRIOR_ART.md](PRIOR_ART.md).

### 16.1 Table formats

| | Iceberg (plain) | Delta Lake | Apache Hudi | Apache Paimon | DuckLake | Lance | **Timelake** |
|---|---|---|---|---|---|---|---|
| Steward | Apache | Linux Foundation (Databricks) | Apache (Onehouse) | Apache (Alibaba) | DuckDB Labs | LanceDB | — |
| Core idea | Spec-first open table format | Transaction log on object storage | Streaming upserts, record indexes | LSM tree per bucket; streaming-first | Metadata in a SQL database | Random access for ML / vectors | Iceberg + time-series conventions + optional hot tier |
| Read support (DuckDB / Spark / Trino / warehouses) | Broadest | Broad | Spark/Flink-centric | Flink/Spark-centric; can emit Iceberg metadata | DuckDB-centric | Limited for analytics | Same as Iceberg |
| Frequent small writes | Weak (per-commit metadata cost) | Moderate | Good | **Strong** (LSM) | **Strong** (inlining in catalog DB) | Moderate | Strong *with* hot tier / batched commits |
| Upserts / dedup | Merge-on-read deletes | MERGE, deletion vectors | Native | Native (merge engines) | Yes | Yes | Dedup by `dedup_key` (§9.1) |
| Time-series aware layout / compaction | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Materialized rollups | ✗ | ✗ | ✗ | Aggregation merge engine (on write) | ✗ | ✗ | ✓ incremental, late-data aware |
| Needs extra infrastructure | Catalog | Optional catalog | Spark/Flink jobs | Flink/Spark jobs | SQL database | ✗ | Catalog (ours or any); hot store optional |

### 16.2 Managed catalogs and maintenance services

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

### 16.3 Time-series databases (baselines, not lakes)
TimescaleDB, InfluxDB 3, QuestDB, TDengine, GreptimeDB. They are fast and feature-rich, but they own the storage. Long retention is expensive, and the data is not an open table that other engines can read. They are the **performance and ergonomics bar** Timelake is measured against (§17).

**TimescaleDB on Tiger Cloud is the closest managed alternative.**
- **Tiered storage** (`timescaledb_osm`) moves older chunks to S3, and they stay queryable through the database. It has to be enabled per service in the Console; it can't be enabled over SQL.
- **`timescaledb_lake`**, Tiger's Iceberg integration, is available on Tiger Cloud services (seen at version 1.0.1, 2026-10). It is to be evaluated.

Both keep the database as the entry point, which is the trade-off Timelake avoids. The benchmark of tiered storage was `bench/v0/tsdb_cloud.py` (in git history at `b99aae9`; results in `bench/results/TIMESCALE.md`).

*All Timelake columns are design targets until measured by `timebench` (§17).*

---

## 17. Benchmark: `timebench` (R7, R14)

One reproducible suite, run against Timelake **and every alternative** in §16, using each system's idiomatic tooling.

### 17.1 Base workload: TSBS IoT, adopted
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

**Data generation:** Use the TSBS generator (`tsbs_generate_data --use-case=iot`), then convert it once to Parquet as the common input for every system. The data is the same as in published TSBS results, but the method isn't: TSBS loads in bulk with parallel workers and reports throughput, while timebench streams commits and times single queries from fresh processes. Numbers that sit next to published ones need TSBS's own loader and query runner (§17.7).

### 17.2 Lake extensions (what TSBS doesn't cover)
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

### 17.3 Systems under test
| Category | Systems | Writer / engine used |
|---|---|---|
| Timelake | Lake-only, object-store WAL, Valkey hot store (Q5) | Timelake writer; Timelake reader **and** plain DuckDB |
| Plain Iceberg | Iceberg + REST catalog | PyIceberg / Spark write; DuckDB, Trino read |
| Managed Iceberg | S3 Tables; R2 Data Catalog | Spark/PyIceberg write; **Firehose → S3 Tables** (AWS's recommended streaming path; planned, needs an AWS Paid plan); Athena / DuckDB read |
| Other formats | Delta Lake, Hudi, Paimon, DuckLake, Lance | delta-rs, Spark, Flink, DuckDB, lance |
| No table format | Plain Parquet files per commit, by day (what a Firehose or Kafka Connect S3 sink writes) | pyarrow; DuckDB and DataFusion over a listing |
| TSDB baselines | TimescaleDB, InfluxDB 3 Core, QuestDB, ClickHouse | Their native TSBS loaders; in timebench, each one's recommended client (§17.6) |

PyIceberg appends are the naive baseline (what v0 compares against today). Published results must also include Iceberg written the way an expert team would (planned): a Flink or Spark streaming sink that batches commits, with scheduled `rewrite_data_files` and sorting; the Kafka Connect Iceberg sink; iceberg-rust; and DuckDB's Iceberg writer. Beating only the naive writer proves too little.

### 17.4 Methodology
- **Same hardware and same object store** for every lake system:
  - Locally: RustFS (or SeaweedFS), with injected latency (e.g. toxiproxy) to approximate S3's per-request latency.
  - Published runs: real S3 in-region, plus S3 Express One Zone and R2. Local object stores don't reproduce S3's request latency, so they are for repeatability only. MinIO's community edition is no longer maintained.
  - **Planned: one published run per popular object store**, each with latency, request counts and $ per month at that store's prices: AWS S3 (Standard, Express One Zone), Cloudflare R2, Tigris, Wasabi, Backblaze B2, Azure Blob, Google Cloud Storage. Stores without request fees (R2, Wasabi) or with different latency (Tigris, Express) change the cost and speed picture most.
- **Object-store compatibility matrix (CI):** S3, GCS, Azure Blob, R2, Tigris, Wasabi, Backblaze B2, RustFS, SeaweedFS, Garage. Each is tested for the features Timelake relies on, especially **conditional writes** (`If-None-Match` / `If-Match`), which the object-store-only catalog and WAL require (§5.2, §8.2).
- **Store notes (measured 2026-10-13 from a laptop, warm connections, [bench/results/PERFORMANCE.md](bench/results/PERFORMANCE.md)):**
  - **Tigris** answers GET, HEAD, PUT and LIST faster than S3 us-east-2 (small GET 37 vs 67 ms, PUT 80 vs 110 ms), but a request for a key that doesn't exist (HEAD or GET 404) takes about 285 ms, against 55 ms on S3. Timelake's reader asks for `vN+1.metadata.json` on every table load, so on Tigris each cold load paid it. A LIST of that exact name answers the same question in about 40 ms; it is the default check on Tigris since P1 (§19.2).
  - **Tigris global buckets** are strongly consistent within a region and eventually consistent across regions; a conditional write is decided by the bucket's leader only with `X-Tigris-Consistent: true`. Writers to one table from different regions need that header or a single-region bucket. To verify in the compatibility matrix.
- **Cold and warm runs reported separately:** cold means no local caches.
- **Results include:** p50/p99 latency, bytes read, **object-store request counts**, and **estimated $** (per query, per GB ingested, per month of storage).
- **Configurations:** Every system's configuration and version is committed. Each system is tuned per its own docs, and vendors are invited to review.
- **Harness (built 2026-10-15):** `timebench` in `bench/timebench`, named on its own because it may move to its own repository: only its Timelake adapter knows Timelake. The website explains its method at /benchmarks/timebench. Our own benchmark system, extended over time with new workloads and systems: public benchmarks miss most of what a time-series lake has to do in practice (streaming commits, late data, maintenance, cost), and it isn't limited to Iceberg.
  - A **workload** is data at a named scale, the order its rows arrive in (in stages, each optionally ending at a checkpoint), and its queries (written once with dialect placeholders; a reader-specific version only where its SQL lacks a construct). Now: TSBS IoT (`small`, `standard`, `large`), TSBS DevOps cpu-only (`small`, `standard` = 4,000 hosts) and IIoT (§17.6).
  - A **system** is one adapter: create tables, append a commit, run its own maintenance, open its readers. Now: Timelake, Iceberg (PyIceberg), S3 Tables, Lance, plain Parquet files, TimescaleDB (any server: a container, the benchmark instance, Tiger Cloud), ClickHouse, QuestDB, InfluxDB 3 Core, and `reference` (the right answers, §17.6).
  - A **run** streams the workload into each system, then runs every query in a fresh process on every reader at each checkpoint (right after ingest, after maintenance), and checks that row counts match across readers.
  - **Under load (2026-10-15):** fresh readers probe each system while it ingests (query latency under writes, and whether they see every finished commit); then 1, 4 and 16 dashboard clients query at once (queries per second, p50/p95/p99). On AWS, every record carries its cost at S3 list prices; machines are not priced yet.
  - **History:** every run is one file in `bench/results/runs/` with the commit, machine, versions and all measurements. The website charts it per target. Runs from before the harness (v0, Tiger Cloud) were converted.
  - `timebench run tsbs-iot --scale small` reproduces a run; `report` and `history` read them back.
- **Weekly regression run (decided 2026-10-15):** `bench/profiles/weekly.toml` every Monday (and `iiot.toml`, industrial IoT, every Thursday, so each fits in a day) on one fixed instance in us-east-2 (c8gd.2xlarge, local NVMe), started by a GitHub Actions workflow through a role limited to that instance. The instance stops itself when done; an hourly job commits finished runs, so a run may take longer than a GitHub job. Same workloads, scales and pinned competitor versions each week, so changes in the history come from Timelake. No benchmark runs in CI.
- **Spike: a CI gate on request and byte counts.** Those counts don't depend on the machine, so they could catch regressions (an extra metadata read, a lost pruning step) on every push. **Measured (2026-10-15, `spikes/request-counts`):** across three identical runs, query request counts were exact or within one or two requests, bytes within 0.5%, and Timelake's ingest within 0.1%. A gate, Timelake only at `small` scale (3–4 minutes), could require exact counts after maintenance (identical in every run) and allow 5% and 3 requests elsewhere. Not built yet: weekly runs only, for now.

### 17.5 Phase 0 baseline (2026-10-11)
Full results: [`bench/results/BASELINE.md`](bench/results/BASELINE.md).
- **Coverage:** TSBS IoT (500 trucks × 3 days), TPC-H SF10, and ClickBench (10% subset), on DuckDB, DataFusion, Polars, DuckDB over Iceberg, and TimescaleDB. Local disk, M1 Pro.
- **No engine dominates.** Polars won TSBS and TPC-H, DataFusion won ClickBench, and DuckDB was close behind on all three. DataFusion is weakest on join-heavy TPC-H, which is not Timelake's workload.
- **TimescaleDB vs. lake:**
  - TimescaleDB is **~100× faster on last-value lookups** (2–3 ms vs. 34–317 ms). → The last-value index / hot store is mandatory.
  - The lake engines are **4–44× faster on analytics** and use **14× less storage** (vs. *uncompressed* TimescaleDB).
- **Sorted, day-partitioned Iceberg beat unsorted Parquet** in DuckDB (`last-loc` 2× faster). This validates time-aware layout and compaction.
- **DuckDB pitfall:** `time_bucket(INTERVAL '1 day', timestamptz)` is ~130× slower than `INTERVAL '24 hours'` (§11.2).
- **Object storage (RustFS + 20 ms per request, [`OBJECT_STORAGE.md`](bench/results/OBJECT_STORAGE.md)):**
  - First runs are 2.7–6.4× slower than local disk.
  - Repeat speed depends on in-process caching: DuckDB caches, DataFusion/Polars don't. → The reader cache is a priority.
  - Iceberg metadata adds round trips. → Catalog-side planning matters.
  - The latest-value query downloads ~165 MB.
- **Parquet vs Vortex ([`VORTEX.md`](bench/results/VORTEX.md)):** Vortex is faster locally (2.4–4× on single-device queries), at parity to slower on object storage (more requests), and not smaller in our layout. → D16.
- **DataFusion (Rust) vs DuckDB on object storage:** DataFusion was faster on first runs (TSBS 0.29 s vs 0.35 s; single-device 0.12 s vs 0.22 s) but slower on repeats (DuckDB caches file data). → D3 plus a Timelake data cache.
- **Parquet filter pushdown** in DataFusion is 1.2–1.6× *slower* on object storage (extra range requests) and slightly faster locally. Enable it per query shape.
- **Real S3 (2026-10-13, laptop → us-east-2, [`S3.md`](bench/results/S3.md)):**
  - Right after 1,728 five-minute commits, TSBS queries take **1.5 s on Timelake** vs 3.3 s on Lance and 20–22 s on naive Iceberg and **real S3 Tables**, with 65 requests per query vs 1,500–2,800.
  - S3 Tables' managed compaction ran 2.4–2.6 h after ingest. After it, Timelake was still faster: 1.2 s vs 2.4 s on TSBS, 1.0 s vs 6.7 s on single-truck queries.
  - Timelake commits fastest: 0.48 s median, vs 0.99 s for Lance, 1.46 s for S3 Tables and 2.41 s for Iceberg.
  - Every engine is 9–24× slower on S3 than on local disk for first runs. → The reader cache matters.
  - Lance reads stale versions on S3 Express One Zone (unsorted listings); Timelake doesn't.

### 17.6 IIoT benchmark (first version built 2026-10-15)
TSBS models a truck fleet at ~10 s per device. Industrial plants look different, and that is the market (Polythings' IIoT platform). The IIoT benchmark is a timebench workload (`iiot`) and runs next to TSBS, not instead of it.

**Principles**
- **Every opponent, one table.** Timelake, InfluxDB 3 Core, TimescaleDB, ClickHouse, QuestDB, naive Iceberg (the S3 Tables write pattern) and Lance.
- **Honest, including where we lose.** Timelake is a lakehouse, not a database. A database on local disk answers point lookups in milliseconds, handles many concurrent dashboards and shows data within a second; Timelake doesn't (yet; a cache layer may close part of the gap). We publish those losses next to the wins.
- **Always in context of cost and scale.** Each result sits next to what it costs per month on AWS (compute + storage + requests) and how it changes as data grows. Readers need statements of the form "system X answers in N ms on an instance costing $A/month; Timelake answers in M ms for $B/month of S3", with measured numbers.
- **Reproducible and reviewable.** Generator seeds, configs and versions are committed; each system is tuned per its own docs; vendors are invited to review.

**Data.** A deterministic Rust generator replays plant behavior faster than real time:
| Workload | Shape |
|---|---|
| Process historian | 100k+ tags, narrow rows (tag, time, value, quality), mixed rates: 1 s, 10 s, 60 s and change-of-value |
| Condition monitoring | Vibration bursts at 1–10 kHz from hundreds of machines, uploaded in 1–10 s blocks |
| Store-and-forward | Gateways offline 1–12 h, then backfill out of order while live data continues; 1–2% duplicates (at-least-once delivery) |
| Meters and fleets | Millions of slow series arriving as periodic batch files (cardinality stress) |

**Scale tiers** by raw ingest rate (uncompressed row bytes): **10 MB/h, 100 MB/h, 1 GB/h, 10 GB/h**. Each tier runs long enough to hold 1 day, 7 days and 30 days of data, and queries are measured at each checkpoint, so every chart shows the trend as data grows.

**Queries** an operator, engineer or report actually runs, all while ingest continues: dashboard refresh (latest value of 1,000 tags; last 15 min of 20 tags), 30-day trend downsampled to ~1,000 points, hourly shift report per asset joined with asset metadata, raw window around an alarm (50 tags, ±10 min), time above a limit, time-weighted average, energy from cumulative counters, and a concurrency run (N dashboards refreshing at once).

**Measured for every system and tier**
- Query latency p50/p95, cold and warm, under ingest; concurrent throughput.
- Freshness: arrival → queryable.
- Sustained ingest rate and the smallest setup that keeps up.
- Maintenance work: CPU time, bytes rewritten, extra services or jobs to run.
- Storage: bytes and object count.
- **$ per month on AWS:** instance hours (or Lambda time) for each system's servers or writers, plus S3 storage and requests by type (GET, PUT, LIST, DELETE, counted by `bench/tools/s3proxy`), plus managed fees where they apply (S3 Tables compaction).
- Where each system keeps data: local disk, EBS, object storage, or a mix (ClickHouse on S3 disks, QuestDB and TimescaleDB on local disk, InfluxDB 3 Core on object storage). Results say so next to every number.

**First version (decided and built 2026-10-15)**
- **Scope:** the process historian with store-and-forward. Condition monitoring (vibration blocks) and metering fleets come later.
- **Generator** (`bench/timebench/workloads/plant.py`): Python and NumPy rather than Rust, seeded per machine group and day, so it is fast enough and timebench stays one language; Rust only if generation becomes the slow part. Sites of 10 areas, areas of 10 lines, lines of 10 machines, 10 tags per machine (pressure at 1 s; flow, temperature, current and speed at 10 s; vibration and an energy counter at 60 s; state, alarm and setpoint on change with an hourly heartbeat). Machines run, stop and fault; pressure follows its setpoint and sometimes exceeds the alarm limit; counters reset; sensors drop out and get stuck. Each line's gateway (but every tenth, wired) goes offline for 1–12 h; its backlog arrives afterwards at an hour per 5-minute window, the last 5 minutes before the outage are sent again, and 1.5% of rows arrive twice. Every injected event is written to `events.parquet`, the agent benchmark's ground truth (§17.8).
- **Scales:** `small` 1,000 tags (~10 MB/h raw), `standard` 10,000, `large` 100,000 (30 days each), `xlarge` 1,000,000 (7 days).
- **A run** loads history in bulk (an hour of arrivals per commit) with queries after 1 and 7 days, then the last day live (a 5-minute window per commit, paced, probed), queries at `ingested`, maintenance, queries at `maintained`, then concurrent dashboards. Queries are relative to the data's present (`@AGO(15 minutes)`), so they mean the same at every checkpoint.
- **Correct answers:** a `reference` system keeps every arrival and reads it with duplicates removed; every other system's answer (row count and per-column sums) is checked against it. Systems that keep duplicates or miss late rows show as wrong next to their speed.
- **Systems:** Timelake (dedup on), TimescaleDB (unique index, `ON CONFLICT DO NOTHING`), ClickHouse 26.8 LTS (`ReplacingMergeTree`, read with `FINAL`), QuestDB 10.0 (WAL tables with `DEDUP UPSERT KEYS`, loaded over QWP), InfluxDB 3 Core 3.12 (line protocol; last write wins), naive Iceberg, Lance and plain Parquet. The servers run in Docker at pinned versions (`bench/infra/servers.sh`): first on the weekly instance, one server running at a time (started for its system's turn and stopped after), and part of every weekly run (decided 2026-10-15); published comparison runs move to dedicated instances, one per database plus a client.
- **InfluxDB 3 Core's query file limit:** by default a query reads at most 432 Parquet files, about 3 days of data. It runs twice: with the limit raised as its docs allow (`influxdb3`), and as shipped (`influxdb3-defaults`), so the results show both.
- **Later:** the same workload against the vendors' managed clouds (ClickHouse Cloud, InfluxDB's and QuestDB's managed offerings), measured in us-east-2 at their own prices (decided 2026-10-15).


### 17.7 Public benchmarks (reviewed 2026-10-15)
We run the public benchmarks that fit a time-series lake, so results compare with published numbers, and say why for the others. The website lists the same, with a methodology page for each benchmark we run or plan (`dashboard/lib/benchmark-catalog.ts`).

| Benchmark | Status | Why |
|---|---|---|
| TSBS IoT | Running | The base workload (§17.1). |
| TSBS DevOps / cpu-only | Running (2026-10-15) | The TSBS use case most databases publish; same generator. 4,000 hosts, streamed and as a bulk load (rows per second). |
| TSBS with its own tools | Planned, after F8 (decided 2026-10-15) | timebench uses TSBS's generator and queries but its own method (§17.1). For numbers comparable with published TSBS results, run TSBS's loader and query runner against Timelake's server mode through the Postgres wire protocol (F8, §21.8), as for TimescaleDB; or add a Timelake target to TSBS. |
| ClickBench | Planned | A 10% subset runs in development. Comparable numbers need the full dataset on its reference machine (c6a.4xlarge). An official entry waits until the system is more refined (issue #90). Its SQL is CC BY-NC-SA, so it stays out of the repository. |
| RTABench | Planned | Fresh data, selective lookups and joins. Its pre-aggregate queries wait for rollups (§10). |
| TSM-Bench | Planned | Seven query templates, queries while ingesting, compression. Its repository has no license: ask the authors first. |
| IoT-benchmark (Apache IoTDB) | Planned | Batch size, writer concurrency, out-of-order data and read/write mixes. Its Java adapters need a server endpoint: F8 (§21.8). |
| LST-Bench | Method only | Rounds of appends, compaction and expiry on our own data, charting how queries degrade; not its TPC-DS workload. |
| TS-Benchmark (ICDE 2021) | Not planned | Unmaintained since 2022; its five queries are covered above. |
| TSDBBench / YCSB-TS | Not planned | Unmaintained since 2019. |
| TPCx-IoT | Not planned | TPC rules allow only audited results; 1 KB key-value records don't suit columnar files. |
| STAC-M3 | Not planned | Closed, member-only, paid audits. |
| TSRBench | Not planned | Measures LLM reasoning with the series pasted into the prompt, so storage plays no part. See §17.8. |

### 17.8 Agent benchmark (planned, design to agree)
Most people now ask questions of their data through LLM agents. The question: does an agent answer more correctly, with fewer tokens and tool calls, when the data is in Timelake? TSRBench doesn't answer it: its models get the series as text or a plot, never through tools. Benchmarks where agents do use tools are close but not it: OpenRCA (root cause over raw telemetry files), AssetOpsBench (industrial assets behind MCP servers), AgentFuel (IoT and telecom questions), Grafana's o11y-bench (answers checked against live queries), DataAgentBench (several databases).

- **Design:** one agent loop (same model, prompt, turn limit and seed) against the same data in different arms: raw files with a Python sandbox; DuckDB over Parquet; TimescaleDB (same column comments for metadata); Timelake through generic SQL tools; Timelake through its MCP tools (F1, §21.1). Comparing arms separates the engine from the tool design; switching single features off (metadata, downsampling, helpful errors, result caps) shows what each is worth.
- **Tasks:** generated from the workload generators with injected events whose answers are known: lookups with units, windowed aggregates, comparing periods, anomalies with their time ranges, a root cause shared by many devices, gaps vs zeros, late data and "as of" questions, counters across resets, long trends that only fit downsampled, and questions that must be refused.
- **Scoring:** a typed `submit_answer` (value, unit, entities, interval), checked with tolerances; an LLM judge only for the explanation. Per task: accuracy, tokens, tool calls and errors, bytes returned to the model, time and dollars.
- **Then:** run OpenRCA's metric tasks and AssetOpsBench's IoT scenarios against Timelake as published comparisons.
---

## 18. Open questions

| # | Question | Current leaning |
|---|----------|-----------------|
| Q1 | Iceberg v2 or v3 as the minimum? v3 brings `timestamp_ns`, variant, and deletion vectors (needed for D6), but reader support varies. | **Research:** track v3 read/write support in DuckDB, Spark, Trino, Athena, Snowflake, PyIceberg, and `iceberg-rust`. If it is broad and stable by Phase 1, go v3-only and skip v2. |
| Q2 | `timelake-catalog`: extend/fork Lakekeeper or build on `iceberg-rust`? | Evaluate in Phase 0 |
| Q3 | Should plain-DuckDB readers ever see duplicates or uncompacted late data? | Accept briefly; the DuckDB extension removes them; aggressive compaction of recent partitions |
| Q4 | Materialize a hidden `_series_id` column or keep it index-only? | **Decided (2026-10-18): index-only.** On the IIoT benchmark (string series key, read dedup on), dedup's key comparisons took about 2 ms of a ~100 ms query; the scan took the rest, decoding rows that pruning should have skipped. An integer column would not prune better: DataFusion uses `IN` lists of at most 20 values for pruning, whatever the type. The fix is pruning for large series-key sets (#103). |
| Q5 | Default hot tier: object-store WAL (B) or Valkey (C)? | **Profile both** (latency, cost per GB, ops burden) before choosing |
| Q6 | Where does the rollup definition live: table properties, or the catalog? | Catalog when `timelake-catalog` is used, mirrored into table properties for portability |
| Q7 | ~~How mature is `iceberg-rust` for writes?~~ | **Answered (spike 1), and the v0 commit layer is implemented on its public types (D13):** append + read are solid and DuckDB-compatible on v2 and v3. Replace, row-delta, delete writers, branch commits, manifest merging, and file cleanup are missing and not extensible from outside the crate. → D13. Details: `spikes/iceberg-write/README.md` |
| Q8 | Vortex: when Iceberg adds pluggable file formats, how do we migrate (compaction rewrites to Vortex)? | Keep a file-format abstraction in `timelake-core` from day one. **Measured (2026-10-11, `bench/results/VORTEX.md`), via DuckDB and via DataFusion + `vortex-datafusion` (Rust):**
- Local disk: Vortex is 20–45% faster, and 2.4–4× faster on single-device queries.
- Object storage: Vortex is at parity to 1.5× slower with DataFusion, because it issues more requests; with DuckDB, 1.3–3× slower (that extension doesn't cache).
- Size: Vortex files are *larger* in our sorted layout.
→ D16 |
| Q9 | ~~License~~ | **Resolved:** Apache 2.0 (D7) |
| Q10 | Rule engine scope: a SQL-defined rules DSL, or YAML presets plus SQL escape hatch? | YAML presets + SQL |
| Q11 | ~~Legal hold / WORM?~~ | **Resolved:** not in v1; GDPR purge must always be possible (D8) |
| Q12 | DataFusion vs. embedded DuckDB as the internal engine (D3) | DataFusion, pending the Phase 0 spike. The spike runs the TSBS IoT queries (§17.1) on our Parquet layout in DataFusion, DuckDB, and Polars, since general benchmarks disagree and depend on the harness. Polars is evaluated as a reference, not as a candidate engine: its Rust API is secondary and it lacks DataFusion's extension points. |
| Q13 | ~~GDPR purge timing defaults~~ | **Resolved:** daily batched purge; `--now` for immediate; alert if `max_purge_delay` (7 days) is exceeded |
| Q14 | ~~Backfill conflict default~~ | **Resolved:** existing rows win; `prefer = backfill` override per job |
| Q15 | ~~Auto-routing late bulk data to backfill~~ | **Resolved for v1:** manual opt-in only; automatic routing later (threshold tuned by benchmarks) |
| Q17 | Rule state on Lambda: object-store checkpoints vs. hot store; sweep cadence for heartbeat rules | Prototype in Phase 6 |
| Q18 | F1: are agent permission sets open source or premium (§20)? Policy file format: our own JSON, or an existing one (Cedar, OPA/Rego)? | Open source enforcement in the reader; our own small JSON format first |
| Q19 | F2: tile format (Arrow IPC, Parquet, or JSON) and tile sizes per resolution; how much regeneration a commit may trigger | Arrow IPC (fastest in `timelake-wasm`), sizes chosen by benchmark; debounce per model |
| Q20 | F3: is the commit-driven trigger an object-store notification (S3 events), a catalog event, or a poll of the newest snapshot? | Support all three; poll as the portable fallback |
| Q21 | F4: how to run user Python safely in a hosted service | Out of scope until F4; the self-hosted version runs in the user's own process |
| Q22 | F5: which Iceberg version and features Snowflake reads from external catalogs, how fast it picks up new snapshots, and whether connection setup is open source or premium (§20) | Verify against Snowflake before Phase 2; leaning open source for the connector, premium for managed setup |
| Q23 | F6: what the device-side local store is: a local Timelake table, or a lighter append log (Arrow IPC) that the agent converts on upload | Local Timelake table first, since it reuses everything; measure write cost on small devices |
| Q24 | F6/F7: open source vs premium. Fleet management (config push, updates, health of thousands of devices and gateways) is a natural premium layer (§20) | Sync and gateway open source; fleet management premium |
| Q16 | ~~Which import sources ship first?~~ | **Resolved:** Phase 1: CSV, Parquet, Iceberg/S3 Tables. Phase 2: TimescaleDB/Postgres with `--sync` cutover. Then InfluxDB, others via the DuckDB-backed source. Historian connectors are premium (D12) |

---

## 19. Roadmap (draft)

### 19.0 v0: a faster, maintenance-free time-series table on S3 (in progress)
**Value proposition.** "Better than naive S3 Tables or Lance for time series, with no Glue jobs." A team that today appends micro-batches to S3 Tables (Firehose, PyIceberg, Spark) and reads with DuckDB/Athena should get, by switching to Timelake v0:
- the same open Iceberg tables (plain DuckDB still reads them),
- on any object store (S3, R2, GCS, Azure, RustFS/MinIO, local disk),
- with time-series layout and maintenance built in (sort, compaction, dedup, retention, snapshot expiry), run inline by the writer or as one stateless `timelake maintain` command,
- and much faster time-series reads through the Timelake reader, including a last-value index.

**In scope (v0):**
| Area | v0 deliverable |
|------|----------------|
| Catalog | Object-store-only catalog: `vN.metadata.json` committed with put-if-absent (conditional write), `version-hint.text` for Hadoop-style readers (DuckDB `iceberg_scan`). No server. |
| Contract | `time_column`, `series_key`, `chunk_interval` (hour/day/month), dedup on/off, retention, all as `timelake.*` table properties. Tables without a time column (dimensions, e.g. device metadata) are allowed. |
| Writer | Append Arrow batches → align to table schema → sort by `(series_key…, time)` → split by time partition → Parquet (ZSTD, page index, time-series encodings). Idempotent batch IDs. |
| Commit layer (D13) | Own append / replace / delete commits on iceberg-rust public types, manifest merging, optimistic retry. Iceberg v2 by default. |
| Last-value index | A small Parquet sidecar per snapshot with the latest row per series (pointer in the snapshot summary). Updated on every append. |
| Maintenance | Compaction (merge, re-sort, dedup, latest write wins), retention (drop expired partitions), snapshot expiry + unreferenced-file cleanup. Inline auto-maintenance after appends (thresholds) or `timelake maintain`. |
| Reader | DataFusion `TableProvider`: file pruning on Iceberg column bounds, Parquet row-group/page pruning, `time_bucket`, and automatic rewrite of latest-row queries (`ROW_NUMBER() … = 1`, `ORDER BY time DESC LIMIT 1`) onto the last-value index. |
| Interfaces | `timelake` CLI (create, append/import Parquet+CSV, sql, latest, maintain, info, duckdb) and a Python package (PyO3): `Lake`, `create_table`, `append`, `sql`, `latest`, `maintain` with Arrow in and out. |
| Benchmark | Same ingest stream (TSBS IoT, 5-minute micro-batches) into naive Iceberg (PyIceberg appends, the S3 Tables write pattern), Iceberg after binpack compaction (S3 Tables' default maintenance), Lance (append, then compacted), and Timelake. Then TSBS IoT + single-device queries, local and on RustFS with 20 ms latency. |

**Out of scope (v0):** REST catalog server, hot tier / WAL, rollups, deletes/GDPR, backfill branches, TimescaleDB import, `timelake-ingest` service and rules, DuckDB extension, Vortex, schema evolution beyond add-column.

**Exit criteria:** On RustFS + 20 ms, Timelake's reader beats naive Iceberg + DuckDB and Lance on the TSBS IoT and single-device suites (geomean, first run), latest-value queries take tens of milliseconds, plain DuckDB reads every Timelake table with identical results, and no maintenance job outside Timelake is needed.

**Status (2026-10-12): implemented; exit criteria met** ([`bench/results/V0.md`](bench/results/V0.md)).
- **S3 (RustFS + 20 ms), first runs:**
  - Right after streaming ingest: 3.7× faster than Lance and 16× faster than naive Iceberg + DuckDB on TSBS IoT; 7.8–9× faster on single-truck queries.
  - After each system's maintenance: 1.2–1.5× faster on TSBS IoT, 2.2–2.7× on single-truck queries.
- **Local disk:** 2.7–3.8× faster on TSBS IoT, 7.7–10× on single-truck queries.
- **Latest-value queries:** 12 requests instead of 1,700–3,600.
- **Ingest on S3:** 2× faster than Lance and 3.7× faster than PyIceberg, including inline compaction.
- **Live size:** 228 MB, vs 262 MB (Iceberg) and 449 MB (Lance).
- **Compatibility:** plain DuckDB reads all tables with identical results.

Reader lessons, now defaults:
- Load all of a query's tables concurrently before planning.
- Read each Parquet footer once.
- Use more scan partitions on object storage.
- Write 1M-row row groups (page indexes keep single-series reads selective).
- Declare the sort order only when there are enough files to keep scans parallel.

Open from v0:
- A reader data cache: DuckDB still wins some repeat runs on its cached data. Planned as P5 (§19.1).
- Plain DuckDB on Timelake files is slower than on PyArrow-written files on S3 (0.52 vs 0.42 s), which needs investigation.
- The REST catalog.

| Phase | Deliverable |
|-------|-------------|
| 0 | This spec. `timebench` skeleton: TSBS IoT generator → Parquet, adapters for DuckDB+Iceberg and TimescaleDB as first baselines. Spikes: ✅ `iceberg-rust` write maturity (Q7); `replace` + deletion-vector commits via our own commit layer (D13); DataFusion vs. embedded DuckDB (Q12); Lakekeeper evaluation (Q2); Iceberg v3 engine-support survey (Q1). Benchmark harness. |
| 1 | `timelake-core` writer (contract, sorting, partitioning, batching, commits); `timelake-catalog` (object-store backend, REST spec); CLI create/adopt/query; **import** (CSV, Parquet, Iceberg/S3 Tables) and **backfill** (plan, final-layout write, branch staging, publish, resume); DuckDB reads committed tables |
| 2 | `timelake-maintain`: compaction, re-sort, dedup, retention, snapshot expiry, triggered by catalog events. Immutable deletes + audit table; GDPR erasure workflow. **TimescaleDB/Postgres import** with `--sync` cutover. Snowflake connection (F5, §21.5), first through metadata-file registration, then through the REST catalog. |
| 3 | Rollups (incremental, hierarchical) + `timelake-functions` core (`time_bucket`, `time_buckets`, `forward_fill`, `time_weighted_mean`, `counter_diff`, quantiles) |
| 4 | DuckDB extension (functions, dedup-on-read, index-aware scans) |
| 5 | Hot tier (profile WAL vs. Valkey; ship both behind `HotStore`) + Timelake reader with merge, Puffin indexes, fast paths |
| 6 | `timelake-ingest` with MQTT/HTTP/Kafka and the rule engine + events table; data → events with pluggable sinks and commit-driven triggers (F3, §21.3); edge gateway (F7) and device sync (F6) |
| 7 | Python package (PyO3 + maturin), docs site with recipes, published benchmarks; Vortex when Iceberg supports it |
| 8 | Agent access with permission sets (F1) and static read models for dashboards (F2) (§21) |
| Later | Computed metrics from user-defined Python functions (F4, §21.4) |

### 19.1 Performance plan (in progress)
**Status:** P4 done (2026-10-13); P1–P3 done (2026-10-14, §19.2). P3 measured no clear gain and is off by default. P7's first version done (2026-10-16, §11.1.1); P5 done (2026-10-17, §19.3). P6's staleness bound built (2026-10-17, below). Next: P8, the rest of P7, and P6's S3 Express pointer. The history of each step and its effect is on the docs page "Performance history". The codebase and CI/CD cleanup it waited for is done (2026-10-13; the audit and plan are in git history at `2d66939`). Gains below are estimates until measured; measurements are in [`bench/results/PERFORMANCE.md`](bench/results/PERFORMANCE.md).

**Where time goes today** (Timelake reader, laptop → AWS S3 us-east-2, after maintenance, [`bench/results/S3.md`](bench/results/S3.md)):

| Query | First run | Best run | Requests | MB |
|---|---:|---:|---:|---:|
| `one-truck-latest` | 618 ms | 2 ms | 7 | 0.2 |
| `last-loc` | 737 ms | 68 ms | 14 | 0.3 |
| `long-daily-sessions` | 1.54 s | 0.30 s | 32 | 11 |
| `avg-vs-projected-fuel-consumption` | 2.42 s | 2.39 s | 53 | 37 |

- **Small queries:** first runs are dominated by round trips that each wait for the previous one: version hint → `vN.metadata.json` (and a HEAD on `vN+1`) → manifest list → manifests → last-value index or Parquet footers → data. That is 6–7 dependent steps at about 90 ms each from a laptop.
- **Heavy queries:** bound by bytes read; warm runs are barely faster.
- **Commits:** p50 was 483 ms, from 5 serial stages: data, then manifests and index, then manifest list, then the conditional metadata write, then the hint. P4 made it 2 stages and 243 ms.
- **Warm queries:** every query re-reads the version hint to stay current. That re-read is most of `last-loc`'s 68 ms.

**Steps, in order:**

| # | Change | Expected effect |
|---|---|---|
| P1 | **Done.** **One-request table state.** Each commit also writes a small head object: version, live files with column bounds, and the last-value index (inline when small, else a pointer). Readers fetch it with one GET instead of 4 dependent ones. Iceberg metadata is still written as today for other engines. | Cold `one-truck-latest` 502 → 314 ms on S3, 868 → 318 ms on Tigris, measured from a laptop |
| P2 | **Done.** **Footers in the head object.** Store the Parquet footers and page indexes of live files next to the file list, so scans skip the footer round trip. | Cold scans 2 round trips instead of 3; Tigris `one-truck-history` 1,187 → 522 ms (with P1) |
| P3 | **Done.** **Connection pre-warm.** Open the scan's connections while the head object is in flight. | Measured at most about 20 ms on cold scans, nothing on point queries: off by default |
| P4 | **Done.** **Two-step commit.** Upload data, manifest, index and manifest list in parallel (all are known locally), then make the one conditional write. Update the hint asynchronously. | Commit p50 483 ms → 243 ms measured (whole v0 ingest 1,307 s → 795 s) |
| P5 | **Done (§19.3).** **Block cache**, in memory and optionally on local disk across processes, for data files, manifests and the last-value index (§12.2 item 6). | Repeat queries at best-run speed; measured: TSBS IoT repeats on S3 54 → 16 ms, 16 clients 288 → 398 queries/s |
| P6 | **Staleness bound built (2026-10-17):** setting `version-check-interval` (table, session; default 1 s, the planning cache's old lifetime). A reader uses a version it loaded or checked that long without the per-query check; writers always check, and a process sees its own commits at once. `0s` checks on every query (one query's lookups share one check). **Not built:** the head pointer on S3 Express. | One request and round trip less per warm query within the interval (unit test: warm `latest` 1 → 0 round trips); not measured on S3 yet |
| P7 | **First version done (§11.1.1); coarser queries, fewer keys and rollups of rollups done (§11.1.2, daily per tag 2.07 → 0.25 s).** **Rollups (§11.1).** TSBS daily queries read hourly or daily rollups instead of raw data. | Likely 10–100× on bandwidth-bound dashboard queries; an hourly query over a week of 200 devices went 113 → 21 ms on a laptop (debug build) |
| P8 | **Started (2026-10-17).** **Scan tuning:** late materialization (setting `filter-pushdown`: `auto` is on for local files, off on object storage), request sizing (#65, done for the Rust reader), pruning for `IN` lists of more than 20 values (#103 done, setting `in-list-pruning`: literal lists, and joins' runtime filters on object storage; Q4's hidden `_series_id` decided against), a newest-first scan for `ORDER BY time DESC LIMIT k` (#72, built: setting `newest-first`, 100 newest before a past time on 30 days 298 → 38 ms), windows over each series in time order read merged in series order (built: setting `window-series-order`, `auto` when the files are in one time partition; IIoT `gaps` 260 → 215 ms, a week of files 2.0 → 3.0 s so `auto` leaves those). | Less data decoded and fetched on selective scans; measured: pushdown cuts TSBS geomeans 4–5% on local disk (selective group-bys up to 40%), costs first queries 12–17% at 20 ms per request; the block cache fetching a read's runs at once cut first queries on S3 23% (162 → 125 ms), and GETs of at most 1 MiB another 9% at 50 MiB/s per request; long `IN` lists 54 → 31 ms (100 tags, IIoT, local) |
| P10 | **Next (2026-10-18).** **Series cardinality (R27):** measure at the typical size (IIoT `large`: 10,000 machines × 10 tags = 100k series, and 400k) and toward the top end (`xlarge`: 1M series, then more), and fix what grows with the number of series. Known suspects: the last-value index is one object rewritten on every commit (at hundreds of millions of series it needs shards by series hash); a join's runtime filter lists at most 150 keys (`hash_join_inlist_pushdown_max_distinct_values`), so a dashboard over thousands of devices prunes by bounds only; rollup refresh and planning per series. | Ingest, latest-value and dashboard queries that stay flat from 1k to 1M series. **Measured (2026-10-18):** 100k and 400k series ingest at 2.2–2.3 M rows/s, dashboards flat (`latest-area` 52 → 53 ms); last-value index 1.8 MB, 0.15 s of a 4.5 s commit at 400k (3.4 s at 10M, in memory). Open: `gaps` grows with rows (88 s at 400k, a rollup case), the index for mostly idle series, and the index's cost per commit, which grows with the series count (2026-10-19, 1M series: a 100k-row append 264 → 171 ms by merging the sorted indexes instead of sorting them; the remaining ~80 ms is encoding the whole index, ~1.5 s a commit at 10M; options: index deltas per commit merged by readers and folded into the base by maintenance, or shards by series hash, which don't help a writer whose batches touch every shard); done: the k-way merge on interleaved inputs (up to 5× faster) |
| P9 | **Planned, after P8 and the rest of P7.** **Real data:** a beta on a company's production-like industrial dataset, with parallel writers fed from a Kinesis stream. Measure ingest throughput, commit contention between writers, query latency and the monthly bill (requests, storage, compute) on real data and access patterns, then tune for what they show. | Settings and defaults chosen from real use instead of synthetic benchmarks only |

**How we measure:** the same v0 suites through `tools/s3proxy` (request counts by kind) on local disk, S3 from a laptop, and S3 in-region. Each step lands only if it improves first-run latency or requests without slowing other queries, and plain DuckDB still reads every table.

### 19.2 One-request table state (P1–P3)

**Status:** P1–P3 built and measured (2026-10-14). From a laptop, cold `one-truck-latest` went 502 → 314 ms on S3 with P1, and 810 → 213 ms on Tigris with P1–P3 ([`bench/results/PERFORMANCE.md`](bench/results/PERFORMANCE.md)). The WASM reader and TypeScript client read the head too. Decisions taken on 2026-10-14 are recorded below.

**Goal:** a cold query learns everything it needs to plan in one GET, so a small query takes two dependent round trips (table state, then data or the last-value index) instead of six or seven.

**The head object.** After each commit, the writer also writes `<table>/metadata/timelake-head` with the state of the current version:
- the whole `vN.metadata.json` (schema, partition spec, sort order, properties, snapshots), so writers can commit from a head-loaded table too. With the default limits it is tens of KB and compresses well.
- the live data files: path, partition, row count, size, sequence numbers, and per column the bounds and counts (what the manifests hold).
- the last-value index while it fits the inline budget, otherwise only its path (in the metadata).
- P2: the Parquet footers and page indexes of the live files, in the same budget.

**Decided:**
1. **Validate the head on every load.** Results are always as current as before; trusting the head stays P6's opt-in staleness bound. How (refined while building P2):
   - **Cold load** (the process has no version of the table): on stores that list by prefix (S3, GCS, Azure, local disk), LIST `metadata/v` together with the head GET. The listing names the newest version, so a current head is confirmed in the same round trip and an outdated one is skipped for the metadata chain. Without this, the check for `vN+1` was a second round trip on the critical path, and once footers came from the head (P2) nothing overlapped it. A LIST costs like a PUT on S3 ($0.005 per 1,000), once per table per process.
   - **Warm load** (the process has version N): check whether `vN+1.metadata.json` exists, with no head download. Queries start the check and the scan awaits it; a stale version makes the scan reload the table and plan the newest one. `Lake::table()` and writers wait for it.
   - **Plain HTTPS** (no listing): GET the version hint with the head, then the deferred check.
2. **How to check for `vN+1`, per store:** a HEAD request by default; a LIST of the exact name (one key, `max-keys=1`) on Tigris, where a HEAD of a missing key takes 285 ms and the LIST 40 ms. Storage option `timelake_freshness_check=head|list` overrides it; `list` needs S3, GCS or Azure. The metadata chain uses the same check.
3. **One inline budget** for optional sections, `timelake.head-inline-bytes`, default 1 MiB: the last-value index first, then (P2) footers. The files and metadata are always in the head.
4. **Format: pluggable, Arrow IPC by default.** The head's content is format-neutral: string properties plus named Arrow tables (`metadata`, `files`, `columns`, `last_values`). A `HeadCodec` encodes it, and the head's header (`TLHD`, header version, table version, codec name) names the codec, so readers decode any head whose codec they have. Built in: `arrow` (Arrow IPC streams, zstd) and `json` (Arrow's JSON rows plus serde schemas). Avro, Vortex, BSON or Parquet are one more codec each (all have Arrow bridges), registered with `Lake::register_head_codec`. Tables choose with `timelake.head-format` (`none`: no head; a writer without the chosen codec deletes the head rather than leave a stale one).

**Writing it.** Iceberg metadata is written exactly as before and `vN+1.metadata.json` stays the commit point, so DuckDB, Spark and PyIceberg see no change. The head is a cache of that commit, written in the background like the version hint (P4), so commits don't get slower. The writer reads the first bytes of the stored head (version and ETag) and replaces it only with a conditional write on that ETag, so racing writers never move it back to an older version. Stores without conditional updates (local disk) overwrite. A commit knows its new file list from its base when the base was loaded from a head, so writing the head reads no manifests.

**Reading it.** The head and the listing (or the version hint) are read at the same time. A table without a head (another engine, an older Timelake, a head removed by orphan-file cleanup) loads exactly as before, with no extra round trip; a listing or hint newer than the head skips the head.

**Measured round trips** (unit test `cold_queries_read_the_head_first`): cold `latest` 3 → 1, cold filtered scan 6 → 2.

**Known limit:** on plain HTTPS (no listing), if an older Timelake (which writes no heads) commits more than `write.metadata.previous-versions-max` (20) times after a newer one wrote a head, and expiry deletes `vN+1.metadata.json`, a reader can trust the old head. Fix: delete `metadata/timelake-head` after writing a table with an old version, or upgrade every writer.

**P2, footers.** The head carries the Parquet footers (encoded with `ParquetMetaDataWriter`, page indexes included) of the newest files, newest partition first, in what the last-value index leaves of the budget. The writer reads each new file's footer once in the background (one GET) and keeps footers in memory, so later heads don't read them again. Scans put footers from the head straight into DataFusion's metadata cache.

**P3, warm connections.** While the head GET and the listing are in flight, a cold load sends a few small HEAD requests (on the version hint), so the data reads that follow find open connections. Storage option `timelake_warm_connections`. Measured on S3 and Tigris: at most about 20 ms off cold scans, nothing on point queries, so it is off by default.

**Cost option.** `timelake_version_lookup=hint` makes a cold load read the version hint instead of listing (one more round trip, no LIST). Options are named for the request they change, not for an outcome ("cheap"), so the trade-off is visible; the docs explain each one and why its default was chosen. Requests per operation and their prices are documented in the storage docs ("Requests and cost").

**TypeScript client and WASM reader.** The client GETs the head and the hint together; the WASM reader decodes heads written with the built-in codecs (the codecs and header live in `timelake-format` under its `head` feature, shared with the engine). A HEAD for `vN+1` runs alongside the query's data reads; if a newer version exists, the result is dropped and the query reruns on the metadata chain from `vN+1`. `latest` answers from the inline index without a request, and scans open files from inline footers.

### 19.3 Block cache (P5)

**Status:** built 2026-10-17. Measured on the benchmark instance on S3 (`bench/results/PERFORMANCE.md`, "P5"):
repeat runs of TSBS IoT 54 → 16 ms (geometric mean) with no requests, 1 / 4 / 16 clients 22 / 94 / 288 → 133 / 339 /
398 queries/s; TSBS DevOps repeats 85 → 27 ms. A second process with the disk cache warm: IoT first runs 151 → 71 ms.
256 KiB blocks gained nothing and cost more requests on first runs, so 1 MiB is the default.

**Why.** Within one process, table metadata, manifests and footers are already cached in memory, but every repeat
query on object storage fetches its data again. In the weekly run of 2026-10-16 on S3, repeat runs took 32 ms
(`one-truck-last-hour`, 4 requests) against 3 ms on local disk, and 135 ms (`cpu-max-all-8`, 8 requests) against
14 ms; 16 clients on S3 reached about half the local throughput. One request per query remains either way: the check
for a newer table version, which P6 addresses.

**Design.**
- A caching layer wraps the object store, so every read goes through it: DataFusion's Parquet reads, manifests,
  manifest lists, the last-value index and footers.
- Only immutable objects are cached: `*.parquet` and `*.avro` (data files, manifests, manifest lists, last-value
  index files), whose names carry a UUID and which Timelake never rewrites. The version hint, the head, table
  listings, `vN.metadata.json` and `timelake-tables.json` always go to the store, so freshness is as before.
- Objects are cached in aligned blocks (`timelake_cache_block_bytes`, default 1 MiB). A read takes the blocks it
  covers; missing blocks next to each other are fetched in one ranged GET, and concurrent reads of the same missing
  block share one fetch. The store's answer also gives the object's size, recorded with its blocks; a block whose
  length doesn't fit that size is treated as missing.
- Writes of cacheable objects put their blocks in the cache too, so a process reads what it just wrote without a
  request. Deleting an object drops its blocks.
- Two levels: memory (least recently used blocks evicted past `timelake_cache_memory_bytes`, default 256 MiB), and
  optionally local disk, shared by every process on the machine (`timelake_cache_disk_bytes`, default 4 GiB, in
  `timelake_cache_dir`, default the OS cache directory). Disk blocks are written to a temporary name and renamed into
  place; a process that finds the directory over its cap deletes the least recently used blocks (by modification
  time, refreshed on use at most once a minute). A block deleted while another process reads it is fetched again.
- `timelake_cache = off | memory | disk`: memory by default for object stores and HTTPS, off for local disk.
- Hits, misses and bytes fetched are counted per lake (`Storage::cache_stats`, `Lake.cache_stats()` in Python, and
  per run in the benchmark output).
- Not covered: the WASM reader and the TypeScript client (the browser has its own HTTP cache).

**Measure:** the S3 query-only runs and the 16-client test on the benchmark instance, and a new CLI process running
`one-truck-last-hour` against a warm disk cache, before and after; block sizes of 256 KiB and 1 MiB.

### 19.4 Versions, releases and demos

**Decided 2026-10-14:**
- **One version for everything** (crates, CLI, Python package, WASM reader, TypeScript client, website), following [Cargo's semver rules](https://doc.rust-lang.org/cargo/reference/semver.html). `cargo-semver-checks` runs on the library crates in the release script (`scripts/release.py`) against the previous tag. Every version is a git tag `vX.Y.Z` with a GitHub release, so performance claims name a version that can be checked out and re-measured. 0.1.0 (before the performance work) and 0.2.0 (two-step commit) were tagged after the fact; 0.3.0 is the first release cut with the script.
- **Table format version** `timelake.format-version`, separate from the software version and a semantic version of its own (`0.2.0`), so compatible changes don't run the number up. A patch bump is compatible both ways. From 1.0, a minor bump breaks older writers (they refuse to write) and a major bump older readers (they refuse to read); before 1.0, as in Cargo, a minor bump breaks both. Every Timelake reads and writes older formats. The integer `N` written by Timelake 0.3.0 reads as `0.N.0`. Decided 2026-10-14, after 0.3.0 had shipped integers. Refusing to read is the simple rule for now; reading a newer format as plain Iceberg (ignoring the head and index) is the alternative if refusals get in the way.
- **`timelake upgrade`** brings tables to the current format without data loss: each step is an ordinary commit, older snapshots stay until expiry, and `--dry-run` shows the plan.

**Publishing (set up 2026-10-14, first release 0.4.0):** each `vX.Y.Z` tag starts two workflows. `release.yml`, generated by dist, builds the CLI for Linux x86_64/aarch64, macOS arm64/x86_64 and Windows x86_64, with shell and PowerShell installers and GitHub build attestations, and creates the GitHub release from the changelog. `publish.yml` publishes with trusted publishing (OIDC, no stored tokens): `timelake-format`, `timelake` and `timelake-cli` to crates.io, `timelake` to PyPI (abi3 wheels for the same five targets, plus the sdist) and `@timelake/client` to npm (compiled to JavaScript, with provenance). It publishes only once every artifact builds, skips versions a registry already has, runs without build caches, and waits for a maintainer's approval per registry. Release artifacts use the `dist` Cargo profile (no debug info, thin LTO): with line tables the Linux builds were 160 MB, over PyPI's 100 MB limit. Releases are cut when a batch of changes is worth installing, not per change; patch releases for urgent fixes. crates.io and npm can't create a package through trusted publishing, so each package's first version is published by hand; PyPI's pending publisher creates the project.

**Later: comparison demos on the website.** Live demos that run the same queries against Timelake and other engines (DuckDB on plain Iceberg, PyIceberg, a TimescaleDB or ClickHouse instance) over the same data, showing time per query and a running cost counter (requests, bytes, and dollars at the store's prices), so visitors feel the difference rather than read about it.

---

## 20. Open source vs. premium (draft, D12)

| Open source | Premium (candidates) |
|-------------|----------------------|
| Table conventions, writer, catalog, maintenance jobs | Industrial historian connectors (AVEVA PI, Ignition, PHD, IP.21), OPC-UA at scale |
| Ingestion pipeline + built-in rules | Managed/hosted control plane, UI, alert management |
| Rollups + function library, DuckDB extension, Python library | Enterprise SSO/RBAC, compliance reporting (GDPR erasure certificates as a service) |
| Import: CSV, Parquet, Iceberg/S3 Tables, Delta, TimescaleDB/Postgres, InfluxDB | Support / SLAs |

The principle: everything needed to run the lake well is open; integrations with proprietary industrial systems and managed operations are premium. The open-source part is Apache 2.0 (D7); premium components would live outside this repository.

---

## 21. Planned features

New product features beyond the original requirements, recorded on 2026-10-13. None of them is built yet. Each one keeps the rule that no always-on Timelake server is required (R13): state lives in the bucket and work is triggered by commits or schedules. F8 adds an optional server; the serverless path stays the default.

| ID | Feature | Req | Builds on | Phase |
|----|---------|-----|-----------|-------|
| F1 | Read access for LLM agents, with permission sets built in | R19 | Reader, multitenancy (§14) | 8 |
| F2 | Static read models in the bucket, generated from the frontend's Zod schemas and refreshed on ingest | R20 | `@timelake/client`, snapshot diffs (§11.1) | 8 |
| F3 | Data → events: produce any event type into any event stream | R21 | Rules (§13.3) | 6 |
| F4 | Computed metrics: user-defined Python functions applied automatically, like rollups | R22 | Rollups (§11.1), Python package | Later |
| F5 | Snowflake connection: query Timelake tables from Snowflake in place | R23 | REST catalog (§5.2), rollups (§11.1) | 2 |
| F6 | Device sync: devices write locally, Timelake moves the data to the lake | R24 | Local tables, idempotent batches, F7 | 6 |
| F7 | Edge gateway: local endpoints for data, metrics, logs and blobs, plus a local cache | R25 | Ingest (§13), reader | 6 |
| F8 | Server mode: Timelake as a database on local disk, the bucket as the durable copy (experiment first) | R26 | Reader, hot tier (§8), F7 | Experiment after §17.6 |
| F9 | Settings: every engine feature can be turned on or off or tuned, per table, per session or per call | — | Table properties (§7.1) | Now (design for review) |
| F10 | Writes from WebAssembly: append and maintain tables from Cloudflare Workers and other JS runtimes | — | `@timelake/client`, streaming compaction (§10.1) | Built |

### 21.1 F1: Agent access with permission sets (R19)

LLM agents should be able to find, understand and read time-series data without a human writing SQL for them, and only see what they are allowed to.

- **Interface:** an MCP server (`timelake mcp`, local over stdio or hosted over HTTP) plus the same operations in the Python and TypeScript clients. Tools: list tables, describe a table (schema, contract, units, series-key meaning, time range), latest values, range query with a bucket and aggregate, and SQL.
- **Self-describing data:** column descriptions and units are stored as table properties, so an agent gets context instead of guessing what `v3` means.
- **Fits in a context window:** results are capped by rows and bytes; long ranges are downsampled automatically (`time_bucket`, `lttb`), and the response says it was downsampled. Every query has a scan budget (bytes, time) so a bad question can't run up an S3 bill.
- **Permission sets:** a named policy stored in the lake (`_timelake/policies/<name>.json`) lists allowed tables, columns, row filters (for example `site = 'plant-3'`, or a tenant), the maximum time window, and operations. Agents only ever get read access. The reader enforces a policy by rewriting the plan (row filter and column projection injected into the DataFusion plan) before it runs. An agent's credential names exactly one permission set.
- **The honest limit:** a policy enforced by the reader only binds clients that go through Timelake. Anyone holding raw bucket credentials can read the files. So the agent never receives bucket credentials: the MCP server holds them, or the REST catalog vends prefix-scoped credentials (§14).
- **Audit:** every agent query is logged with its permission set to an Iceberg table, consistent with D6.

### 21.2 F2: Static read models (R20)

Dashboards almost always ask the same thing: one device or asset, a recent time range, a few fields at a fixed resolution. F2 answers those requests with one object GET from the bucket or a CDN instead of planning a query.

- **Defined from the frontend:** a read model is declared next to the component that renders it, with the Zod schema of the shape it needs (`@timelake/client` already uses Zod) plus its query: source table or rollup, series key (device/asset), fields, aggregates and resolutions. A build step exports the definition (Zod → JSON Schema) to `_timelake/read-models/<name>.json`, so the lake knows what to precompute and the frontend gets typed results.
- **Time tiles:** results are stored like map tiles: per series, per resolution level, per fixed tile (for example 1-minute buckets in day tiles, 1-hour buckets in month tiles) at deterministic paths such as `read-models/<name>/<series>/<resolution>/<tile>.arrow`. Any time range is a few tiles. Closed tiles never change, so they cache forever; only the open (current) tile and tiles hit by late data are rewritten.
- **Invalidation and regeneration on ingest:** each commit knows which series and time partitions it touched (the same snapshot diff that drives incremental rollups). Only the affected tiles are regenerated, by the writer inline or by a commit-triggered job. A small per-model manifest records the snapshot each tile reflects, so readers and caches never mix versions.
- **Cost guardrails:** regeneration is debounced per model (at most once per N seconds) and can be limited to "hot" series, because each refresh is a PUT. Tiles are built from rollups where a matching rollup exists.
- **Browser path:** `timelake-wasm` / `@timelake/client` fetch tiles by path, stitch them, and fall back to a normal query when a model doesn't cover the request.

### 21.3 F3: Data → events (R21)

Turn incoming data into events and publish them into whatever event system the user already runs. This generalizes the rule outputs in §13.3.

- **Events are defined, not hard-coded:** an event is a typed record produced from data by a built-in rule (threshold, flatline, heartbeat, …), a SQL definition (state change, "batch arrived", a derived condition), or pass-through of rows (change-data-capture style). Its payload schema is declared (JSON Schema or Zod).
- **Any sink:** pluggable outputs for Kafka/Redpanda, NATS JetStream, MQTT, Kinesis, SQS/SNS, EventBridge, Google Pub/Sub, Azure Event Hubs, Redis Streams, and webhooks. CloudEvents is the default envelope.
- **Two triggers:** in-stream inside `timelake-ingest` for the lowest latency, or commit-driven: a function triggered per commit reads the snapshot diff, so any writer (CLI, Python, Spark) can produce events without running the ingest service.
- **Delivery:** at least once. Every event carries a deterministic ID (table, series, time, rule), so consumers can deduplicate. Events are emitted only after the commit succeeds and are also written to the `events` Iceberg table, which is the durable record and the replay source after a crash (a commit-driven outbox).

### 21.4 F4: Computed metrics (R22, later)

Let users define a Python function and the result it produces, for example a computed metric, and have Timelake keep that result up to date automatically, the way materialized rollups stay up to date.

- **Definition:** the function, its inputs (tables, series selection, window) and its output table with a normal contract. Sketch:
  ```python
  @tl.metric(inputs={"flow": "iot.flow", "power": "iot.power"},
             output="iot.pump_efficiency", window="1m", series="pump_id")
  def efficiency(flow: pl.DataFrame, power: pl.DataFrame) -> pl.DataFrame: ...
  ```
- **Incremental like rollups:** on each commit, compute which output windows are affected (snapshot diff), pass the inputs as Arrow (zero-copy into Polars or pandas), and replace those windows in the output table. Late data recomputes the affected windows the same way it does for rollups.
- **Contract for functions:** deterministic and per-window by default, so any window can be recomputed. State carried across windows is opt-in.
- **Where it runs:** in the writer's process (Python bindings), in a scheduled `timelake maintain` run, or in a Lambda. There is still no server. A hosted version needs sandboxing.
- **Composes:** computed metrics are ordinary tables, so they can feed read models (F2) and events (F3). SQL and Rust functions can follow on the same mechanism.

### 21.5 F5: Snowflake connection (R23)

Pitch: **an optimized time-series store for your Snowflake.** Many data teams, including large oil and gas operators, already land historian and sensor data in Snowflake. Timelake lets them keep that data in their own bucket, laid out for time series, and query it from Snowflake with no copy and no Snowpipe.

- **What the user gets:** Snowflake sees Timelake tables as Iceberg tables. Data never moves into Snowflake storage, so there is no ingest pipeline into Snowflake, no Snowflake storage bill for raw telemetry, and the same tables still serve DuckDB, Spark, dashboards and agents.
- **Why it is faster and cheaper than plain Iceberg in Snowflake:** Snowflake prunes Iceberg files on manifest column stats. Timelake files are sorted by (series, time), compacted, and time-partitioned, so a query for one asset or a recent window touches few files and burns fewer credits. Rollups are Iceberg tables too, so dashboards in Snowflake can read the 1-minute or 1-hour table instead of raw data.
- **Two ways to connect:**
  - **Through the REST catalog (preferred):** Snowflake attaches the Timelake catalog through a catalog integration, discovers tables, and picks up new snapshots automatically. Depends on `timelake-catalog` (§5.2).
  - **Without a catalog server:** each table is registered in Snowflake from its metadata file in object storage, and Timelake refreshes the registration after commits (or on a schedule). Works with today's v0 layout (D17), at the cost of a refresh step.
- **Setup in one command:** `timelake snowflake connect` creates the external volume, catalog integration and tables (or prints the SQL for a DBA to run), with least-privilege IAM for the bucket.
- **Time-series helpers in Snowflake:** a published set of SQL views and UDFs that map the function library (§11.2) onto Snowflake built-ins (`TIME_SLICE`, `ASOF JOIN`, window functions), like the DuckDB macro pack.
- **Proof:** a benchmark of the same IIoT workload (§17.6) as a native Snowflake table, as naive Iceberg in Snowflake, and as Timelake tables in Snowflake, reporting latency and credits per query plus monthly storage cost.

### 21.6 F6: Device sync (R24)

Devices write to local storage and Timelake moves the data to the lake behind the scenes. The device code never decides how, when or over what the data travels.

- **On the device:** the app writes through the Timelake library to a local table on the device's disk (the same table format; v0 already runs on local disk). Writes are fast and work with no network.
- **Sync agent:** a small background process (or a thread in the library) ships committed local data to the lake and handles everything about the trip:
  - store and forward: offline for hours or weeks, then catch up;
  - resume after a crash or reboot from what the lake has acknowledged;
  - retries with backoff, compression, and a bandwidth cap so sync doesn't starve the device's own traffic;
  - priority: alarms and the latest values first, history after;
  - a local disk quota: data is deleted locally only after the lake acknowledges it, and the oldest acknowledged data goes first when space runs low.
- **Exactly once in the lake:** every uploaded batch carries an idempotent batch ID (already in v0), so resends after a lost acknowledgement never duplicate rows. Device clock skew is recorded and can be corrected on arrival.
- **Many devices, one table:** thousands of devices committing directly to one table would fight over commits. Devices upload files to a staging prefix, and a committer (the edge gateway, F7, or a scheduled job) folds them into the table in batches.
- **Where it syncs to:** an edge gateway (F7) when one is on site, otherwise straight to the object store or an HTTPS endpoint. Devices never hold long-lived cloud keys: each device authenticates with its own identity (a certificate) and receives short-lived credentials scoped to its staging prefix.
- **Targets:** Linux-class devices (gateways, industrial PCs, Raspberry Pi, robots, vehicles). Microcontrollers too small for the library publish over MQTT to a gateway instead.

### 21.7 F7: Edge gateway (R25)

A single binary on site (factory, vessel, rig, substation) that devices send data to on the local network. Devices only need to reach the gateway; the gateway knows how to reach the cloud.

- **Local endpoints for every kind of data:**
  - device and IoT data: MQTT (built-in broker or bridge), HTTP JSON, InfluxDB line protocol; Sparkplug B and OPC-UA later;
  - metrics: OTLP and Prometheus remote write;
  - logs: OTLP logs and syslog, stored as time-partitioned log tables (queryable by time, device and level; full-text search stays a non-goal, §3);
  - blobs (images, vibration captures, files): an S3-compatible upload endpoint. The file goes to the bucket as an object, and a row in a blob index table records time, device, key, size and type, so blobs can be found by device and time range like any other series.
- **Discovery:** devices find the gateway by a fixed local name (mDNS), so device config doesn't change between sites.
- **Buffering:** everything is written to the gateway's disk first and forwarded when the uplink allows (the same store-and-forward as F6). It is also the committer for devices that sync through it.
- **Serving from the local cache:** recent data stays on the gateway, and the gateway answers queries locally (the same reader and SQL), so local HMIs, dashboards and machines keep working when the uplink is down. Queries for older data are read through to the lake and cached.
- **Rules at the edge:** the gateway runs the ingest pipeline (§13), so alarms fire locally even when offline, and are also forwarded as events (F3).
- **Security:** devices authenticate to the gateway; only the gateway holds cloud credentials, scoped to its site.
- This turns the "edge binary" deployment form in §13.1 into a product.

### 21.8 F8: Server mode, Timelake as a database (R26, experiment)

**The question.** Timelake is already fast on local disk. Could the same engine, run as one long-lived server on an instance with local NVMe, compete with a managed time-series database like TimescaleDB, while the bucket stays the durable copy?

**What we measured so far** (indicative: different hardware, not one benchmark):
- **Latest values:** with a warm process on a laptop's local disk, `last-loc` takes 2.5 ms and one truck's latest row 0.4 ms. In Phase 0, TimescaleDB answered `last-loc` in 2–3 ms and plain lake engines needed 34–317 ms (§17.5). The last-value index closed that gap.
- **Analytics:** Timelake's TSBS queries take 22–217 ms on the first run from local disk (12 queries).
  - Tiger Cloud's smallest tier needed 2.0 s (columnstore) and 8.9 s (row store), server time.
  - In Phase 0, lake engines were 4–44× faster than an uncompressed TimescaleDB on the same laptop.
- **What a server would remove:** most of today's cost on S3 is per-process cold start, fetching metadata, footers and the index before the first byte of data. A long-lived process keeps those in memory.

**Design: the same modules, assembled as a server.** Nothing in the core may assume a server; the library, the serverless reader, the edge gateway (F7) and server mode are the same crates put together differently.
- **Storage:** the table format on local NVMe as the primary store. Every commit is also uploaded to the bucket, synchronously (durable on commit) or asynchronously (faster commits, bounded loss window). Old partitions can live only in the bucket and be read through a cache.
- **Memory:** table metadata, Parquet footers, the last-value index and recent data stay in memory, with the hot tier (§8) as the in-process write buffer. That makes data visible in under a second.
- **Protocols (both, decided 2026-10-15):**
  - Postgres wire protocol, so Grafana, psql and BI tools connect unchanged;
  - Arrow Flight SQL, for fast Arrow clients (and JDBC/ADBC drivers);
  - the existing HTTP API and MCP (F1).
- **Operations:** backups and point-in-time restore come from the bucket (snapshots). Failover is a new instance that warms up from the bucket. Readers that don't need the server keep reading the bucket directly.

**The experiment, before building it:**
- **Setup:** one instance with local NVMe (for example c8gd.2xlarge) and the same data (TSBS IoT, then the IIoT benchmark, §17.6). Compare:
  - TimescaleDB, as a hypertable and as columnstore;
  - Timelake behind a thin server: DataFusion plus Flight SQL or Postgres wire.
  - Then managed Tiger Cloud and InfluxDB 3 at similar $/month.
  - Then TSBS with its own loader and query runner over the Postgres wire protocol, and IoTDB's IoT-benchmark, so results sit next to published ones (§17.7).
- **Measure:**
  - p50 and p95 latency, cold and warm;
  - throughput with N concurrent dashboards;
  - sustained ingest and freshness;
  - storage;
  - $ per month;
  - recovery time after losing the instance.
- **Bar to build it:**
  - within about 2× of TimescaleDB on point lookups and concurrency;
  - faster on analytics;
  - cheaper per month;
  - no data loss beyond the configured window when the instance dies.
- **Known gaps to weigh:**
  - one writer per table today, so concurrent writers would go through the server;
  - no multi-statement transactions;
  - Postgres compatibility limited to the SQL that DataFusion supports;
  - one server holds one site's working set.

### 21.9 F9: Settings (built 2026-10-16)

**Principle.** Every engine feature has a setting that turns it on or off or picks its behaviour, a bit like Postgres's configuration variables. During fast development this keeps trade-offs explicit, lets benchmarks measure each feature on and off, and records how behaviour changed.

**One registry.** Every setting is declared once (in `timelake-format`): name, type, default, the levels it may be set at, a description, and the version that added it. From it come the docs page, `SHOW timelake.*` in SQL, `timelake settings` on the CLI, and validation: an unknown name or a bad value is an error, not ignored.

**Levels, the more specific winning:**

| Level | How it's set | Lifetime |
|---|---|---|
| Built-in default | the registry | — |
| Table | table property `timelake.<name>`, at creation or later (`timelake alter`, `Table.set_properties`) | stored in the table, applies to every reader and writer |
| Session | SQL `SET timelake.<name> = …`; `Session` / `Lake` options in Rust and Python; `--set <name>=<value>` on the CLI | one connection or process |
| Call | `AppendOptions`, `MaintainOptions`, … | one call |

A setting declares which levels it allows: the time column and series key only at creation; read settings at every level; write settings at table and call level.

**First settings:**

| Setting | Values (default) | Levels | Meaning |
|---|---|---|---|
| `read-dedup` | on, off (on when `dedup` is on) | table, session | Remove copies when the Timelake reader reads (§9.1) |
| `read-dedup-strategy` | merge, keys-first (decided by benchmark) | table, session | How copies are removed on read (§9.1) |
| `append-dedup` | on, off (on when `dedup` is on) | table, call | Keep only the last of equal keys within one append (not built, see below) |
| `ingest-time-column` | a column name, or none (`_ingest_ts`) | creation | Insertion time per row (§9.1), built |
| `latest-by` | a column (`_ingest_ts`) | table | Which copy of a row is the latest write (§7.4, proposed) |
| `as_of_inserted` | a timestamp, or none | session | Leave out rows inserted after it, before dedup (§7.4, proposed) |
| `compact-cold-after` | a duration, or off (1h) | table, call | Full compaction of partitions whose time range has ended (§9.1) |
| `version-check-interval` | a duration (1s) | table, session | How long the reader uses a table version without checking for a newer one (§19.1 P6), built |
| `data-page-rows` | a count (20,000; 8,192 when written from WebAssembly) | table | Rows per Parquet data page (§21.10), built |

The existing `timelake.*` properties become registry entries unchanged. Hard-coded behaviour moves into settings as it's touched: compaction tiers, the small-file ratio, scan parallelism. The planning cache's lifetime became `version-check-interval`.

**Decided 2026-10-16:**
- **Names in SQL** use underscores (`SET timelake.read_dedup = off`); every spelling (dashes, underscores, with or without `timelake.`) names the same setting.
- **No locked settings:** a session override is the reader's choice.
- **No environment variables** until something needs them.

**Built 2026-10-16:**
- The registry is `timelake_format::settings`: name, kind, default, levels, the version that added it, description, and a value check. A test keeps it in step with the contract's properties and defaults.
- Table level: properties are checked at creation and by `Table::set_properties` (CLI `timelake alter --set/--unset`, Python `Table.set_properties`). Unknown `timelake.*` names, bad values and settings not allowed at the table level are errors; properties a newer Timelake wrote are ignored when reading.
- Session level: every setting the registry allows there can be `SET`, shown with `SHOW`, passed with `--set`, or given to Python's `Lake(settings=…)`. The CLI's `--set` no longer panics on a bad key.
- `timelake settings [table] [--json]` lists them; the docs page "Settings" renders the same list (`dashboard/lib/settings.json`, checked by a test).
- Changes from the proposal: `dedup` is fixed at creation (files written with it off may hold copies, which dedup on read assumes they don't). `append-dedup` isn't built for the same reason: an append that keeps copies would need its files marked for the reader. `format-version` is managed by Timelake (create, upgrade) and can't be set by hand. `latest-by` and `as_of_inserted` wait for §7.4. Storage options (`timelake_freshness_check`, …) stay lake options, documented on the Storage page.

### 21.10 F10: Writes from WebAssembly (decided 2026-10-17, built 2026-10-17)

**Status:** built and measured in workerd ([`bench/results/WORKERS.md`](bench/results/WORKERS.md)). `TableWriter` in `@timelake/client` (`/workers` entry with `initWasm` for Workers, which can't compile WebAssembly from bytes) appends and maintains through a `Store` (`r2Store`, `memoryStore`, or the application's). The shared write path lives in `timelake-format` (`write` feature): rows, file writer, Iceberg manifests and metadata, commits, maintenance, over a `Host`. Measured in workerd: appends of 100,000 rows 0.7 s and 37 MiB; compaction of a day of 24 files (1.2M rows) 2.7 s and 95 MiB. Interop tests: the engine and the shared writer append to and maintain one table in turn (`crates/timelake/tests/shared_writer.rs`). Changes from the design below: the read side of compaction reads a window of pages at a time and merges at most 24 files at once; files written from JavaScript have 8,192-row data pages (setting `data-page-rows`); the WASM host caps row groups being written at 16 MiB. Not yet: creating tables from JavaScript, rollup refresh, signing S3 requests.

**Goal.** Append to Timelake tables, and keep them maintained, from JavaScript runtimes where the Rust engine can't run: Cloudflare Workers first, then browsers, Deno, Bun, and Node. Workers run JavaScript and WebAssembly; Python Workers can't load the native Python package.

**Limits it is designed for** (Cloudflare, October 2026): 128 MB of memory per isolate, WebAssembly included; CPU time 10 ms per request on the free plan, 30 s by default and up to 5 minutes on paid plans; 64 MiB per Worker uncompressed. The read bundle is 8.9 MB.

**Design.**
- **Who does what.** The WASM module builds every file and decides every step; it does no I/O. The TypeScript client (`@timelake/client`) sends the requests through a store: an R2 binding, a local directory (Node, tests), memory (tests), or one the application supplies (`get`, `getRange`, `put`, `putIfAbsent`, `delete`, `list`).
- **Append.** The same path as the engine: rows (JavaScript objects, or Arrow IPC) are aligned to the table's schema, given `_ingest_ts`, sorted by `(partition, series…, time)`, deduplicated within the append, and written as Parquet with the engine's settings (ZSTD, page statistics, delta and dictionary encodings, the sort order). Then the manifest, manifest list, updated last-value index and head.
- **Commit.** The two-step commit (P4): upload everything in parallel, then make the one conditional write of `vN+1.metadata.json` (R2 and S3 support create-if-absent). On a conflict, the WASM side rebases onto the new version and retries, reusing the data files. The version hint and head are written after.
- **Iceberg metadata in our own code.** iceberg-rust pulls in tokio, reqwest and ring and doesn't build for `wasm32-unknown-unknown`, so the shared core writes manifests, manifest lists and table metadata itself, as the WASM reader already reads them. Tests keep the two writers equivalent: the engine (iceberg-rust) loads tables the WASM writer wrote and the other way round, plain DuckDB reads them, and the same input gives the same rows.
- **One core for both.** Writing files (sort, split, dedup, Parquet settings, last-value index, head) and the streaming merge (§10.1) move to a crate without I/O that the engine and `timelake-wasm` both use, so they can't drift.
- **Inline maintenance.** After an append, the same thresholds as the engine (`auto-maintain`, `compact-min-files`, `compact-cold-after`) run streaming compaction within the per-run byte cap, then retention and snapshot expiry with file cleanup. `timelake maintain` elsewhere is optional, not required.
- **Not in the first version:** rollup refresh (it needs the SQL engine; rollups stay correct, since changed buckets are read from the raw table, and are refreshed by the next engine append or `timelake maintain`), schema changes, and request signing for S3 from the client (applications pass a store that signs).

**Recommended deployment.** One writer per table: a Queues consumer or a Durable Object that batches incoming rows into appends of a few thousand rows or more. Concurrent writers are safe (optimistic commits), but each conflict costs a retry, and many tiny appends make many tiny files.

**Measured before it ships:** peak memory and CPU time per append and per maintenance run in workerd, for appends of 1k, 10k and 100k rows and compaction of 8 small files and of a cold partition, to set the defaults (byte cap, batch and row-group sizes).
