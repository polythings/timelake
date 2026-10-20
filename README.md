<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="brand/logo/timelake-lockup-v1-white.svg">
    <img alt="Timelake" src="brand/logo/timelake-lockup-v1.svg" height="64">
  </picture>
</p>

<h3 align="center">The open time-series lakehouse</h3>

<p align="center">Fast queries on your own object storage, with no database and no maintenance jobs.<br><a href="https://timelake.dev">timelake.dev</a> · <a href="https://timelake.dev/docs">Docs</a> · <a href="https://timelake.dev/benchmarks">Benchmarks</a></p>

<p align="center">
  <a href="https://crates.io/crates/timelake"><img alt="crates.io" src="https://img.shields.io/crates/v/timelake?label=crates.io"></a>
  <a href="https://pypi.org/project/timelake/"><img alt="PyPI" src="https://img.shields.io/pypi/v/timelake?label=PyPI"></a>
  <a href="https://www.npmjs.com/package/@timelake/client"><img alt="npm" src="https://img.shields.io/npm/v/%40timelake%2Fclient?label=npm"></a>
  <a href="https://github.com/polythings/timelake/releases/latest"><img alt="CLI release" src="https://img.shields.io/github/v/release/polythings/timelake?label=CLI&amp;sort=semver"></a>
  <a href="https://docs.rs/timelake"><img alt="docs.rs" src="https://img.shields.io/docsrs/timelake?label=docs.rs"></a>
  <a href="https://github.com/polythings/timelake/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/polythings/timelake/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/polythings/timelake/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

Keep time series in your own bucket on S3, R2, GCS, Azure, MinIO or a local disk. Stream or batch-load them, query them fast, and run nothing else.

- **Fast every day.** Dashboards, trends and "what is the value now" read only the files and row groups they need: data is partitioned by time and sorted by `(series key, time)`, and the latest value of every series comes from an index, not a scan.
- **Nothing to maintain.** Compaction, deduplication (the latest write wins), retention and snapshot cleanup run inside the writer, or as one stateless `timelake maintain` command (cron, Lambda, Kubernetes CronJob). No Spark or Glue jobs.
- **Pay for storage, not a database.** No server, no catalog service: the catalog is a few files next to the data. The bill is object-storage bytes and requests.

**Open format** (Parquet files and Apache Iceberg metadata today; DuckDB, Spark and PyIceberg read the tables, and Trino, Athena and Snowflake will through the planned REST catalog) · **any object store** · **Apache 2.0**.

> **Status: v0, open source, under active development.** See the [specs](spec/README.md) for the design ([the milestone "v0"](spec/milestones/v0.md) covers v0's scope). On real AWS S3 (us-east-2, queried from a laptop), against the same data streamed in 5-minute batches, Timelake answered the TSBS IoT queries 2.1–13× faster than Lance, PyIceberg-written Iceberg and S3 Tables right after ingest, and the "latest position of every truck" query took 18 requests instead of 1,733 ([bench/results/S3.md](bench/results/S3.md); the earlier RustFS runs are in [V0.md](bench/results/V0.md)). An IIoT benchmark against InfluxDB 3, TimescaleDB, ClickHouse and QuestDB is next ([spec 45](spec/0045-iiot-benchmark.md)).

## Quick start

### CLI
```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/polythings/timelake/releases/latest/download/timelake-cli-installer.sh | sh   # or: cargo install timelake-cli
export TIMELAKE_WAREHOUSE=./warehouse         # or s3://bucket/prefix

# Create a time-series table from a file's schema and load it
timelake import iot.readings readings.parquet --time-column time --series-key device_id
timelake import iot.devices devices.csv       # a plain table (no time column)

timelake sql "SELECT device_id, time_bucket(INTERVAL '1 hour', time) AS h, avg(temp)
              FROM iot.readings WHERE time > now() - INTERVAL '1 day' GROUP BY 1, 2"
timelake latest iot.readings                  # newest row per device, from the index
timelake info iot.readings                    # contract, files, partitions, snapshots
timelake maintain                             # compact + retention + expire, all tables
timelake duckdb iot.readings                  # the SQL to read it from plain DuckDB
```

S3-compatible stores take `-o key=value` options (or the usual `AWS_*` environment variables):
```bash
timelake -w s3://lake/wh -o aws_endpoint=http://localhost:9000 -o aws_allow_http=true ls
```

### Python
```bash
pip install timelake                      # or `uv sync` in a clone for the dev environment
```
```python
import datetime as dt

import pyarrow as pa
import timelake

schema = pa.schema([("time", pa.timestamp("us", tz="UTC")), ("device_id", pa.string()), ("temp", pa.float64())])
batch = pa.table({"time": [dt.datetime.now(dt.timezone.utc)], "device_id": ["pump-7"], "temp": [71.5]}, schema=schema)

lake = timelake.Lake("s3://lake/wh", storage_options={"aws_region": "eu-west-1"})
t = lake.create_table("iot.readings", batch.schema,
                      time_column="time", series_key=["device_id"],
                      chunk="day", retention="365d")
t.append(batch)                          # pyarrow Table/RecordBatch/reader, polars, ...
t.append(batch, batch_id="kinesis-shard-7-seq-1234")   # idempotent retries

lake.sql("SELECT * FROM iot.readings WHERE device_id = 'pump-7' ORDER BY time DESC LIMIT 1")
lake.latest("iot.readings")              # pyarrow.Table, one row per device
t.maintain()                             # also runs inline after appends
```

### Plain DuckDB
```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('s3://lake/wh/iot/readings');   -- reads metadata/version-hint.text
```

## Concepts

| Concept | What it means |
|---|---|
| **Warehouse** | A root location. Tables live at `<warehouse>/<namespace>/<table>/`. |
| **Time-series table** | Has a `time_column` (required, NOT NULL) and optionally a `series_key` (e.g. `device_id`). Partitioned by `chunk` (hour/day/month) and sorted by `(series_key…, time)`. |
| **Plain table** | No time column: device metadata, dimensions. Unpartitioned. |
| **Commit** | Every `append` is one atomic Iceberg snapshot. Commits use put-if-absent on `vN.metadata.json`, so concurrent writers are safe without a catalog service. |
| **Last-value index** | A small Parquet file per snapshot with the newest row per series. Used by `latest()` and by automatic query rewrites. |
| **Maintenance** | Size-tiered compaction inline after appends; full re-sort/dedup, retention and snapshot expiry via `maintain`. |

### Table settings (Iceberg table properties)
| Property | Default | Meaning |
|---|---|---|
| `timelake.time-column` | (none) | Time column |
| `timelake.series-key` | (none) | Comma-separated series key columns |
| `timelake.chunk-interval` | `day` | `hour`, `day` or `month` partitions |
| `timelake.dedup` | on if a series key is set | Rows with the same `(series, time)` count once, the latest write winning: within an append, when the Timelake reader reads, and at compaction |
| `timelake.retention` | forever | Drop partitions older than this, e.g. `90d` |
| `timelake.auto-maintain` | `true` | Run compaction, retention and expiry inline after appends |
| `timelake.compact-min-files` | `8` | Inline compaction merges a size tier once it has this many files |
| `timelake.compact-cold-after` | `1h` | Inline maintenance rewrites a partition whole once newer data is this far past its end (`off`: size tiers only) |
| `timelake.read-dedup` | as `timelake.dedup` | The Timelake reader returns only the latest write of each `(series, time)`; `SET timelake.read_dedup = false` per session |
| `timelake.target-file-size-bytes` | 256 MiB | Compaction output size |
| `timelake.row-group-rows` | 1048576 | Parquet row group size (page indexes keep single-series reads selective) |
| `timelake.snapshot-retention` | `1h` | Expire snapshots older than this… |
| `timelake.min-snapshots` / `timelake.max-snapshots` | `5` / `100` | …keeping at least / at most this many |

### Queries the reader accelerates
- **Time and series filters** prune files using Iceberg column bounds, then row groups and pages inside the sorted Parquet files.
- **Latest row per series:** `ROW_NUMBER() OVER (PARTITION BY <series key> ORDER BY <time> DESC) = 1` is rewritten to read the last-value index.
- **Latest row:** `… [WHERE <series key filter>] ORDER BY <time> DESC LIMIT 1` is also rewritten to read the index (tables with a series key).
- `time_bucket(interval, ts [, origin])` (same as DataFusion's `date_bin`).
- **Object storage:** every table a query references is loaded concurrently before planning (version hint, metadata, manifests, last-value index), each Parquet footer is fetched once, and scans use more partitions on remote stores (latency-bound I/O). Table versions loaded for planning are reused for 1 s within a process; the process's own commits are always visible.

## Repository layout
| Path | What |
|---|---|
| `crates/timelake` | Core library: storage (any object store, read-only HTTPS), catalog, writer, commit layer, maintenance, DataFusion reader |
| `crates/timelake-format` | Table conventions shared by the engine and the WASM reader (contract, partitions, Iceberg bounds) |
| `crates/timelake-cli` | `timelake` command |
| `crates/timelake-py` | Python package (PyO3 + maturin) |
| `crates/timelake-wasm` | WebAssembly reader: metadata, pruning, Parquet decoding, latest/range/bucket queries (no I/O) |
| `packages/timelake-client` | `@timelake/client`: TypeScript client around the WASM reader, with Zod-typed rows |
| `dashboard/` | Web app: website, docs, and benchmark results. Deploys to Vercel; see its README |
| `brand/` | Logo, wordmark, colors and words ([brand/README.md](brand/README.md)); `bun run brand:editor` opens the mark editor |
| `bench/` | Benchmarks: TSBS IoT, TPC-H, ClickBench, the v0 comparison (`bench/v0`), TimescaleDB on Tiger Cloud |
| `scripts/` | Demo warehouse build (`build-demo-warehouse.sh`) and a static server with Range support for testing (`serve-range.mjs`) |
| `spikes/` | Findings of Phase 0 experiments (code in git history) |
| `spec/` | Design and decisions: one numbered spec per feature, with its status, and the milestones of the roadmap |

## Documentation
The [docs](https://timelake.dev/docs) cover a [quick start](https://timelake.dev/docs/quick-start) with sample data, [how it works](https://timelake.dev/docs/how-it-works), the CLI, Python and TypeScript APIs, a [glossary](https://timelake.dev/docs/glossary), a [tour of this codebase](https://timelake.dev/docs/codebase) and [development setup](https://timelake.dev/docs/development). Their source is in `dashboard/app/(site)/docs/`.

## Development
```bash
uv sync && bun install && prek install                   # environments and git hooks (uv tool install prek)
prek run --all-files --hook-stage pre-push               # every formatter, linter and type checker
cargo test --workspace                                   # Rust tests
uv run pytest crates/timelake-py/tests                   # Python package
uv run pytest bench/tests                                # benchmark helpers
cargo build -p timelake-cli && (cd packages/timelake-client && bun test)   # TS client + WASM vs the engine
```
See [Development](https://timelake.dev/docs/development) for each part, the tools and CI, [CONTRIBUTING.md](CONTRIBUTING.md) for the code guide, and [CHANGELOG.md](CHANGELOG.md) for what changed. Report
security problems privately, as [SECURITY.md](SECURITY.md) describes.

## Acknowledgements
Timelake is built on [Apache Iceberg](https://iceberg.apache.org), [Apache Parquet](https://parquet.apache.org), [Apache Arrow](https://arrow.apache.org) and [Apache DataFusion](https://datafusion.apache.org). Its compaction follows [Apache Cassandra](https://cassandra.apache.org)'s size tiers, and its benchmarks use [TSBS](https://github.com/timescale/tsbs), [ClickBench](https://github.com/ClickHouse/ClickBench) and TPC-H via [DuckDB](https://duckdb.org). We take ideas, never code: [PRIOR_ART.md](PRIOR_ART.md) credits the projects we learned from, and [NOTICE](NOTICE) lists third-party files and their licenses.

## License
Licensed under the [Apache License, Version 2.0](LICENSE). Unless you state otherwise, any contribution you submit for inclusion in Timelake is licensed under the same terms, without additional conditions (Apache 2.0, §5).
