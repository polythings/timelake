# timebench

Benchmarks for time-series storage ([spec 44](../spec/0044-timebench.md)): Timelake against the table formats and databases a team would
otherwise use. Results, their history and the methodology of each benchmark are on
[timelake.dev/benchmarks](https://timelake.dev/benchmarks); how timebench runs them is on
[timelake.dev/benchmarks/timebench](https://timelake.dev/benchmarks/timebench).

timebench lives here for now and may move to its own repository: only its Timelake adapter
(`timebench/systems/timelake.py`) knows about Timelake.

| What | Run | Results |
|---|---|---|
| **`timebench`**: a workload streamed into each system, then its queries before and after each system's maintenance | `uv run timebench run …` | `results/runs/` (the history), `timebench report` |
| **Weekly regression run** on a fixed AWS instance | `.github/workflows/bench-weekly.yml` → `infra/weekly.sh` | `results/runs/` |
| **Phase 0 engine comparison**: DuckDB, DataFusion, Polars, Iceberg and Vortex over static files | `run.py` | [`results/BASELINE.md`](results/BASELINE.md), [`OBJECT_STORAGE.md`](results/OBJECT_STORAGE.md), [`VORTEX.md`](results/VORTEX.md) |
| **Any S3-compatible store**, small and without the proxy (free tiers) | `v0/small.py` | [`results/PERFORMANCE.md`](results/PERFORMANCE.md) |

## Setup
```bash
uv sync                                                    # at the repo root: Python, the engines, and timelake (built from crates/timelake-py)
cargo build --release -p timelake-cli                      # the Timelake reader (target/release/timelake)
(cd bench/tools/s3proxy && go build -o ../../.bin/s3proxy .)   # request-counting S3 proxy
GOBIN=$PWD/bench/.bin go install github.com/timescale/tsbs/cmd/tsbs_generate_data@v0.0.0-20260527045238-8323e59c7402
```
Run Python from anywhere in the repo with `uv run …`; it uses the root `.venv`. Tests: `uv run pytest bench/tests`
(offline, on a tiny generated workload).

## timebench
```bash
uv run timebench list                                         # workloads, scales, systems, readers
uv run timebench run tsbs-iot --scale small                   # every lake system on the local disk, a few minutes
uv run timebench run tsbs-iot --systems timelake,timescaledb  # with BENCH_TIMESCALEDB_DSN set
uv run timebench run iiot --param machines=50 --param days=9 --systems reference,timelake,clickhouse  # a quick IIoT run
uv run timebench report latest                                # one run as Markdown
uv run timebench history --workload tsbs-iot                  # one line per run
```
- **Workloads** (`timebench/workloads/`): data at a named scale, the order its rows arrive in, and the queries. The
  data is generated on first use into `data/workloads/<workload>-<scale>/`. Both stream in 5-minute windows
  (`--param window_minutes=…`; `--param max_windows=N` for a quick check):
  - `tsbs-iot` ([spec 44](../spec/0044-timebench.md)): `small` (50 trucks), `standard` (500 trucks, the published runs), `large` (5,000).
  - `tsbs-devops`, TSBS's cpu-only use case and its 15 query types: `small` (100 hosts), `standard` (4,000 hosts,
    104M rows, the size published TSBS results use). Its file is in time order, so it streams without being held in
    memory.
  - `iiot` ([spec 45](../spec/0045-iiot-benchmark.md)): a plant's process historian from our own generator (`timebench/workloads/plant.py`), arriving
    through gateways that go offline and send some rows twice. `small` (1,000 tags, 30 days), `standard` (10,000),
    `large` (100,000), `xlarge` (1,000,000, 7 days); `--param machines=N --param days=D` for a quick check. It
    arrives in stages: history in bulk (an hour per commit) with checkpoints `1d` and `7d`, then the last day live (a
    5-minute window per commit) and `ingested`. Its queries are relative to the data's present (`@AGO(15 minutes)`).
- **Variants** (`--variant NAME`): a different kind of run of the same workload and target, kept apart in reports and
  on the site. The weekly `bulk` variant loads TSBS DevOps in 1-hour windows as fast as each system commits, with no
  queries (`--checkpoints ""`), and is judged by rows per second.
- **Tunings** (`--tuning`, a profile's `tunings`; D23 in spec 44): every system runs `tuned` (its docs'
  recommendations for the workload, precomputation included) and `default` (as installed). A system lists the tunings
  it has built (`tunings`); a run of another tuning leaves it out. A default system's tables live apart
  (`<system>-default`), and a default run's file ends in `-default`. A tuned run first runs the statements in
  `tunings/<workload>/<system>.sql` after creating the tables (Timelake's rollups), and reads a query's
  `-- name: <query> [<reader> tuned]` version where it has one: the SQL the system's docs write for its
  precomputation, checked against the reference like every answer. The reference runs in both tunings.
- **Systems** (`timebench/systems/`), each written and read the way its docs suggest:

  | System | Writes | Maintenance | Tuned (default leaves it out) | Readers |
  |---|---|---|---|---|
  | `timelake` | `Table.append` per commit, inline compaction | `Table.maintain()` | the workload's rollups, tuned queries | `timelake` (the CLI), `duckdb` |
  | `iceberg` | PyIceberg `append`, `day(time)` partitions | binpack rewrite (S3 Tables' default) | sort order on (series key, time), compaction sorts | `duckdb` |
  | `s3tables` | the same, through S3 Tables' REST endpoint (AWS only) | its managed compaction (waits up to 6 h) | sort order, compaction strategy `sort` | `duckdb` |
  | `lance` | `write_dataset(mode="append")` | none | `compact_files()`, `cleanup_old_versions()`, B-tree on the series key, zone map on time | `duckdb`, `datafusion` |
  | `parquet` | a Parquet file per commit under `day=…/` | none | one sorted file per day | `duckdb`, `datafusion` |
  | `timescaledb` | `COPY` per commit into hypertables (via a staging table and `ON CONFLICT DO NOTHING` where rows repeat) | `ANALYZE` | (series, time) index, columnstore at maintenance, continuous aggregates | `postgres` |
  | `clickhouse` | Arrow inserts into `MergeTree` ordered by time (`ReplacingMergeTree` by series and time where rows repeat) | none (background merges) | ordered by (series, time), monthly partitions, `OPTIMIZE … FINAL`, materialized views where rows don't repeat | `clickhouse` (with `FINAL`) |
  | `questdb` | QWP (`questdb.connect().dataframe`) into WAL tables, `DEDUP UPSERT KEYS` where rows repeat | none (waits for the WAL) | indexed series symbol, materialized views | `questdb` (PGWire) |
  | `influxdb3` | line protocol over HTTP, 4 concurrent requests | none (Core has no compaction) | query file limit raised, last value cache | `influxdb3` (SQL over HTTP) |
  | `reference` | a local Parquet file per commit | none | the same | `duckdb`, rows that arrived twice removed |

  The database servers run in Docker at pinned versions: `infra/servers.sh up` starts them (data in
  `data/servers/`, ports 55432, 58123, 59900/58812, 58181, 58182), and `eval "$(infra/servers.sh env)"` sets the
  variables the adapters read (`BENCH_TIMESCALEDB_DSN`, `BENCH_CLICKHOUSE_URL`, ...). Each dataset gets its own
  schema or database that runs drop; QuestDB holds one dataset at a time. `influxdb3` tuned runs with
  `--query-file-limit` raised (server `influxdb3`), default as shipped (server `influxdb3-defaults`: a query reads at
  most 432 files). With
  `BENCH_SERVERS` set to `infra/servers.sh`, a run starts each system's server just before that system's turn and
  stops it after, so one server uses the machine at a time; the run file records each server's version.
- **Targets** (`--target`): `local` (tables under `data/lakes/`), or S3 through `tools/s3proxy` on `localhost:59100`:
  `s3` for a local RustFS, `aws-s3` / `aws-s3x` for real buckets (`infra/`). TimescaleDB ignores the target.
- **Reusing tables** (`--reuse`): query the tables an earlier run left on the target, at the given checkpoints, and run
  the concurrent dashboards if asked, without ingesting. `TIMEBENCH_TIMELAKE_OPTIONS=key=value,...` adds storage
  options to Timelake's readers, to compare settings on the same tables (`timelake_cache=off`), and
  `TIMEBENCH_TIMELAKE_SETTINGS=key=value,...` session settings of the CLI reader
  (`timelake.filter_pushdown=true`). The run file records both (`reader_settings`, `reader_options`). Such A/B runs go
  in `results/experiments/<date>/` with an index of what each arm changed, not in `results/runs`.
- **Adding a workload or system:** subclass `Workload` or `System` and register it in its package's `__init__.py`.
  Queries go in `queries/<suite>/queries.sql` with the dialect placeholders of `timebench/queries.py`.

### Method
- Each query runs in a fresh process: one first run, then `--runs` repeats (default 2). Every run fully
  materializes the result; row counts are compared across readers, and a mismatch is recorded in the run.
- Right answers: when the run includes `reference` (first), every other answer is compared with its answer at the
  same checkpoint: the same row count and, per column, the same sum (timestamps in epoch seconds) within 1e-6. Each
  query record gets `correct`, and the `end` record lists the wrong ones.
- "First run" is not cold-cache on local disk: the OS page cache stays warm (dropping it needs root on macOS). A
  database server's buffer cache stays warm too.
- On S3 targets every request goes through the proxy, which counts requests (by kind, in AWS mode) and bytes for
  ingest, maintenance and each query run. With `--ttfb-ms` it adds a fixed delay per request (local stores).
- A failing system or query is recorded and the run goes on. `--timeout` (600 s; the reference gets
  `--reference-timeout`, 1800 s) caps each query process, first run and repeats together. A query that timed out on a
  system's reader is recorded as `skipped` at that system's later checkpoints instead of running again
  (`--no-skip-after-timeout` runs it anyway). Profiles set the same as `timeout`, `reference_timeout` and
  `skip_after_timeout`.
- `--live-interval S`: while each system ingests, a probe round every S seconds runs the dashboard queries (the
  workload's `dashboard_suite`) in fresh processes with the system's own reader, plus a freshness probe (the newest
  timestamp of `live_table`). Recorded per system as a `live` record: query latency under writes, and how far behind
  the newest finished commit each probe was (`lag_s`).
- `--concurrency 1,4,16`: after the last checkpoint, that many client processes per system, each keeping a reader open
  and running the dashboard queries in turn for `--concurrency-seconds` (30): queries per second and p50/p95/p99, as
  `concurrency` records. A query that fails counts as one error per client and is left out of that client's turns.
- Cost: on AWS targets, every query, ingest and maintenance record carries `usd` (its requests at S3 list prices,
  `timebench/cost.py`), and ingest and maintenance `storage_usd_month`. Machines aren't priced yet.

### Results
`results/runs/<started>-<workload>-<scale>-<target>.jsonl`, one per run: a `run` record (commit, uncommitted changes,
machine, versions, settings), `ingest` and `maintain` records per system, a `query` record per query and reader, and
an `end` record. Commit the runs worth keeping: they are the history the website charts (it reads a copy in
`dashboard/data/runs`, refreshed by `bun dashboard/scripts/sync-results.ts`). The runs before 2026-10-15 were
converted from the v0 scripts' result files.

## Local S3 (RustFS + 20 ms per request)
On macOS, run RustFS natively (the `rustfs-macos-aarch64` build from its GitHub releases): behind Docker Desktop's
networking, new connections opened several at a time stall about 200 ms in their first response, which shows up
in every first-run time. On Linux the container works as well.
```bash
RUSTFS_ACCESS_KEY=benchkey RUSTFS_SECRET_KEY=benchsecret123 \
  rustfs server --address 127.0.0.1:59000 --console-address 127.0.0.1:59001 data/rustfs &
# Linux: docker run -d --name timelake-bench-rustfs -p 59000:9000 -v timelake-bench-rustfs:/data \
#   -e RUSTFS_ACCESS_KEY=benchkey -e RUSTFS_SECRET_KEY=benchsecret123 rustfs/rustfs:latest
.bin/s3proxy -listen :59100 -listen-uncounted :59102 -upstream localhost:59000 -admin :59101 -ttfb 20ms &
uv run timebench run tsbs-iot --scale small --target s3 --ttfb-ms 20 --mibps 50
```
Create the bucket `bench` first (any S3 client against `localhost:59000`). `--ttfb-ms` is the latency the proxy adds
before each response, `--mibps` caps each response's throughput (S3 serves one request at roughly 50–90 MiB/s;
`0` for no cap), so request sizes and concurrency matter as they do on S3. Both default to 0, and every S3 run sets
both and records them, so a run never inherits the last one's.

## Real AWS S3
From this machine (`run_aws.sh`) or from an instance in the region, including the weekly run: see
[`infra/README.md`](infra/README.md).

## Phase 0 engine comparison (`run.py`)
Query engines over the same static files, one suite × one engine:
```bash
.bin/tsbs_generate_data --use-case=iot --seed=123 --scale=500 \
  --timestamp-start=2026-01-01T00:00:00Z --timestamp-end=2026-01-04T00:00:00Z \
  --log-interval=10s --format=timescaledb | uv run python scripts/tsbs_to_parquet.py data/tsbs/parquet
uv run python scripts/to_vortex.py          # Vortex copies
uv run python scripts/tsbs_to_iceberg.py    # Iceberg copy (PyIceberg); add "s3" to write it through the proxy
uv run python run.py --suite tsbs_iot --engine duckdb --layout sorted --tag mytag
```

| Suite | Data | Queries |
|---|---|---|
| `tsbs_iot` | TSBS IoT, 500 trucks × 3 days at 10 s, seed 123 | 12 TSBS IoT queries ported to portable SQL (`queries/tsbs_iot/queries.sql`, porting rules in its header) |
| `iot_point` | the same data | 5 single-truck queries (`queries/iot_point`) |
| `tpch` | TPC-H SF10 as Parquet (`scripts/tpch_to_parquet.py`) | 22 TPC-H queries from DuckDB's `tpch` extension |
| `clickbench` | ClickBench `hits`, 10% subset (as in `infra/remote.sh`, `data`) | 43 official queries, downloaded at a pinned commit (`queries/README.md`) |

Engines: `duckdb`, `datafusion`, `polars` (Python packages), `duckdb-iceberg`, `duckdb-vortex`, and `datafusion-rs*`
(DataFusion in Rust, `rust-reader/`). Results: `results/<suite>.<tag>.jsonl`.

The v0 scripts (`v0/ingest.py`, `run_queries.sh`, `summarize.py`, `tsdb_cloud.py`) that produced `V0.md`, `S3.md`
and `TIMESCALE.md` were replaced by `timebench` on 2026-10-15; recover them with
`git checkout b99aae9 -- bench/`. The older Phase 0 runners: `git checkout 2d66939 -- bench/`.
