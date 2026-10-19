# Baseline results (Phase 0)

> **Run:** 2026-10-11 · Apple M1 Pro, 10 cores, 32 GB RAM, macOS, local SSD · DuckDB 1.5.6, DataFusion 54.1.0 (Python), Polars 2.0.0, PyIceberg 0.12.0, TimescaleDB 2.29.1 (pg17, Docker)
> **Noise:** The machine was *not* idle (a kind cluster, Postgres, Redis, and MinIO containers were running). All data was on local disk, not object storage. Treat differences under ~20% as noise.
> **Method, data, and reproduction:** [`../README.md`](../README.md). Times are the best of 3 repeat runs, in seconds, with the result fully materialized; TimescaleDB is the median of 20 randomized TSBS instances. **Bold** = fastest engine for that query.

## Key findings

1. **No single engine wins.**

   | Suite | Fastest (geomean) | DuckDB | DataFusion |
   |---|---|---|---|
   | TSBS IoT | Polars | 1.43× | 1.32× |
   | TPC-H SF10 | Polars | 1.11× (close second) | 1.65× slowest |
   | ClickBench (10%) | DataFusion | 1.15× | 1.00× |

   - The "DataFusion is slower than DuckDB" question depends on the workload. DataFusion trails on join-heavy TPC-H (q07, q17, q18 are 2.5–3× slower), leads on scan/aggregate ClickBench, and sits in the middle on IoT.
   - For Timelake's workload (time-filtered scans and aggregates over sorted Parquet, few joins), DataFusion is competitive.
2. **TimescaleDB vs. the lake: opposite strengths.**
   - **Last-value lookups:** TimescaleDB answers them in ~2–3 ms via indexes (`last-loc`, `low-fuel`, `high-load`). The lake engines need 34–317 ms because they have to scan.
   - **Analytical queries:** On the 8 analytical queries, every lake engine is **4–44× faster** than TimescaleDB (the fastest lake engine 11–44×).
   - **Narrow time window:** On `stationary-trucks` (a 10-minute window), all engines are within 7–16 ms.
   - **Storage:** TimescaleDB used **3.5 GB** for data that is **244 MB** as Parquet, 14× more. Compression (hypercore) was *not* enabled; enabling it would shrink storage and speed up its scans, so these numbers understate TimescaleDB.
   - **Load:** TimescaleDB loaded 23.3M rows in 57 s (408k rows/s with 8 workers).
3. **Layout matters, even on plain Iceberg.** The same TSBS data as Iceberg (partitioned by day, sorted by truck + time) read via DuckDB was **faster than the as-generated Parquet** in DuckDB:
   - geomean 1.19× vs. 1.43× (relative to Polars)
   - `last-loc` 0.068 s vs. 0.135 s
   - `long-driving-sessions` 0.020 s vs. 0.039 s

   Iceberg's metadata overhead was negligible at this size.
4. **Correctness: every engine returned identical row counts** on all 12 TSBS queries, all 22 TPC-H queries, and all ClickBench queries they completed.
5. **A DuckDB pitfall that matters for Timelake's function pack:** `time_bucket(INTERVAL '1 day', <timestamptz>)` was **~130× slower** than `INTERVAL '24 hours'` (14.7 s vs. 0.11 s on 11.7M rows; same result). Day/month intervals take a timezone-aware calendar path. Timelake's DuckDB macros must use fixed-width intervals, or `date_trunc`, for UTC data.
6. **Polars SQL is the least portable.** It has no time-bucketing functions, can't compare tz-aware timestamps with literals, and failed 4 ClickBench queries on DataFusion-only functions. It needed integer-arithmetic workarounds on TSBS. It's fast, but not a SQL engine to build on.

## What this means for Timelake

| Finding | Implication |
|---|---|
| Lake engines need 34–317 ms for "latest value per device"; TimescaleDB needs 2–3 ms | Confirms the **last-value index / hot store** ([spec 9](../../spec/0009-write-path.md), [spec 17](../../spec/0017-read-paths.md)) as a must-have. It's the one query shape where the lake loses badly. |
| Sorted, day-partitioned Iceberg beats unsorted Parquet in DuckDB | Validates **time-aware compaction and sort order** (§7.3, §10) as a real, measurable win, even for plain-DuckDB readers. |
| 14× storage gap vs. uncompressed TimescaleDB; lake 4–44× faster on analytics | The cost-and-scale argument for a lake holds. Next: compare against *compressed* TimescaleDB to make it fair. |
| DataFusion competitive on scans/aggregates, weaker on multi-way joins | Keeps D3 (DataFusion inside, DuckDB extension for users) reasonable. Timelake's queries are join-light. |
| `time_bucket('1 day')` pitfall | Add to the DuckDB function-pack design (§11.2). |

## Caveats and gaps (next runs)
- **Small TSBS scale** (500 trucks × 3 days, ~250 MB Parquet): many queries take under 50 ms, where fixed overheads dominate. Next: 4,000 trucks × 30 days on a machine with more free disk.
- **Local disk only.** Object storage (MinIO / S3) will change the picture: request latency, metadata reads, range GETs. This is the most important next run for a lake.
- **ClickBench is a 10% subset** (disk constraints), so it isn't comparable with official ClickBench numbers.
- **Not yet covered:** TimescaleDB with compression, InfluxDB 3, QuestDB, Delta, DuckLake, S3 Tables, and the lake-extension workloads from [spec 32](../../spec/0032-timebench.md) (ingest freshness, late data, compaction, deletes).
- **Ports and dialects:**
  - TSBS lake queries use `ROW_NUMBER()` for TimescaleDB's `LATERAL … LIMIT 1` and fixed parameters (see `queries/tsbs_iot/queries.sql`). `breakdown-frequency` fixes a TSBS bug.
  - TPC-H q11 returns 0 rows: DuckDB's query text uses the SF1 fraction. Identical across engines, so timings are still comparable.
  - `stationary-trucks` returns 0 rows for the fixed window on all lake engines.

## Data generation
```bash
# TPC-H SF10 → Parquet (DuckDB CLI): CALL dbgen(sf=10); COPY each table TO '<t>.parquet' (FORMAT parquet, COMPRESSION zstd)
# ClickBench 10%: hits_{0..9}.parquet from https://datasets.clickhouse.com/hits_compatible/athena_partitioned/
.bin/tsbs_generate_data --use-case=iot --seed=123 --scale=500 \
  --timestamp-start=2026-01-01T00:00:00Z --timestamp-end=2026-01-04T00:00:00Z \
  --log-interval=10s --format=timescaledb | .venv/bin/python scripts/tsbs_to_parquet.py data/tsbs/parquet
.venv/bin/python scripts/tsbs_to_iceberg.py
```

## Full results

### tsbs_iot

| query | duckdb 1.5.6 | duckdb-iceberg 1.5.6 | datafusion 54.1.0 | polars 2.0.0 | timescaledb 2.29.1 | rows |
|---|---:|---:|---:|---:|---:|---|
| last-loc | 0.135 | 0.068 | 0.317 | 0.215 | **0.002** | 945 |
| low-fuel | 0.190 | 0.127 | 0.260 | 0.158 | **0.003** | 175 |
| high-load | 0.036 | 0.034 | 0.241 | 0.170 | **0.002** | 377 |
| stationary-trucks | **0.007** | 0.011 | 0.009 | 0.016 | 0.010 | 0 |
| long-driving-sessions | 0.039 | 0.020 | 0.021 | **0.017** | 0.192 | 135 |
| long-daily-sessions | 0.064 | 0.042 | 0.053 | **0.024** | 0.600 | 135 |
| avg-vs-projected-fuel-consumption | 0.045 | **0.042** | 0.048 | 0.046 | 0.595 | 4 |
| avg-daily-driving-duration | 0.127 | 0.120 | 0.055 | **0.041** | 0.830 | 1524 |
| avg-daily-driving-session | 0.136 | 0.127 | 0.067 | **0.043** | 0.905 | 1487 |
| avg-load | 0.024 | 0.023 | 0.021 | **0.020** | 0.330 | 39 |
| daily-activity | 0.200 | 0.187 | 0.069 | **0.048** | 0.781 | 57 |
| breakdown-frequency | 0.159 | 0.140 | 0.068 | **0.045** | 1.973 | 4 |

Geometric mean of best times over the 12 queries every engine completed, and failures:

| engine | geomean (s) | relative | failed |
|---|---:|---:|---:|
| duckdb | 0.068 | 1.43× | 0 |
| duckdb-iceberg | 0.056 | 1.19× | 0 |
| datafusion | 0.063 | 1.32× | 0 |
| polars | 0.047 | 1.00× | 0 |
| timescaledb | 0.109 | 2.30× | 0 |

### tpch

| query | duckdb 1.5.6 | datafusion 54.1.0 | polars 2.0.0 | rows |
|---|---:|---:|---:|---|
| q01 | **0.379** | 0.510 | 0.523 | 4 |
| q02 | 0.085 | 0.128 | **0.055** | 100 |
| q03 | 0.269 | 0.334 | **0.262** | 10 |
| q04 | 0.141 | 0.168 | **0.127** | 5 |
| q05 | 0.258 | 0.542 | **0.258** | 5 |
| q06 | **0.133** | 0.175 | 0.147 | 1 |
| q07 | **0.259** | 0.652 | 0.284 | 4 |
| q08 | **0.328** | 0.499 | 0.355 | 2 |
| q09 | 0.561 | 0.770 | **0.450** | 175 |
| q10 | **0.339** | 0.429 | 0.360 | 20 |
| q11 | 0.060 | 0.094 | **0.034** | 0 |
| q12 | 0.160 | 0.261 | **0.132** | 2 |
| q13 | 0.511 | 0.402 | **0.376** | 45 |
| q14 | 0.227 | **0.201** | 0.204 | 1 |
| q15 | 0.168 | 0.314 | **0.145** | 1 |
| q16 | **0.069** | 0.166 | 0.078 | 27840 |
| q17 | **0.309** | 0.832 | 0.319 | 1 |
| q18 | **0.376** | 1.183 | 0.427 | 100 |
| q19 | 0.291 | 0.350 | **0.220** | 1 |
| q20 | 0.193 | 0.277 | **0.192** | 1804 |
| q21 | 0.663 | 0.850 | **0.450** | 100 |
| q22 | 0.100 | 0.076 | **0.070** | 7 |

Geometric mean of best times over the 22 queries every engine completed, and failures:

| engine | geomean (s) | relative | failed |
|---|---:|---:|---:|
| duckdb | 0.221 | 1.11× | 0 |
| datafusion | 0.329 | 1.65× | 0 |
| polars | 0.200 | 1.00× | 0 |

### clickbench

| query | duckdb 1.5.6 | datafusion 54.1.0 | polars 2.0.0 | rows |
|---|---:|---:|---:|---|
| q00 | 0.003 | **0.001** | 0.001 | 1 |
| q01 | 0.004 | 0.003 | **0.002** | 1 |
| q02 | 0.012 | 0.006 | **0.005** | 1 |
| q03 | 0.011 | 0.006 | **0.006** | 1 |
| q04 | 0.029 | 0.024 | **0.016** | 1 |
| q05 | 0.057 | **0.045** | 0.065 | 1 |
| q06 | 0.005 | **0.001** | 0.003 | 1 |
| q07 | 0.004 | 0.004 | **0.003** | 8 |
| q08 | **0.039** | 0.039 | 0.041 | 10 |
| q09 | **0.053** | 0.054 | 0.053 | 10 |
| q10 | 0.013 | **0.012** | 0.019 | 10 |
| q11 | 0.015 | **0.014** | 0.020 | 10 |
| q12 | 0.057 | **0.046** | 0.059 | 10 |
| q13 | 0.080 | **0.068** | 0.102 | 10 |
| q14 | 0.063 | **0.046** | 0.056 | 10 |
| q15 | 0.032 | 0.028 | **0.021** | 10 |
| q16 | 0.089 | **0.077** | 0.090 | 10 |
| q17 | 0.081 | **0.077** | 0.096 | 10 |
| q18 | 0.147 | **0.145** | ERR | 10 |
| q19 | 0.003 | 0.005 | **0.003** | 0 |
| q20 | 0.102 | **0.098** | 0.126 | 1 |
| q21 | **0.088** | 0.114 | 0.155 | 10 |
| q22 | **0.218** | 0.257 | 0.322 | 10 |
| q23 | **0.241** | 0.583 | 0.961 | 10 |
| q24 | 0.047 | **0.023** | 0.037 | 10 |
| q25 | 0.041 | **0.026** | 0.040 | 10 |
| q26 | 0.036 | **0.024** | 0.041 | 10 |
| q27 | **0.073** | 0.096 | 0.120 | 12 |
| q28 | 1.211 | **0.566** | ERR | 15 |
| q29 | **0.012** | 0.016 | 0.021 | 1 |
| q30 | 0.059 | **0.047** | 0.056 | 10 |
| q31 | 0.064 | **0.050** | 0.061 | 10 |
| q32 | 0.131 | **0.114** | 0.126 | 10 |
| q33 | **0.169** | 0.222 | 0.242 | 10 |
| q34 | **0.174** | 0.220 | 0.252 | 10 |
| q35 | 0.049 | **0.027** | ERR | 10 |
| q36 | 0.049 | 0.062 | **0.048** | 10 |
| q37 | 0.051 | 0.032 | **0.028** | 10 |
| q38 | **0.023** | 0.040 | 0.032 | 10 |
| q39 | 0.097 | 0.119 | **0.065** | 10 |
| q40 | 0.010 | 0.011 | **0.009** | 10 |
| q41 | 0.011 | 0.011 | **0.007** | 0 |
| q42 | 0.016 | **0.009** | ERR | 10 |

Geometric mean of best times over the 39 queries every engine completed, and failures:

| engine | geomean (s) | relative | failed |
|---|---:|---:|---:|
| duckdb | 0.035 | 1.15× | 0 |
| datafusion | 0.030 | 1.00× | 0 |
| polars | 0.033 | 1.10× | 4 |

<details><summary>Failures</summary>

- `polars` q18: SQLInterfaceError: unsupported function (DataFusion-dialect ClickBench SQL, e.g. to_timestamp_seconds)
- `polars` q28: SQLInterfaceError: unsupported function (DataFusion-dialect ClickBench SQL, e.g. to_timestamp_seconds)
- `polars` q35: SQLInterfaceError: unsupported function (DataFusion-dialect ClickBench SQL, e.g. to_timestamp_seconds)
- `polars` q42: SQLInterfaceError: unsupported function (DataFusion-dialect ClickBench SQL, e.g. to_timestamp_seconds)

</details>

