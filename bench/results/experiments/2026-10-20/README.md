# Experiments of 2026-10-20 (D23, #149)

Tuned and default Timelake (D23) on the industrial IoT workload, and rollup routing through filters from another
table. The numbers and decisions are in [`../../PERFORMANCE.md`](../../PERFORMANCE.md). M1 Pro laptop, release builds,
local disk, under other load (load average 20–30), so absolute times are rough; both arms ran back to back.

| Experiment | Data | Arms | Result | Decision (section of PERFORMANCE.md) |
|---|---|---|---|---|
| Tuned IIoT SQL for Timelake | IIoT small, an hourly rollup per tag, CLI best of 3 | the shared SQL, Timelake's tuned version | daily-stats 551 → 37 ms, gaps 249 → 38, latest-area 91 → 21, time-above-limit 360 → 193; same answers | tuned versions kept (D23: tuned Timelake on IIoT) |
| Routing through `IN (SELECT …)` | IIoT small daily statistics of one line's tags | a literal tag list, the asset model's subquery before and after | 54 ms, 515 ms, 37 ms | built (same section) |

## Timebench runs

| Experiment | Workload, target | Arms (label: what differs) | Runs | Result | Decision |
|---|---|---|---:|---|---|
| Timelake tuned and default | IIoT small; local; reference checking answers | `tuning A/B tuned`: the hourly rollup and tuned queries; `tuning A/B default`: neither | 2 | After maintenance, geomean best repeat 43 → 16 ms; ingest 450 → 456 s, commit p50 132 ms both; dashboards 61 → 187 queries/s at 16 clients | rollup and tuned queries are Timelake's tuned IIoT configuration |
| Lake systems' tunings work | TSBS IoT small; local | `tuning check tuned` / `tuning check default`: Iceberg, Lance, Parquet with the reference | 2 | every answer right; maintenance tuned/default: Iceberg 8.6/8.3 s (sorting/binpack), Lance 1.5/0 s (5,190 → 26 objects), Parquet 0.9/0 s | the configurations as built |
| QuestDB loads IIoT again (#154) | IIoT small, 2 days; local, Docker | `questdb fix check`: QuestDB with the reference, each batch's chunks combined before `dataframe()` | 1 | ingest ok (15 s), every answer right | the fix |
| TimescaleDB tuned and default (#150) | IIoT small, 7 days; local, Docker | `timescaledb A/B tuned`: (series, time) index, columnstore at maintenance, an hourly continuous aggregate with real-time aggregation, tuned queries; `timescaledb A/B default`: hypertables as created, row store | 2 | After maintenance geomean best repeat 378 → 59 ms, right after ingest 430 → 103 ms; storage 13.5 → 0.7 GB; dashboards 2.7 → 144 queries/s at 16 clients; ingest 1,643 vs 1,664 s (the tuned run at load 75–93). Right after ingest the three queries on the continuous aggregate were wrong: late rows into already materialized hours wait for the next refresh. `DISTINCT ON` for latest-area measured slower than the shared query (1,015 vs 90 ms) and was dropped | the tuned configuration as built |
| ClickHouse, QuestDB and InfluxDB tuned and default (#151–#153) | IIoT small, 7 days; local, Docker, each server started in turn | `servers A/B tuned`, `servers A/B default`; InfluxDB's default again as `servers A/B default influxdb3 rerun` (its server dropped mid-ingest in the first, most likely short of memory with a TimescaleDB container still up) | 3 | QuestDB (indexed symbol, hourly materialized view, `LATEST ON`): after maintenance 376 → 82 ms, daily-stats 7.1 s → 19 ms, dashboards 80 → 992 queries/s at 16 clients. ClickHouse (ordered by series and time, monthly partitions, merged; no views on IIoT, whose rows repeat): 267 → 199 ms, dashboards 14 → 28 queries/s. InfluxDB (file limit raised, last value cache): as shipped 6 of 11 queries fail or time out, the tuned server answers all; on the 5 both answer 221 → 107 ms, dashboards 0.4 → 20 queries/s. Every answer the systems gave was right | the tuned configurations as built |
| ClickHouse's TSBS views (#151) | TSBS IoT standard; local, Docker | `clickhouse views A/B tuned`, `… default` (most answers wrong in both: TSBS's nulls stored as 0 and empty strings, and one query failed on a DateTime bucket); again as `…, nullable` with every column but the ordering ones nullable and the epoch placeholder casting to DateTime64 | 3 | Every answer right in both but avg-daily-driving-session (wrong in both: `lag()` gives a default, not NULL, at a series' edge). After maintenance 58 → 43 ms; from the 10-minute views avg-daily-driving-duration 84 → 41 ms, daily-activity 89 → 51; one truck's queries 221 → 13 ms (ordered by series); whole-fleet time ranges slower (stationary-trucks 12 → 69 ms) | the tuned configuration as built |
| Lance maintenance through the proxy (#164) | TSBS IoT small; s3: RustFS 1.0.1 (native) behind tools/s3proxy, 20 ms | `lance maintenance through the proxy (#164)`: Lance tuned, maintenance through the counting proxy (no `BENCH_LANCE_DIRECT`), pylance 13.0.0 | 1 | Maintenance ok: 15.6 s, 9,086 requests counted, 5,187 → 23 objects; every query ran. The multipart failures of 2026-10-12 (V0.md) didn't recur | `BENCH_LANCE_DIRECT` removed: Lance's maintenance is counted like every other system's |

## Rollups for the TSBS workloads (copies of the standard lakes, CLI best of 3, routing off → on, same answers)

| Workload | Rollups | Queries that change | Kept |
|---|---|---|---|
| TSBS IoT | 10-minute per truck on readings (velocity, fuel sums and counts) and diagnostics (status, load) | avg-daily-driving-duration 95 → 30 ms, avg-daily-driving-session 100 → 40, avg-load 55 → 25, long-daily-sessions 44 → 35, daily-activity (tuned version, one bucket) 101 → 28; the rest within noise | both |
| TSBS DevOps | 1-minute (maxima) and hourly (maxima, sums, counts) per host | double-groupby-1/5/all 146 → 68, 236 → 88, 348 → 112 ms; the minute queries already read one host's hour (about 20 ms) and didn't move; the 1-minute rollup has 17 M rows and took 37 s to build | hourly only |

## Join filters listing more keys (#121)

TSBS IoT standard (1,004 of the trucks in the `South` fleet), CLI, interleaved, best of 5, same answers:
`hash_join_inlist_pushdown_max_distinct_values` 150 (DataFusion's default, the filter then holds the keys' bounds only)
against 10,000 (the filter lists the keys). The fleet's trucks are spread through every file, so listing them prunes
nothing and costs a hash lookup per row: avg-load 55 → 189 ms, avg-daily-driving-duration 94 → 258, daily-activity
112 → 262, long-daily-sessions 37 → 68; the queries without a fleet join unchanged. Decision: keep 150.

## First runs of point queries (#132)

On a quiet machine (load average 2–3), the release CLI's in-process first run (`--bench-runs 1`, median of 15 fresh
processes, interleaved), TSBS IoT standard: imported with the ingest-time column, imported without it
(`--no-ingest-time`), and the benchmark's own streamed and maintained table.

| Query | With | Without | Streamed |
|---|---:|---:|---:|
| one-truck-latest | 3.20 ms | 2.96 ms | 4.44 ms |
| one-truck-last-hour | 8.13 ms | 8.16 ms | 6.79 ms |
| last-loc | 5.08 ms | 5.07 ms | 6.49 ms |

The ingest-time column costs at most 0.2 ms, and the streamed table is back at the 2026-10-16 run's numbers
(one-truck-latest 4, one-truck-last-hour 7 ms): the 2026-10-19 rise to 8 and 13 ms came from a machine at load 40–100,
not from the code. Decision: nothing to change; first runs on a busy machine aren't comparable across runs.

## Declaring the sort order on byte ranges

Plain scans leave DataFusion to split large files into byte ranges, and so don't declare the files' (series, time)
order. A prototype split each sorted file into byte ranges itself, one per partition, and declared the order, so
aggregates by series could stream instead of hashing. TSBS IoT standard, interleaved, best of 5, same answers: worse
on most queries, as DataFusion then chooses order-preserving plans (avg-daily-driving-duration 100 → 185 ms,
daily-activity 145 → 261, breakdown-frequency 121 → 211, avg-load 61 → 74), better only on
avg-vs-projected-fuel-consumption (122 → 101). Decision: not kept.
