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
