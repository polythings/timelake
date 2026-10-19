# Experiments of 2026-10-19 (P7, P8)

Decisions measured with scratch builds, the CLI, DataFusion from Python or `EXPLAIN ANALYZE`, and one set of timebench
A/B runs (in `runs/`, below). The numbers and decisions are in [`../../PERFORMANCE.md`](../../PERFORMANCE.md). M1 Pro laptop, release
builds, local disk, best of 7 runs of a fresh process (DataFusion from Python: best of 7 in one process).

| Experiment | Data | Arms | Result | Decision (section of PERFORMANCE.md) |
|---|---|---|---|---|
| Segment layout of the signal aggregates | IIoT small, hourly per tag | neighbours as `Option<f64>`, as `f64`, the open segment inline in each group | 2.30, 2.17, 2.95 s (0.7.0: 2.17 s) | `f64` (P7: `integral`, `counter_diff`, `counter_rate`) |
| Scan partitions under signal aggregates | IIoT small, `GROUP BY tag` over 30 days | files spread by size, stretches of time with and without DataFusion's work stealing | the first two fail with overlapping runs; stretches without stealing exact, 2.21 s hourly | stretches, stealing off (same section) |
| One state for several signal aggregates | IIoT small, hourly per tag | a state each, one shared | all four 3.47 → 2.11 s | shared, setting `shared-signal-state` (same section) |
| Windows over each series | IIoT small window queries, 1, 7 and 30 days | repartition and sort, the series-order merge | one file 260 → 215 ms, seven 1.95 → 3.03 s | merge within one time partition, setting `window-series-order` (P8: windows over each series) |
| Parquet files of TSBS DevOps | DevOps standard | Timelake's files rewritten without `sorting_columns`, with dictionary floats, hourly chunks | 843 → 303 → 212 ms (plain DataFusion), hourly 438 → 343 ms | (P8: float encoding) |
| Float encoding | DevOps standard, IIoT last 5 days, 4 M random doubles | byte-stream-split, dictionary, plain, dictionary falling back to byte-stream-split | see the section | dictionary with fallback, setting `float-encoding` (same section) |
| Gap-filling window functions | 10 M generated rows, 1,000 partitions | `forward_fill` vs `last_value IGNORE NULLS`, `backward_fill` vs `first_value IGNORE NULLS` | 0.26 vs 3.58 s, 0.27 vs 0.37 s | built (P7: gap filling) |
| Rollups for time ranges | IIoT small, a 1-minute and an hourly rollup | readings, each rollup, for statistics, gaps and time-weighted means over a day and 30 days; daily buckets | see the section | widest first, states only at enough readings per row (P7: rollups for time ranges) |
| Gap aggregates | IIoT `gaps`, a day and 30 days; hourly signal aggregates for the refactor | `lead()` window, `gap_count`/`gap_seconds`; signal aggregates before and after the generic run code | 226 → 77 ms, 17.8 → 1.36 s; 3.58 → 3.50 s, 4.80 → 4.68 s | built (P7: gaps in the readings) |
| Time in a state | IIoT `time-above-limit`, `uptime` | `lead()` window, signal aggregate of a condition | 334 → 191 ms, 33 → 31 ms, same answers | conditions as values (P7: time in a state) |
| Last-value index per commit | 100k and 1M series, 100k-row appends | sort vs k-way merge of the indexes; ZSTD 3, ZSTD 1, LZ4, none | append 264 → 171 ms at 1M; codecs 50–78 ms | merge, ZSTD 3 (P10: extending the last-value index) |
| Newest-first buckets | DevOps `groupby-orderby-limit`, IIoT newest rows | one or many output partitions, skipping old rows | 254 → 115 ms; rows 70 → 61 ms | many partitions for buckets, one for rows (P8: newest-first for the newest buckets) |

## Timebench runs

| Experiment | Workload, target | Arms (label: what differs) | Runs | Result | Decision |
|---|---|---|---:|---|---|
| Dictionary size limit | TSBS IoT standard; local | `dict-1m-r1/2`: Parquet's 1 MiB; `dict-128k-r1/2`, `dict-32k-r1/2`: the limit set by a scratch `TL_DICT_LIMIT`; `bss-r1/2`: byte-stream-split floats (`TL_BSS`) | 8 | Geomean first/best 21.8/10.5, 20.9/9.7, 22.0/9.5, 23.3/10.8 ms; an IIoT day 24.7 MB at 1 MiB, 71.2 MB at 128 KiB | 1 MiB stays (P8: float encoding) |
