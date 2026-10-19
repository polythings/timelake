# Experiments of 2026-10-19 (P7, P8)

Decisions measured outside timebench, with scratch builds, the CLI, DataFusion from Python or `EXPLAIN ANALYZE`; no
run files. The numbers and decisions are in [`../../PERFORMANCE.md`](../../PERFORMANCE.md). M1 Pro laptop, release
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
| Time in a state | IIoT `time-above-limit`, `uptime` | `lead()` window, signal aggregate of a condition | 334 → 191 ms, 33 → 31 ms, same answers | conditions as values (P7: time in a state) |
| Last-value index per commit | 100k and 1M series, 100k-row appends | sort vs k-way merge of the indexes; ZSTD 3, ZSTD 1, LZ4, none | append 264 → 171 ms at 1M; codecs 50–78 ms | merge, ZSTD 3 (P10: extending the last-value index) |
