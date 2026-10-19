# Experiments of 2026-10-17/18 (P7, P8, P10)

A/B runs made while deciding defaults for 0.7.0. They aren't charted on the site (the published runs are in
[`../../runs`](../../runs)); they're kept so a decision can be checked or redone. Each file in `runs/` is a timebench
run file; its `label` names the arm. The measured results and decisions are written up in
[`../../PERFORMANCE.md`](../../PERFORMANCE.md); this index says which runs belong to which experiment and what
each arm changed.

**Machine and method:** M1 Pro laptop (10 cores), release builds, Timelake reader only, `--reuse` of tables loaded
once, `--checkpoints maintained`, one first run and 2–5 repeats per query in a fresh process. Arms ran interleaved.
Compare arms by the median of their first runs and their best repeats per query, geomean over queries.

**How the arms differ:** the run files of these experiments don't record it (timebench now records
`TIMEBENCH_TIMELAKE_SETTINGS` and `TIMEBENCH_TIMELAKE_OPTIONS` in the run header as `reader_settings` and
`reader_options`). Each arm set the session settings or storage options below through those variables, or ran a
different build (its run's `commit`; `dirty: true` means a build of uncommitted changes, as described).

**Targets:** `local` is local disk. `s3` is RustFS behind `tools/s3proxy` with `ttfb_ms` added per request and, where
`mibps` is set, each response capped at that many MiB/s. Runs before 2026-10-18T04:30Z used RustFS in Docker
Desktop, which stalls bursts of new connections by about 200 ms: their first runs are bimodal and unreliable
(experiments 5 and 6). Later `s3` runs used RustFS's native macOS build.

| # | Experiment | Workload, target | Arms (label: what differs) | Runs | Result | Decision |
|---|---|---|---|---:|---|---|
| 1 | Baselines before P8 | TSBS IoT small and standard, DevOps standard; local | `p8-baseline`, `p8-baseline-standard`, `p8-devops-baseline`: defaults at `7bdccb5` | 3 | IoT standard geomean first 30 ms, best 11 ms; DevOps 31 / 17 ms | Reference |
| 2 | Parquet filter pushdown, IoT | TSBS IoT standard; local | `off`, `off-2`: `datafusion.execution.parquet.pushdown_filters=false`; `pushdown`: `…=true`; `pushdown+reorder`: and `reorder_filters=true` | 4 | Best geomean 10.7 → 10.3 → 10.2 ms; selective queries up to −40%, others up to +20% | [P8 filter pushdown](../../PERFORMANCE.md#p8-filter-pushdown-by-store-2026-10-17) |
| 3 | Parquet codec | TSBS IoT standard; local | `codec-zstd3`, `codec-zstd1`, `codec-lz4`, `codec-snappy`: readings written with that `write.parquet.compression-codec` | 4 | Best 11.5 / 11.1 / 12.3 / 10.8 ms; data 200 / 204 / 242 / 277 MB | Keep ZSTD 3; honour the property |
| 4 | Parquet filter pushdown, DevOps | TSBS DevOps standard; local | `off`, `off-2` and `pushdown+reorder`, `pushdown+reorder-2`, as in 2 (commit `d215922`) | 4 | Geomean first/best 30.6/16.5, 30.6/16.2 → 28.9/15.6, 29.2/15.5 ms | Pushdown on for local files |
| 5 | Filter pushdown on object storage (Docker RustFS, unreliable) | TSBS IoT standard; s3, 20 ms | `s3-off` (the load and its queries), `s3-false`: `pushdown_filters=false`; `s3-true`: `pushdown_filters=true` and `reorder_filters=true` | 14 | Median first 169 vs 181 ms; best 13.5 vs 12.6 ms; bimodal first runs | Superseded by 8–9 |
| 6 | Block cache: fetch a read's runs together (Docker RustFS, unreliable) | TSBS IoT standard; s3, 20 ms, 50 MiB/s set by hand | `base50`: build of `284d89c`; `concurrent50`: that plus concurrent fetches | 10 | Median first 187 vs 175 ms, bimodal | Superseded by 8 |
| 7 | Load on native RustFS | TSBS IoT standard; s3, 20 ms | `native-base`: load and queries at `284d89c` | 1 | First geomean 126 ms | Reference for 8–10 |
| 8 | Concurrent fetch, and pushdown, on native RustFS | TSBS IoT standard; s3, 20 ms, 50 MiB/s | `n-push-off`, `n-push-on`: build `284d89c`, `timelake.filter_pushdown=false` / `true`; `n-concurrent`: the concurrent-fetch build, pushdown off | 12 | First 162 / 161 / 125 ms; best 13.3 / 12.4 / 12.7 ms | Concurrent fetch (`780b874`) |
| 9 | Pushdown with concurrent fetch | TSBS IoT standard; s3, 20 ms, 50 MiB/s and no cap | `c-push-off`, `c-push-on` (50 MiB/s), `c0-push-off`, `c0-push-on` (no cap): `timelake.filter_pushdown=false` / `true` | 14 | First 125 → 147 ms (cap), 102 → 115 ms (no cap); best 12.5 → 11.9 / 11.5 ms | `filter-pushdown = auto`: off on object storage |
| 10 | Largest GET of the block cache | TSBS IoT standard; s3, 20 ms | `split-N` and `s2-N` (50 MiB/s), `s2u-N` (no cap): `timelake_cache_request_bytes` = N MiB, 0 for no limit; `s3-1m`: 1 MiB; `s3-512k`: 512 KiB with `timelake_cache_block_bytes` 512 KiB | 54 | First, 50 MiB/s: 126 (no limit), 126 (8, 4), 123 (2), 115 (1), 111 ms (512 KiB); no cap: 104 (no limit), 104 (2), 105 ms (1) | 1 MiB default (`f8c9b22`) |
| 11 | Cost of dedup on read (Q4) | IIoT small; local, loaded with inline maintenance only | `iiot-ingested`: the load; `dedup-true`, `dedup-false`: `timelake.read_dedup` | 5 | Best geomean 45.0 vs 37.3 ms; key comparisons 2 ms of a 100 ms query | No hidden `_series_id` (Q4) |
| 12 | Pruning with long `IN` lists, local | IIoT small; local, maintained | `iiot-maint`: the load; `ilp-true`, `ilp-false`: `timelake.in_list_pruning`, first build (`eb7ecc9`+, join filters split everywhere); `ilp2-true`, `ilp2-false`: final build (join filters only without pushdown) | 7 | First build: best 47.6 → 50.5 ms (`daily-stats` +52%, `energy` −49%); final: 49.8 → 49.0 ms | Join filters only on object storage |
| 13 | Pruning with long `IN` lists, object storage | IIoT small; s3, 20 ms, 50 MiB/s | `iiot-s3`: the load; `s3ilp-true`, `s3ilp-false`: `timelake.in_list_pruning` | 7 | Best 38.4 → 36.7 ms; `energy` 54 → 26 ms best, 204 → 183 ms first | On (`076e9c2`) |

The scale runs of the same days (IIoT `large` at 100,000 and 400,000 series, one failed before the fix for appends
past 2 GiB) are published runs, in [`../../runs`](../../runs).

## Experiments outside timebench

Measured with scratch tests, the CLI or `EXPLAIN ANALYZE`; their numbers are in [`PERFORMANCE.md`](../../PERFORMANCE.md):

| Experiment | Section of PERFORMANCE.md |
|---|---|
| Filter pushdown with 20 ms per GET and no cache (in-process store over local files) | P8: filter pushdown, by store |
| Docker Desktop's stall on bursts of new connections (fresh vs pooled connections, native vs container) | P8: block cache fetches separate runs at once |
| Hidden `_series_id`: dedup operator time, `IN` lists of 20 vs 21 tags | Q4: no hidden `_series_id` column |
| `IN`-list pruning: literal lists and joins, access plan vs predicate split, keep vs replace the list | P8: pruning with long `IN` lists |
| Last-value index cost by series count; k-way merge vs sort for the index | P10: 100,000 series |
| k-way merge on alternating inputs: before, gathered in one pass, galloping; TSBS compaction | P10: k-way merge on inputs that alternate key by key |
| Coarser queries and rollups of rollups on a 1-minute rollup | P7: coarser queries from a rollup |
| `m4`: per-group vs group accumulator, raw vs rollup | P7: `m4` |
| `time_weighted_mean`: segments vs windows | P7: `time_weighted_mean` |
| Newest-first scans on TSBS (local, S3) and IIoT; filters through the operator | P8: newest-first scans |
