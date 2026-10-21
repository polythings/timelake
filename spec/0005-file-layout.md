---
number: 5
title: File layout and encodings
area: Tables and storage
status: in-progress
updated: 2026-10-20
versions: [0.1.0, 0.6.0, 0.7.0, unreleased]
related: [4, 19, 23]
issues: [33, 44, 55, 74, 76, 131]
---

# 5. File layout and encodings

**In progress** · updated 2026-10-20 · versions 0.1.0, 0.6.0, 0.7.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Fast first queries on object storage](milestones/fast-first-queries.md)
· related: [spec 4](0004-table-contract.md), [spec 19](0019-compaction.md), [spec 23](0023-filter-pushdown.md)

## Summary

How a Timelake table's data sits in Parquet: time partitions from the contract, files sorted by (series…, time),
1M-row row groups with page indexes, delta-encoded times, dictionary-encoded values (floats falling back to
byte-stream-split) and ZSTD. Parquet stays the format; Vortex was measured and kept for places Timelake owns the read
path.

## Design

### Physical layout defaults (derived from the contract; all overridable)
- **Partitioning:** `<chunk_interval>(time_column)` plus optional `bucket(N, series_id)` or `identity(tenant)`, chosen
  automatically from expected volume.
- **Tiered partition granularity (planned, #131):** Recent data is partitioned by `hour(ts)`. After compaction, older
  data moves to `day(ts)` or `month(ts)`. This uses Iceberg partition evolution plus a rewrite.
- **Sort order:** `(series_key…, time_column)` within each file. This gives excellent compression (delta-encoded
  timestamps, similar adjacent values) and tight row-group statistics for series pruning. The sort order is declared
  in the table metadata only when there are enough files to keep scans parallel (a v0 lesson).
- **File sizing:** Target 128–512 MB files with row groups sized for selective reads. Tunable per table. Row groups
  of 1M rows: page indexes keep single-series reads selective (v0).
- **Pages:** `data-page-rows` rows per data page (default 20,000; 8,192 for files written from JavaScript, where
  compaction holds a page per column of each file it merges, [spec 13](0013-wasm-writes.md)). Built 0.6.0.
- **Writer:** Timelake writes Parquet with its own writer, shared with the WebAssembly writer, encoding each row
  group's columns on threads of their own (0.6.0: the same files byte for byte, compaction of TSBS IoT 2.37 → 1.17 s).
- **Codec:** the codec in Iceberg's `write.parquet.compression-codec` and `write.parquet.compression-level` (ZSTD 3 by
  default; `lz4`, `snappy`, `gzip`, `brotli`, `uncompressed`). Built 0.7.0. A level the codec doesn't have fails the
  write rather than falling back to the default, and creation and `alter` reject an unknown codec or such a level
  (unreleased).
- **Encodings:** Parquet `DELTA_BINARY_PACKED` for the time column, dictionaries for the other columns (floats
  falling back to `BYTE_STREAM_SPLIT` once the dictionary outgrows its page; setting `float-encoding`), and ZSTD.
  Measured 2026-10-19 against `BYTE_STREAM_SPLIT` for floats: IIoT readings a third of the size (366 → 119 MB for 5
  days) at equal or better speed, TSBS DevOps 38% larger but scans up to twice as fast (`high-cpu-all` 274 →
  136 ms), random doubles +5% size and +6% scan time. The dictionary size limit stays Parquet's 1 MiB: 128 KiB was a
  few percent faster on TSBS IoT and made IIoT data three times larger.

### Vortex (Q8, measured 2026-10-11)

Keep a file-format abstraction from day one, for when Iceberg supports pluggable formats. Measured
([`bench/results/VORTEX.md`](../bench/results/VORTEX.md)), via DuckDB and via DataFusion with `vortex-datafusion`:
on local disk Vortex is 20–45% faster, 2.4–4× on single-device queries; on object storage at parity to 1.5× slower with
DataFusion (more requests), 1.3–3× slower with DuckDB (no cache); and its files are larger in our sorted layout. Hence
D16.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D16 | Parquet stays the lake's source of truth (Iceberg-compatible). Vortex is used where Timelake owns the read path and data is local: reader SSD cache, hot tier, edge. Re-evaluate when Iceberg supports Vortex files or Vortex object-store I/O matures (`bench/results/VORTEX.md`). | Proposed |

## Open questions and issues

- [#33](https://github.com/polythings/timelake/issues/33) Write: bloom filters on high-cardinality tag columns
- [#44](https://github.com/polythings/timelake/issues/44) Write: check whether time-ordered file names concentrate S3 load
- [#55](https://github.com/polythings/timelake/issues/55) Write: delta encoding for monotonic integer counters
- [#74](https://github.com/polythings/timelake/issues/74) Write: ALP encoding for float columns once Parquet and arrow-rs support it
- [#76](https://github.com/polythings/timelake/issues/76) Spike: a time-series file format of our own (research)
- [#131](https://github.com/polythings/timelake/issues/131) Tiered partition granularity: hour for recent data, day or month after compaction

## History

- 2026-10-11: layout defaults; Vortex measured (D16)
- 2026-10-12: built in v0, with its reader lessons (1M-row row groups, sort order declared only with enough files)
- 2026-10-17: own Parquet writer, columns encoded in parallel; `data-page-rows`
- 2026-10-18: codec from Iceberg's write properties
- 2026-10-19: dictionary-encoded floats (`float-encoding`); moved from SPEC.md into spec 6 and spec 20, then into this
  spec when the specs were regrouped one per feature.
- 2026-10-20: a compression level the codec doesn't have is an error, at creation and `alter` too
