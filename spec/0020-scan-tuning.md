---
number: 20
title: Scan tuning
area: Reading
status: in-progress
updated: 2026-10-19
related: [17, 19, 26, 6]
issues: [33, 34, 36, 37, 55, 59, 65, 72, 74, 75]
---

# 20. Scan tuning

**In progress** · updated 2026-10-19
· related: [spec 17](0017-read-paths.md), [spec 19](0019-block-cache.md), [spec 26](0026-series-cardinality.md), [spec 6](0006-table-contract.md)

## Summary

Reading and decoding less on selective scans: late materialization chosen per store, request sizing, pruning with long `IN` lists, newest-first scans for rows and buckets, windows reading each series in order, and dictionary-encoded floats.

## Design

Steps that make the reader's scans read and decode less (P8 of the performance plan, spec 27). Each one is a setting,
measured on and off ([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md), sections P8).

### Built

- **Late materialization** (setting `filter-pushdown`): filters applied while decoding Parquet, other columns decoded
  only for rows that pass. `auto` turns it on for local files and off on object storage, where it adds a round trip
  per row group. TSBS geomeans 4–5% faster on local disk, selective group-bys up to 40%; first queries 12–17% slower
  at 20 ms per request (2026-10-17).
- **Request sizing** (#65, Rust reader): the block cache fetches a read's runs at once (first queries on S3 162 →
  125 ms) in GETs of at most 1 MiB (another 9% at 50 MiB/s per request) (2026-10-18).
- **Pruning with long `IN` lists** (#103, setting `in-list-pruning`): lists of more than 20 values skip row groups
  and pages, for literal lists and, on object storage, for a join's runtime filter. 100 tags 54 → 31 ms (IIoT, local)
  (2026-10-18). A hidden `_series_id` column was measured and decided against (Q4, spec 6).
- **Newest-first scans** (#72, setting `newest-first`): `ORDER BY time DESC LIMIT k` reads time partitions newest
  first and stops early: 100 newest before a past time 298 → 38 ms (2026-10-18). The same for the k newest buckets of
  `GROUP BY time_bucket(…) ORDER BY bucket DESC LIMIT k`: TSBS DevOps `groupby-orderby-limit` 254 → 115 ms
  (2026-10-19).
- **Windows over each series in series order** (setting `window-series-order`): a window partitioned by the series
  key and ordered by time reads the files merged in series order, so DataFusion neither repartitions nor sorts. `auto`
  when the files are in one time partition: IIoT `gaps` 260 → 215 ms; a week of files 2.0 → 3.0 s, so `auto` leaves
  those (2026-10-19).
- **Float encoding** (setting `float-encoding`): dictionaries, falling back to byte-stream-split. IIoT data a third
  of the size; TSBS DevOps scans up to twice as fast (2026-10-19). The dictionary size limit stays Parquet's 1 MiB:
  128 KiB was a few percent faster on TSBS IoT and made IIoT data three times larger.

## Open questions and issues

- [#33](https://github.com/polythings/timelake/issues/33) Write: bloom filters on high-cardinality tag columns
- [#34](https://github.com/polythings/timelake/issues/34) Query: tune the partial-aggregation skip threshold
- [#36](https://github.com/polythings/timelake/issues/36) Reader: a series index that covers some files, with statistics pruning for the rest
- [#37](https://github.com/polythings/timelake/issues/37) WASM reader: plan byte ranges from page indexes, not whole row groups
- [#55](https://github.com/polythings/timelake/issues/55) Write: delta encoding for monotonic integer counters
- [#59](https://github.com/polythings/timelake/issues/59) Query: cap the number of time buckets
- [#65](https://github.com/polythings/timelake/issues/65) Reader: size and schedule object-store requests for S3 (2–16 MiB, concurrent)
- [#72](https://github.com/polythings/timelake/issues/72) Query: newest-first pruning for latest-before-T and ORDER BY time DESC LIMIT
- [#74](https://github.com/polythings/timelake/issues/74) Write: ALP encoding for float columns once Parquet and arrow-rs support it
- [#75](https://github.com/polythings/timelake/issues/75) Query: re-measure Parquet filter pushdown (late materialization)

## History

- 2026-10-17: started (P8 in SPEC.md §19.1)
- 2026-10-18: request sizing, `IN`-list pruning, newest-first
- 2026-10-19: newest buckets, windows in series order, float encoding
- 2026-10-19: moved from SPEC.md into its own spec.
