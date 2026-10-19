---
number: 12
title: Late data and dedup
area: Writing
status: done
updated: 2026-10-16
related: [6, 15, 21, 22]
issues: [38, 66, 106]
---

# 12. Late data and dedup

**Done** · updated 2026-10-16
· related: [spec 6](0006-table-contract.md), [spec 15](0015-maintenance.md), [spec 21](0021-rollups.md), [spec 22](0022-signal-aggregates.md)

## Summary

Late data is normal: it lands in its time partition and each commit records the ranges it touched. Copies of a row are removed by compaction and on read (the latest write wins by insertion time, then commit order); cold partitions compact sooner so dedup on read only touches recent data.

## Design

### Late and out-of-order data, dedup

- **Late data is normal**, not an error. It is written to its correct time partition. Each commit records the **time ranges it touched** (per partition, optionally per series bucket) in the snapshot summary.
- **Small-file control:** Late data scattered across old partitions creates small files. The compactor prioritizes partitions by *small-file count × query heat*.
- **Rollup invalidation:** The rollup refresher diffs snapshots since its last processed snapshot and recomputes only the affected buckets ([spec 21](0021-rollups.md)). No separate change log is needed.
- **Duplicates and corrections:** IoT devices resend data. Per-table policy:
  - `append` (default for raw): Duplicates are removed at compaction and at read time by the Timelake reader, using `dedup_key` and keeping the latest `_ingest_ts`.
  - `upsert`: Same as `append`, but exposed as a first-class semantic for corrections.
- **Trade-off:** Plain DuckDB may see duplicates in recently written, not-yet-compacted partitions. The Timelake DuckDB extension removes them on read. *Open question Q3 ([#106](https://github.com/polythings/timelake/issues/106)).*

**Built so far (2026-10-16).** The key is `(series…, time)`. The latest write wins: the higher Iceberg data sequence number (commit order), and within one append the later row. An append keeps only the last of equal keys, so every file is free of duplicates on its own. The Timelake reader removes copies on read (*merge* below); compaction merges only runs of files next to each other in commit order, and a merged file keeps the newest data sequence number it replaces. The first version cost 1–4.6× on not-yet-compacted IIoT data ([spec 33](0033-iiot-benchmark.md)), because there nearly every file of a day overlaps every other; see the measurements below for where it stands now.

**Insertion time, `_ingest_ts` (built 2026-10-16, table format 0.3.0).**
- Tables created with a time column get a `_ingest_ts` column (timestamp with time zone, µs, not null) unless created with `ingest_time = false` (`--no-ingest-time`). The table records it in `timelake.ingest-time-column`.
- The writer sets it once per append, to the time the append started, for every row. An input that already has the column (an import keeping the original arrival times) keeps its values.
- The latest write is the highest `(_ingest_ts, commit order)`; tables without the column use commit order alone. Writers with skewed clocks can therefore order copies by their clocks rather than by commit; commit order breaks ties.
- Other engines can dedup with it: `QUALIFY row_number() OVER (PARTITION BY <series…>, <time> ORDER BY _ingest_ts DESC) = 1`. Arrival delay (`_ingest_ts - time`) measures store-and-forward lateness per row.
- It costs little storage (one value per append, run-length encoded). Adding it needs table format 0.3.0: older Timelakes can't open such tables. Existing tables can't gain the column until schema evolution exists.

**Dedup on read strategies** (setting `read-dedup-strategy`, [spec 25](0025-settings.md)):
- **merge** (built): files that may share a key are read with their commit rank, split by a hash of the key, merged in key order per slice, and the last row of each key kept. Only filters on key columns may apply before the merge, so filters on other columns can't prune those files.
- **keys-first** (built 2026-10-16): pass 1 reads only the key columns (and `_ingest_ts` or the rank) of the files that may share keys, restricted by key filters, merges them and lists the older copies as `(file rank, key)`. Pass 2 is the normal parallel scan with every filter pushed down, anti-joined against that list. Pass 1 reads two or three columns instead of every projected one; pass 2 prunes as freely as a scan without dedup, because the list was built from all rows.
- **Measured (2026-10-16, IIoT 50 machines × 9 days, before maintenance, laptop):** keys-first was slower on every query (1.3–6×; pass 2's anti-join probe costs more than merging the value columns), so merge is the default and keys-first stays a setting.
- **What made merge cheap:** splitting by the series hash inside the dedup operator (DataFusion otherwise re-sorts each slice), keeping each series whole in one slice so windows and groups by series need no new sort, filtering rows by key filters and a join's runtime filters before the merge, and comparing neighbouring keys column by column instead of row by row. Window queries (`gaps`, `time-above-limit`) then cost the same with and without dedup; scans of every row (`daily-stats`) still cost 2.6×, which cold-partition compaction removes (1.1×), leaving only the newest day to merge.

**Cold partitions compact sooner (built 2026-10-16, setting `compact-cold-after`).** Inline maintenance rewrites a partition into one sorted, duplicate-free file once its time range ended longer ago than the setting (default 1 hour) and it has more than one file. Backfill into a cold partition triggers the same rewrite after `compact-min-files` new files, not after every append. Dedup on read then only touches the current partitions, and DuckDB sees copies for a shorter time.

## Open questions and issues

- [#38](https://github.com/polythings/timelake/issues/38) Reader: deduplicate only files that may overlap, flagged at write time
- [#66](https://github.com/polythings/timelake/issues/66) Maintenance: keep late data for a closed slot in its own tier
- [#106](https://github.com/polythings/timelake/issues/106) Question: may plain DuckDB readers see duplicates before compaction

## History

- 2026-10-16: dedup on read (merge and keys-first), `_ingest_ts`, cold-partition compaction built
- 2026-10-19: moved from SPEC.md into its own spec.
