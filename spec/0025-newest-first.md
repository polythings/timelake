---
number: 25
title: Newest-first scans
area: Reading
status: done
updated: 2026-10-19
versions: [0.7.0, unreleased]
related: [21, 27]
issues: [72]
---

# 25. Newest-first scans

**Done** · updated 2026-10-19 · versions 0.7.0, unreleased · milestone: [Fast first queries on object storage](milestones/fast-first-queries.md)
· related: [spec 21](0021-reader.md), [spec 27](0027-rollups.md)

## Summary

`ORDER BY time DESC LIMIT k`, and the k newest buckets of a `GROUP BY time_bucket(…)`, read time partitions newest
first and stop once older partitions can't hold a row of the answer. "The latest readings before a time" and "the last
few buckets" stop scanning the whole table.

## Design

- **Setting `newest-first`** (on by default; table and session).
- **Rows** (0.7.0, #72): `ORDER BY time DESC LIMIT k` reads time partitions newest first, in one output partition,
  and stops once it holds k rows newer than everything left. IIoT, 30 days: the 100 newest readings before a past time
  298 → 38 ms; TSBS IoT on local S3: the 10 newest readings 392 → 320 ms.
- **Buckets** (2026-10-19): the k newest buckets of `GROUP BY time_bucket(…) ORDER BY bucket DESC LIMIT k` (one
  aggregate, no `HAVING`): older time partitions are skipped once they can't hold a row of those buckets. The scan's
  partitions run in parallel under the aggregate and share what they've seen; rows not newer than the k-th bucket seen
  are skipped. TSBS DevOps `groupby-orderby-limit` 254 → 115 ms. A first version with one output partition was slower
  (705 ms).

## Open questions and issues

- [#72](https://github.com/polythings/timelake/issues/72) Query: newest-first pruning for latest-before-T and ORDER BY time DESC LIMIT

## History

- 2026-10-18: rows built and measured (P8, #72)
- 2026-10-19: buckets; moved from SPEC.md into spec 20, then into this spec when the specs were regrouped one per
  feature.
