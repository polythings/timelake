---
number: 24
title: Pruning with long IN lists
area: Reading
status: done
updated: 2026-10-18
versions: [0.7.0]
related: [4, 21]
issues: [103, 121]
---

# 24. Pruning with long IN lists

**Done** · updated 2026-10-18 · versions 0.7.0 · milestones: [Fast first queries on object storage](milestones/fast-first-queries.md), [Millions of series](milestones/millions-of-series.md)
· related: [spec 4](0004-table-contract.md), [spec 21](0021-reader.md)

## Summary

`column IN (…)` filters of more than 20 values skip row groups and pages whose range holds none of the values, which
DataFusion's pruning leaves alone. On object storage the same pruning uses a join's runtime filter, so a dashboard
over a list of devices reads only their pages.

## Design

- **Setting `in-list-pruning`** (on by default; table and session). Literal lists of more than 20 values, and on
  object storage a join's runtime filter, prune row groups and pages by their bounds (#103).
- **Measured** (2026-10-18, IIoT, compacted, local disk): a 100-tag list 54 → 31 ms, a 21-tag list 53 → 26 ms. IIoT
  `energy` on local S3 (20 ms per request): 54 → 26 ms repeated, 204 → 183 ms first run.
- **Alternative decided against (Q4):** a hidden `_series_id` column holding the series hash, so lists of series
  prune on one integer column. Pruning by the key columns' own bounds did as well without changing the table
  ([spec 4](0004-table-contract.md)).
- **Limit:** DataFusion builds a join's runtime filter list for at most 150 keys
  (`hash_join_inlist_pushdown_max_distinct_values`); past that, a dashboard over thousands of devices prunes by bounds
  only (#121, [the milestone "Millions of series"](milestones/millions-of-series.md)).

## Open questions and issues

- [#103](https://github.com/polythings/timelake/issues/103) Reader: prune row groups and pages for series-key sets of more than 20 values (IN-lists, join filters)
- [#121](https://github.com/polythings/timelake/issues/121) Pruning for dashboards over thousands of devices: join runtime filters cap at 150 keys

## History

- 2026-10-18: built and measured (P8, #103)
- 2026-10-19: moved from SPEC.md into spec 20, then into this spec when the specs were regrouped one per feature.
