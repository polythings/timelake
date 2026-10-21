---
number: 4
title: Exact answers before compaction
status: done
versions: 0.5.0
updated: 2026-10-17
description: Rows sent twice count once, from the moment they land: dedup on read, an insertion time per row, and cold partitions compacted sooner.
specs: [12, 19]
---

# Exact answers before compaction

**Done** · versions 0.5.0 · updated 2026-10-17

## Summary

Devices resend data, and compaction runs later. The Timelake reader keeps only the latest write of each reading from
the moment it lands, each row records when it arrived, compaction never lets an older write win, and partitions that
are no longer written are compacted sooner so dedup on read only touches recent data.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 12 | [Late data and dedup on read](../0012-late-data-and-dedup.md) | Done | 0.5.0 |
| 19 | [Compaction](../0019-compaction.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased |

<!-- specs:end -->

## History

- 2026-10-16: dedup on read, insertion time, cold partitions, compaction fix (0.5.0)
- 2026-10-19: milestone written when the specs were regrouped one per feature.
