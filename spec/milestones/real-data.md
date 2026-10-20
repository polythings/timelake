---
number: 9
title: Tuned on real data
status: planned
updated: 2026-10-18
description: A beta on a production industrial dataset with parallel writers fed from Kinesis: throughput, latency, and cost measured, then tuned for.
specs: [2, 11, 20]
---

# Tuned on real data

**Planned** · updated 2026-10-18

## Summary

Run Timelake on a production-like industrial dataset with parallel writers fed from a Kinesis stream, measure ingest,
commit contention, latency and cost on real access patterns, and tune the defaults from what it shows. Was P9 of the
performance plan, after the scan tuning and the remaining rollup work.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 2 | [Commits](../0002-commits.md) | Done | 0.1.0, 0.2.0, unreleased |
| 11 | [Appends](../0011-appends.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0, unreleased |
| 20 | [Maintenance runs, retention and expiry](../0020-maintenance.md) | In progress | 0.1.0, 0.3.0, 0.5.0, unreleased |

<!-- specs:end -->

## Plan

- **Measure:** ingest throughput, commit contention between parallel writers ([spec 2](../0002-commits.md)), query
  latency, and the monthly bill (requests, storage, compute) on real data and access patterns.
- **Then:** tune settings and defaults for what they show, rather than for synthetic benchmarks only.

## History

- 2026-10-18: planned (P9)
- 2026-10-19: was spec 44; a milestone on the roadmap since the specs were regrouped one per feature.
