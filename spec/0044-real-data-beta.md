---
number: 44
title: Real-data beta
area: Services and planned features
status: planned
updated: 2026-10-18
related: [26, 27, 33]
---

# 44. Real-data beta

**Planned** · updated 2026-10-18
· related: [spec 26](0026-series-cardinality.md), [spec 27](0027-performance-plan.md), [spec 33](0033-iiot-benchmark.md)

## Summary

Run Timelake on a production-like industrial dataset with parallel writers fed from a Kinesis stream, measure ingest, commit contention, latency and cost on real access patterns, and tune the defaults from what it shows.

## Design

After the scan tuning (spec 20) and the remaining rollup work: a beta on a company's production-like industrial dataset,
with parallel writers fed from a Kinesis stream. P9 of the performance plan (spec 27).

- **Measure:** ingest throughput, commit contention between parallel writers, query latency, and the monthly bill
  (requests, storage, compute) on real data and access patterns.
- **Then:** tune settings and defaults for what they show, rather than for synthetic benchmarks only.

## Open questions and issues

None open.

## History

- 2026-10-18: planned (P9)
- 2026-10-19: moved from SPEC.md into its own spec.
