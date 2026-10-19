---
number: 13
title: Technical preview
status: planned
updated: 2026-10-19
description: Timelake open for anyone to try on real workloads, labelled a technical preview, once tuning on real data, the engine connections, the data lifecycle and seconds-fresh ingestion are in.
specs: [2, 11, 20, 32, 33, 34, 35, 4, 10, 15, 16, 17, 18, 14, 36, 37]
issues: [145, 146]
---

# Technical preview

**Planned** · updated 2026-10-19

## Summary

A technical preview open to anyone: Timelake installed from the usual registries and run on real workloads, labelled
a preview, with its limits and what may still change written down. It opens once most of what a team needs is in:
the milestones before it on the roadmap (tuned on real data, any engine can attach, data lifecycle, seconds-fresh data
and ingestion). Its specs are theirs, so its progress is theirs.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 2 | [Commits](../0002-commits.md) | Done | 0.1.0, 0.2.0 |
| 11 | [Appends](../0011-appends.md) | Done | 0.1.0, 0.5.0, 0.6.0, 0.7.0 |
| 20 | [Maintenance runs, retention and expiry](../0020-maintenance.md) | In progress | 0.1.0, 0.3.0, 0.5.0 |
| 32 | [REST catalog](../0032-rest-catalog.md) | Planned | – |
| 33 | [DuckDB extension](../0033-duckdb-extension.md) | Planned | – |
| 34 | [Snowflake connection](../0034-snowflake.md) | Planned | – |
| 35 | [Multitenancy](../0035-multitenancy.md) | Planned | – |
| 4 | [Time-series table contract](../0004-table-contract.md) | In progress | 0.1.0, 0.5.0 |
| 10 | [Supplemental timestamps](../0010-supplemental-timestamps.md) | Planned | – |
| 15 | [Backfill](../0015-backfill.md) | Planned | – |
| 16 | [Import and migration](../0016-import.md) | In progress | 0.1.0 |
| 17 | [Deletes](../0017-deletes.md) | Planned | – |
| 18 | [GDPR erasure](../0018-gdpr-erasure.md) | Planned | – |
| 14 | [Hot tier and seconds-fresh reads](../0014-hot-tier.md) | Planned | – |
| 36 | [Ingestion and rules](../0036-ingest-and-rules.md) | Planned | – |
| 37 | [Data to events](../0037-data-to-events.md) | Planned | – |

<!-- specs:end -->

## Plan

- **Open to anyone:** no application; the website, the docs and the release notes say it is a technical preview.
- **Written down before it opens:** what is known not to work yet (the docs' Limitations page), what stays stable
  during the preview and what may change (#145), and where feedback goes (#146).
- **During the preview:** issues from preview users are triaged first; benchmarks and the weekly run keep going, so
  regressions show on the site.
- **What ends it** is decided before it opens (#145).

## History

- 2026-10-19: planned, gated on the four planned milestones before it, open to anyone.
