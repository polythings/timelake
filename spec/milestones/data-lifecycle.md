---
number: 11
title: Data lifecycle
status: planned
updated: 2026-10-19
description: Bring history in from TimescaleDB, Iceberg, or S3 Tables and publish it at once, adopt existing tables, and delete or erase data with an audit trail.
specs: [16, 15, 4, 17, 18, 10]
---

# Data lifecycle

**Planned** · updated 2026-10-19

## Summary

Everything a table's data goes through besides appends: importing history from files and databases with a cutover,
backfilling it into a live table in one atomic publish, adopting existing Iceberg tables, deletes that are audited and
can be undone, GDPR erasure within 7 days, and the other times a reading carries.

<!-- specs:start -->

## Specs

| # | Spec | Status | Versions |
|---:|---|---|---|
| 16 | [Import and migration](../0016-import.md) | In progress | 0.1.0 |
| 15 | [Backfill](../0015-backfill.md) | Planned | – |
| 4 | [Time-series table contract](../0004-table-contract.md) | In progress | 0.1.0, 0.5.0 |
| 17 | [Deletes](../0017-deletes.md) | Planned | – |
| 18 | [GDPR erasure](../0018-gdpr-erasure.md) | Planned | – |
| 10 | [Supplemental timestamps](../0010-supplemental-timestamps.md) | Planned | – |

<!-- specs:end -->

## History

- 2026-10-19: milestone written when the specs were regrouped one per feature.
