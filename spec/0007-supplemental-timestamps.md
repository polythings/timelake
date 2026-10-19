---
number: 7
title: Supplemental timestamps
area: Foundations
status: planned
updated: 2026-10-16
related: [6, 12]
issues: [118]
---

# 7. Supplemental timestamps

**Planned** · updated 2026-10-16
· related: [spec 6](0006-table-contract.md), [spec 12](0012-late-data-and-dedup.md)

## Summary

Declare the other times a sample carries (server, gateway, sent, corrected) in the contract, choose which time decides the latest copy of a row (`latest-by`), record per commit the ranges and delays of each time, and query a table as of an insertion time. Proposed, waiting for review.

## Design

**The need.** A sample often carries more than one time. In industrial data the source (a PLC or sensor) stamps when it measured; a server or gateway stamps when it received the value (OPC UA sends both: `SourceTimestamp` and `ServerTimestamp`); a device stamps when it sent a store-and-forward batch; a correction carries when it was made. Timelake adds its own insertion time (`_ingest_ts`, [spec 12](0012-late-data-and-dedup.md)). Users want to keep these times, query them, measure delays between them, and sometimes decide by one of them which copy of a reading is current.

**Today** they can already be ordinary timestamp columns: stored, queried, and pruned on (every column has Iceberg min/max bounds). What Timelake doesn't know is what they mean.

**Proposal: declare them in the contract.**
- **Declaring:** `timelake.supplemental-time-columns = server_ts, sent_ts` at creation (`supplemental_time_columns=[…]` in Python, `--time-columns server_ts,sent_ts` on the CLI). Each must be a timestamp; it is stored as µs UTC like the time column. Adding one later waits for schema evolution.
- **Which copy wins** (setting `latest-by`, [spec 25](0025-settings.md)): by default the latest `_ingest_ts`, then the later commit. A table can instead name a column, for example `latest-by = server_ts`, so a resent old reading never overrides a newer correction however late it arrives; `_ingest_ts` and commit order still break ties. Dedup on read, compaction, the last-value index and in-append dedup all use the same order. A numeric version column would work the same way; whether to allow non-timestamp columns is a question below.
- **What arrived when, from metadata:** each commit records the min and max of every supplemental time and of `_ingest_ts` in its snapshot summary, next to the time ranges it touched ([spec 12](0012-late-data-and-dedup.md)). "Which partitions got data the server stamped after 10:00" and freshness dashboards then need no scan.
- **Delays:** per commit, the largest gap between the time column and `_ingest_ts` (and each supplemental time) goes in the snapshot summary, so store-and-forward lateness is visible per gateway or table without a query; SQL can compute it per row (`_ingest_ts - time`).
- **As of an insertion time:** `SET timelake.as_of_inserted = '2026-01-05 10:00'` answers "what did we know at 10:00": rows inserted later are left out before dedup, so a reading corrected at 10:30 shows its 10:00 value. It reads only `_ingest_ts`, or the column `latest-by` names.

**Questions for review:**
- **Names:** `supplemental-time-columns` and `latest-by`, or role names (`server-time-column`, `sent-time-column`) that tools could rely on, OPC UA style?
- **Non-timestamp `latest-by`:** allow a version number or sequence column, or timestamps only?
- **As-of:** a session setting (switchable, works in any SQL), or also a table function (`as_of_inserted('iot.readings', TIMESTAMP …)`) for one table in a join?
- **Writers that don't fill them:** a supplemental column may be null (the source didn't send it); `latest-by` then falls back to `_ingest_ts` for those rows. Or require it?

## Open questions and issues

- [#118](https://github.com/polythings/timelake/issues/118) Review: supplemental timestamps (names, latest-by, as-of)

## History

- 2026-10-16: proposed (SPEC.md §7.4)
- 2026-10-19: moved from SPEC.md into its own spec.
