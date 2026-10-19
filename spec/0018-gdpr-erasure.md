---
number: 18
title: GDPR erasure
area: Writing
status: planned
updated: 2026-10-11
versions: []
related: [17, 35]
---

# 18. GDPR erasure

**Planned** · updated 2026-10-11 · milestone: [Data lifecycle](milestones/data-lifecycle.md)
· related: [spec 17](0017-deletes.md), [spec 35](0035-multitenancy.md)

## Summary

Erasure of a data subject: an immediate logical delete across every table, then a batched physical purge (daily by default, `--now` on demand, alerts past 7 days) that reaches every copy, verified and certified without logging personal data.

## Design

A logical delete hides data but is not erasure: the bytes still exist in the original files and older snapshots. GDPR requires physical removal from **every copy**.

**Identifying the data subject.** In IoT, personal data is usually *indirect*: a person is linked to a vehicle, wearable, smart meter, or badge, not to individual rows. Tables can therefore declare a `subject_key`:
- **Direct:** a column on the table itself, e.g. `user_id`.
- **Indirect:** a mapping through a dimension table, e.g. `devices.owner_id`. Erasing a person resolves to "these series, over these time ranges of ownership."

**Erasure workflow**
1. **Request:** `timelake erase --subject <id>` (or an explicit predicate) creates an erasure request with an ID and a deadline.
2. **Immediately:** A logical delete is applied across every affected table, and the hot tier is evicted. The data is invisible within seconds.
3. **Physical purge** rewrites files, which is the expensive part, so it is *batched* by default:
   - `purge_schedule = daily` (default): All erasure requests from the day are purged together in the next maintenance run, so a file shared by several requests is rewritten once.
   - `timelake erase … --now`: Purge immediately. This takes minutes; duration scales with the amount of data touched, not the table size.
   - `max_purge_delay` (default 7 days): A hard upper bound. If a purge hasn't completed by then (e.g. jobs failing), Timelake alerts. GDPR expects erasure "without undue delay", generally within one month.
   - The purge job:
     - Rewrites affected files without the rows. The series-sorted layout keeps the number of touched files small.
     - Expires every snapshot that references the old files and deletes the orphaned files.
4. **Verify and certify:** A verification query confirms zero matching rows remain. The request is closed with a record of files rewritten, snapshots expired, and objects deleted.

**Coverage checklist (what purge must reach)**

| Location | Action |
|----------|--------|
| Raw tables, events table | Rewrite files; expire snapshots; delete orphans |
| Rollups | Recompute affected buckets; delete per-subject rollup rows (aggregates across many subjects are generally not personal data) |
| Hot tier | Delete/rewrite WAL segments; evict Valkey/DB keys |
| Puffin indexes, bloom filters, sketches | Rebuild for rewritten files |
| Catalog and reader caches | Invalidate; local caches have a bounded TTL |
| Ingest state checkpoints | Remove per-series state |
| **Bucket versioning / replication** | Purge must also delete non-current object versions and replicas, or the job fails loudly. |
| User backups outside Timelake | Out of scope; documented |

**Audit without leaking personal data:** Erasure audit records store the request ID and a *salted hash* of the subject identifier, never the raw identifier or predicate values. (Ordinary logical-delete audits, [spec 17](0017-deletes.md), store the predicate as-is.)

**Time-travel trade-off:** Erasure shortens time travel for affected files, because snapshots older than the purge that reference purged files must be expired. This is documented and surfaced in the erasure report.

**Tenant offboarding: crypto-shredding (optional):**
- With a warehouse or bucket per tenant ([spec 35](0035-multitenancy.md)), each tenant can have its own KMS encryption key.
- Destroying that key makes all of the tenant's data unreadable instantly, followed by asynchronous deletion of the objects.
- This doesn't work for individual subjects inside a shared table; those use the purge workflow above.

**Legal hold / WORM:** Not in v1 (Q11 resolved). Purge must always be possible for GDPR. Per-table legal hold may come later, with explicit rules for how it interacts with erasure requests.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D8 | GDPR erasure is supported: immediate logical delete, then a batched physical purge (daily by default, `--now` on demand, alert past 7 days), covering every copy of the data ([spec 18](0018-gdpr-erasure.md)). No legal hold / WORM in v1. | Accepted |

## Open questions and issues

None open.

## History

- 2026-10-11: D8 accepted (SPEC.md §9.3); Q11 and Q13 resolved
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 14; specs regrouped one per feature, with milestones on the roadmap.
