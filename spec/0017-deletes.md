---
number: 17
title: Deletes
area: Writing
status: planned
updated: 2026-10-11
versions: []
related: [18, 27]
issues: [16, 41]
---

# 17. Deletes

**Planned** · updated 2026-10-11 · milestone: [Data lifecycle](milestones/data-lifecycle.md)
· related: [spec 18](0018-gdpr-erasure.md), [spec 27](0027-rollups.md)

## Summary

Deletes never mutate data: a logical delete adds deletion vectors and an audit record and can be undone; retention drops whole partitions (built); a purge is the explicit, audited, irreversible exception.

## Design

**Principle:** A delete never mutates or destroys existing data. It is a new, audited, immutable fact layered on top. Iceberg already works this way: data files are never modified, and deletes are recorded as *new* delete files / deletion vectors in a new snapshot.

| Operation | Mechanism | Visible to plain DuckDB? | Reversible? |
|-----------|-----------|--------------------------|-------------|
| **Logical delete** (default) | Predicate → Iceberg v3 deletion vectors (v2: position deletes). Never rewrites data files. | Yes (if the engine supports the delete-file type; *verify DuckDB v3 DV support*) | **Yes**: `UNDELETE <delete_id>` commits a snapshot that removes those delete vectors. Original rows were never touched. |
| **Retention drop** | Whole-partition removal by policy | Yes | Until the snapshot expires |
| **Purge** (hard delete, for compliance) | Explicit, privileged operation: rewrites affected files without the rows, expires snapshots that still reference them, removes orphans | Yes | **No**, by design. Requires a reason and is audited. |

**Time-series-shaped deletes are first-class:**
```sql
DELETE FROM iot.sensor
WHERE device_id = 'pump-7' AND ts BETWEEN '2026-10-01' AND '2026-10-02'
WITH (reason = 'sensor miscalibrated', ticket = 'OPS-123');
```

**Audit trail:**
- Every delete gets a `delete_id` and records actor, timestamp, predicate, reason, and affected row count.
- These records go in the snapshot summary **and** in an append-only `_timelake_audit` Iceberg table, so audits are a SQL query.

**Consistency with rollups:** A delete, undelete, or purge marks its time ranges as changed, so the affected rollup buckets are recomputed ([spec 27](0027-rollups.md)). Rollups never silently include deleted data.

**Corrections vs. deletes:** Bad values usually need *correcting* rather than deleting. An `upsert` writes the corrected row, and dedup keeps the latest `_ingest_ts`. The original stays in older snapshots, so history is preserved without a delete.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D6 | Deletes are immutable: logical, audited, and reversible; physical purge is a separate explicit operation ([spec 17](0017-deletes.md)) | Accepted |

## Open questions and issues

- [#16](https://github.com/polythings/timelake/issues/16) Deletes: bump the format version so older binaries can't resurrect deleted rows
- [#41](https://github.com/polythings/timelake/issues/41) Deletes: Iceberg v3 deletion vectors, one per data file (Q1)

## History

- 2026-10-11: D6 accepted (SPEC.md §9.2)
- 2026-10-12: retention drops built
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 13; specs regrouped one per feature, with milestones on the roadmap.
