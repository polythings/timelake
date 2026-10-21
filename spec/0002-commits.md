---
number: 2
title: Commits
area: Tables and storage
status: done
updated: 2026-10-20
versions: [0.1.0, 0.2.0, unreleased]
related: [1, 3, 11]
issues: [26, 39, 47, 48, 51]
---

# 2. Commits

**Done** · updated 2026-10-20 · versions 0.1.0, 0.2.0, unreleased · milestones: [v0: a maintenance-free time-series table on S3](milestones/v0.md), [Fast first queries on object storage](milestones/fast-first-queries.md), [Tuned on real data](milestones/real-data.md), [Technical preview](milestones/technical-preview.md)
· related: [spec 1](0001-iceberg-tables.md), [spec 3](0003-object-store-catalog.md), [spec 11](0011-appends.md)

## Summary

Every append, compaction and upgrade is one atomic Iceberg commit, made by Timelake's own commit layer on iceberg-rust's
public types. Concurrent writers are safe without a catalog service: a conflicting commit is rebased and retried.
Commits take two steps: everything uploaded in parallel, then one conditional write.

## Design

**Commit layer (D13).** Append, replace, delete, overwrite, branch commits and rewrite-manifests, with manifest merging
and optimistic retry, on iceberg-rust's public types. Iceberg v2 by default.

**Concurrency.** A commit writes `vN+1.metadata.json` with put-if-absent ([spec 3](0003-object-store-catalog.md)). A
writer that loses the race reloads the table, rebases its change onto the new version (reusing its data files) and
tries again.

**Uncertain outcomes.** A conditional write can land and still report a failure: the store client retries a request
whose response was lost and gets "already exists" for the file it just created, and a timeout says nothing. After a
failed or refused write the writer reads `vN+1.metadata.json` back. Its own bytes there are the commit; another
writer's mean it lost. When that can't be told (the file can't be read, or is missing after an error, so the write may
still land), the commit fails as "commit outcome unknown" and deletes nothing it wrote: orphans only take space, while
deleting files a committed version references breaks the table. Metadata the writer couldn't load itself is never
written.

**Idempotency.** Each batch carries an ID that is recorded in the snapshot summary, so a retried commit is a no-op and
an append of a batch already committed reports 0 rows.

### Two-step commit (P4, built 2026-10-13)

A commit uploads everything it already knows (data files, manifest, last-value index, manifest list) in parallel, then
makes the one conditional write of `vN+1.metadata.json`. The version hint is updated afterwards, in the background
(Rust programs call `timelake::flush()` before exiting; the CLI and the Python package do). Commit p50 went 483 →
243 ms on S3 from a laptop, and the whole v0 ingest 1,307 → 795 s
([`bench/results/PERFORMANCE.md`](../bench/results/PERFORMANCE.md)).

### iceberg-rust's write support (Q7, answered 2026-10-11)

Append and read are solid and DuckDB-compatible on v2 and v3. Replace, row-delta, delete writers, branch commits,
manifest merging and file cleanup are missing and can't be added from outside the crate, so Timelake implements its
commit layer on iceberg-rust's public types (D13). Details: `spikes/iceberg-write/README.md`.

## Decisions

| ID | Decision | Status |
|---|---|---|
| D13 | Timelake owns its commit layer (append, replace, row-delta, overwrite, branch commits, rewrite-manifests) on iceberg-rust's public types. Forking and maintaining our own iceberg-rust is acceptable if needed. Upstreaming is welcome but low priority. Iceberg Java, iceberg-cpp, and iceberg-go serve as **reference implementations** to learn from (never to copy) and as **conformance oracles** in tests (Phase 0 finding, `spikes/iceberg-write`) | Accepted |

## Open questions and issues

- [#26](https://github.com/polythings/timelake/issues/26) Tests: crash at every step of a commit
- [#39](https://github.com/polythings/timelake/issues/39) Commit: specify how each pair of concurrent operations resolves
- [#47](https://github.com/polythings/timelake/issues/47) Commit: carry Timelake state forward on every commit, compaction included
- [#48](https://github.com/polythings/timelake/issues/48) Commit: batch-id idempotency ends when snapshots expire
- [#51](https://github.com/polythings/timelake/issues/51) Lake: a dropped and recreated table can take a commit from a stale writer

## History

- 2026-10-11: commit layer decided (D13, Phase 0)
- 2026-10-12: built in v0
- 2026-10-13: two-step commit (P4)
- 2026-10-19: moved from SPEC.md into spec 3, then into this spec when the specs were regrouped one per feature.
- 2026-10-20: uncertain outcomes: a failed or refused metadata write is read back before anything is deleted (#21,
  #165).
- 2026-10-20: the protocol's rules (summaries, last-value index, manifest plan, read-back, backoff) defined once in
  `timelake_format::commit` for the engine and the WASM writer; the WASM writer loads new metadata before it commits
  it, as the engine does.
