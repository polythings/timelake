---
number: 9
title: Settings
area: Tables and storage
status: done
updated: 2026-10-19
versions: [0.5.0]
related: [4, 21]
---

# 9. Settings

**Done** · updated 2026-10-19 · versions 0.5.0 · milestone: [Installable, upgradable, switchable](milestones/upgrades-and-settings.md)
· related: [spec 4](0004-table-contract.md), [spec 21](0021-reader.md)

## Summary

Every engine feature has a setting that turns it on or off or picks its behaviour, at the table, session or call level, declared once in a registry that drives validation, `SHOW`, the CLI and the docs.

## Design

**Principle.** Every engine feature has a setting that turns it on or off or picks its behaviour, a bit like Postgres's configuration variables. During fast development this keeps trade-offs explicit, lets benchmarks measure each feature on and off, and records how behaviour changed.

**One registry.** Every setting is declared once (in `timelake-format`): name, type, default, the levels it may be set at, a description, and the version that added it. From it come the docs page, `SHOW timelake.*` in SQL, `timelake settings` on the CLI, and validation: an unknown name or a bad value is an error, not ignored.

**Levels, the more specific winning:**

| Level | How it's set | Lifetime |
|---|---|---|
| Built-in default | the registry | — |
| Table | table property `timelake.<name>`, at creation or later (`timelake alter`, `Table.set_properties`) | stored in the table, applies to every reader and writer |
| Session | SQL `SET timelake.<name> = …`; `Session` / `Lake` options in Rust and Python; `--set <name>=<value>` on the CLI | one connection or process |
| Call | `AppendOptions`, `MaintainOptions`, … | one call |

A setting declares which levels it allows: the time column and series key only at creation; read settings at every level; write settings at table and call level.

**First settings:**

| Setting | Values (default) | Levels | Meaning |
|---|---|---|---|
| `read-dedup` | on, off (on when `dedup` is on) | table, session | Remove copies when the Timelake reader reads ([spec 12](0012-late-data-and-dedup.md)) |
| `read-dedup-strategy` | merge, keys-first (decided by benchmark) | table, session | How copies are removed on read ([spec 12](0012-late-data-and-dedup.md)) |
| `append-dedup` | on, off (on when `dedup` is on) | table, call | Keep only the last of equal keys within one append (not built, see below) |
| `ingest-time-column` | a column name, or none (`_ingest_ts`) | creation | Insertion time per row ([spec 12](0012-late-data-and-dedup.md)), built |
| `latest-by` | a column (`_ingest_ts`) | table | Which copy of a row is the latest write ([spec 10](0010-supplemental-timestamps.md), proposed) |
| `as_of_inserted` | a timestamp, or none | session | Leave out rows inserted after it, before dedup ([spec 10](0010-supplemental-timestamps.md), proposed) |
| `compact-cold-after` | a duration, or off (1h) | table, call | Full compaction of partitions whose time range has ended ([spec 12](0012-late-data-and-dedup.md)) |
| `version-check-interval` | a duration (1s) | table, session | How long the reader uses a table version without checking for a newer one ([spec 7](0007-table-head.md)), built |
| `data-page-rows` | a count (20,000; 8,192 when written from WebAssembly) | table | Rows per Parquet data page ([spec 13](0013-wasm-writes.md)), built |

The existing `timelake.*` properties become registry entries unchanged. Hard-coded behaviour moves into settings as it's touched: compaction tiers, the small-file ratio, scan parallelism. The planning cache's lifetime became `version-check-interval`.

**Decided 2026-10-16:**
- **Names in SQL** use underscores (`SET timelake.read_dedup = off`); every spelling (dashes, underscores, with or without `timelake.`) names the same setting.
- **No locked settings:** a session override is the reader's choice.
- **No environment variables** until something needs them.

**Built 2026-10-16:**
- The registry is `timelake_format::settings`: name, kind, default, levels, the version that added it, description, and a value check. A test keeps it in step with the contract's properties and defaults.
- Table level: properties are checked at creation and by `Table::set_properties` (CLI `timelake alter --set/--unset`, Python `Table.set_properties`). Unknown `timelake.*` names, bad values and settings not allowed at the table level are errors; properties a newer Timelake wrote are ignored when reading.
- Session level: every setting the registry allows there can be `SET`, shown with `SHOW`, passed with `--set`, or given to Python's `Lake(settings=…)`. The CLI's `--set` no longer panics on a bad key.
- `timelake settings [table] [--json]` lists them; the docs page "Settings" renders the same list (`dashboard/lib/settings.json`, checked by a test).
- Changes from the proposal: `dedup` is fixed at creation (files written with it off may hold copies, which dedup on read assumes they don't). `append-dedup` isn't built for the same reason: an append that keeps copies would need its files marked for the reader. `format-version` is managed by Timelake (create, upgrade) and can't be set by hand. `latest-by` and `as_of_inserted` wait for [spec 10](0010-supplemental-timestamps.md). Storage options (`timelake_freshness_check`, …) stay lake options, documented on the Storage page.

## Open questions and issues

None open.

## History

- 2026-10-16: built (SPEC.md §21.9)
- 2026-10-19: 35 settings
- 2026-10-19: moved from SPEC.md into its own spec.
- 2026-10-19: was spec 25; specs regrouped one per feature, with milestones on the roadmap.
