# Experiments of 2026-10-20 (D23, #149)

Tuned and default Timelake (D23) on the industrial IoT workload, and rollup routing through filters from another
table. The numbers and decisions are in [`../../PERFORMANCE.md`](../../PERFORMANCE.md). M1 Pro laptop, release builds,
local disk, under other load (load average 20–30), so absolute times are rough; both arms ran back to back.

| Experiment | Data | Arms | Result | Decision (section of PERFORMANCE.md) |
|---|---|---|---|---|
| Tuned IIoT SQL for Timelake | IIoT small, an hourly rollup per tag, CLI best of 3 | the shared SQL, Timelake's tuned version | daily-stats 551 → 37 ms, gaps 249 → 38, latest-area 91 → 21, time-above-limit 360 → 193; same answers | tuned versions kept (D23: tuned Timelake on IIoT) |
| Routing through `IN (SELECT …)` | IIoT small daily statistics of one line's tags | a literal tag list, the asset model's subquery before and after | 54 ms, 515 ms, 37 ms | built (same section) |

## Timebench runs

| Experiment | Workload, target | Arms (label: what differs) | Runs | Result | Decision |
|---|---|---|---:|---|---|
| Timelake tuned and default | IIoT small; local; reference checking answers | `tuning A/B tuned`: the hourly rollup and tuned queries; `tuning A/B default`: neither | 2 | After maintenance, geomean best repeat 43 → 16 ms; ingest 450 → 456 s, commit p50 132 ms both; dashboards 61 → 187 queries/s at 16 clients | rollup and tuned queries are Timelake's tuned IIoT configuration |
