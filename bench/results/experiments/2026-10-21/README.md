# Experiments of 2026-10-21 (#184)

DataFusion 55.2 and Arrow 59 (through `polythings-iceberg`) against DataFusion 54.1 and Arrow 58. The numbers and
the decision are in [`../../PERFORMANCE.md`](../../PERFORMANCE.md). M1 Pro laptop, release builds, local disk, under
other load (load average 8–20), so absolute times are rough; the arms ran back to back.

## Timebench runs

| Experiment | Workload, target | Arms (label: what differs) | Runs | Result | Decision |
|---|---|---|---:|---|---|
| DataFusion 55 upgrade | TSBS IoT small; local; reference checking answers | `DataFusion 55 A/B: main (DataFusion 54.1, Arrow 58)` (`20261021T231702Z`, commit `61ea950`); `DataFusion 55 A/B: DataFusion 55.2, Arrow 59` (`20261021T232334Z`, `82a0a2f`); `…, 10 MiB file splits` (`20261021T233147Z`), with Timelake's file-split threshold | 3 | Every answer right in all three. The upgrade alone: after maintenance breakdown-frequency 10.8 → 15.7 ms, the rest within noise. With 10 MiB file splits: geomean best repeat after maintenance 4.21 → 4.03 ms, right after ingest 5.24 → 5.34 ms, no query more than 25% apart; ingest 9.45 → 9.44 s | upgrade, with the 10 MiB threshold (DataFusion 55 upgrade) |

The second run is kept as measured: it is the upgrade without the threshold, the regression the third run fixes.
