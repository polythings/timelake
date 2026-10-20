"""Result records (one JSON object per line) and the statistics the reports use.

A run of ``timebench`` writes ``results/runs/<run id>.jsonl``: a ``run`` record first, then one ``ingest`` and
``maintain`` record per system and one ``query`` record per query and reader, and an ``end`` record last. The older
``results/<suite>.<tag>.jsonl`` files hold the Phase 0 and v0 runs, one query record per line without a ``kind``.
"""

from __future__ import annotations

import datetime as dt
import json
import statistics
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path


class RunRecord(TypedDict, total=False):
    """The first line of a run file: what ran, where, and on which commit."""

    kind: Literal["run"]
    id: str
    workload: str
    scale: str
    params: dict[str, object]
    """Workload settings beyond the scale (e.g. ``window_minutes``)."""
    target: str
    """Where the lake systems keep their tables: ``local``, ``s3`` (RustFS), ``aws-s3``, ``aws-s3x``, ..."""
    storage: str
    """``local`` or ``s3``: how the target is reached (S3 goes through ``tools/s3proxy``)."""
    ttfb_ms: int
    """Latency the proxy adds per request (local object stores only)."""
    mibps: int
    """Throughput cap per response in MiB/s the proxy applied (local object stores only)."""
    machine: str
    commit: str
    dirty: bool
    """Uncommitted changes outside ``bench/results``: the run isn't exactly ``commit``."""
    label: str
    """``weekly`` for the regression runs; free text otherwise."""
    variant: str
    """A kind of run of the same workload and target (``bulk``: loaded in large commits); absent for the default."""
    reader_settings: dict[str, str]
    """Session settings of Timelake's reader (``TIMEBENCH_TIMELAKE_SETTINGS``), for comparing them; absent if unset."""
    reader_options: dict[str, str]
    """Storage options of Timelake's readers (``TIMEBENCH_TIMELAKE_OPTIONS``); absent if unset."""
    started: str
    readers: dict[str, list[str]]
    """The systems of the run -> their readers in it."""
    versions: dict[str, str]
    """Package -> version, for Timelake and every system and reader."""
    runs: int
    """Repeat runs per query after the first run."""
    tuning: str
    """``tuned`` or ``default`` (D23); absent in runs before 2026-10-20, which count as tuned."""
    live_interval_s: float
    """Seconds between probe rounds during ingest (absent: no probes)."""
    pace_s: float
    """At least this many seconds between arrivals (absent: as fast as each system commits)."""
    concurrency: list[int]
    """Client counts of the concurrency phase (absent: none)."""
    concurrency_seconds: float
    imported: str
    """For runs converted from the older result files: where they came from."""


class EndRecord(TypedDict, total=False):
    """The last line of a complete run file."""

    kind: Literal["end"]
    finished: str
    seconds: float
    mismatches: list[str]
    """Queries whose row counts differ between readers (should be none)."""
    wrong: list[str]
    """Answers that differ from the reference system's (``checkpoint/query: system+reader``)."""


class QueryRecord(TypedDict, total=False):
    """One query on one reader: a line of a run file (``kind`` = ``query``), or of an older result file."""

    kind: Literal["query"]
    system: str
    reader: str
    checkpoint: str
    """The tables' state when the query ran: ``ingested``, ``maintained``, or a workload's own (``1d``, ``7d``)."""
    suite: str
    engine: str
    version: str
    machine: str
    storage: str
    layout: str
    ts: str
    query: str
    direct: bool
    status: str
    """``ok``, ``error``, ``timeout``, or ``skipped`` (it timed out at an earlier checkpoint)."""
    error: str
    times: list[float]
    """Seconds per run: the first run, then the repeats."""
    rows: int | None
    setup_s: float
    requests: list[int]
    """S3 requests per run (through the counting proxy)."""
    bytes: list[int]
    cache_hits: list[int]
    """Timelake's block cache: blocks read from memory or disk, per run (when the cache is on)."""
    cache_misses: list[int]
    """Blocks the cache fetched from the store, per run."""
    ttfb_ms: int
    kinds: list[dict[str, int]]
    """S3 requests by kind, per run."""
    kinds_total: dict[str, int]
    """S3 requests by kind over all runs, for engines that don't report them per run."""
    rtt_ms: float
    server_ms: list[float]
    value: float
    """The first value of the result, for probes (the newest timestamp a reader sees, in epoch seconds)."""
    sums: list[float | None]
    """Per result column, the sum of its values (timestamps in epoch seconds; None for text): the answer's check."""
    correct: bool
    """Whether the answer matches the reference system's (absent when the run has no reference)."""
    usd: float
    """The first run's requests at the target's list prices (priced targets only, timebench/cost.py)."""


class LiveRecord(TypedDict, total=False):
    """Probes while one system ingests (``kind`` = ``live``).

    Fresh readers run the dashboard queries, and a freshness probe tells how far behind the newest commit they see.
    """

    kind: Literal["live"]
    system: str
    reader: str
    interval_s: float
    rounds: int
    """Probe rounds: each runs the freshness probe and every dashboard query once, each in a fresh process."""
    lag_s: list[float]
    """Per freshness probe: seconds since the oldest commit the reader didn't see had finished (0 = up to date)."""
    queries: dict[str, list[float]]
    """Dashboard query -> first-run seconds of each probe."""
    errors: list[str]


class ConcurrencyRecord(TypedDict, total=False):
    """N dashboard clients at once (``kind`` = ``concurrency``).

    Each client keeps a reader open and runs the dashboard queries in turn for a fixed time.
    """

    kind: Literal["concurrency"]
    system: str
    reader: str
    checkpoint: str
    clients: int
    seconds: float
    queries: int
    """Queries all clients finished within ``seconds`` (``qps`` is these per second)."""
    qps: float
    p50_s: float
    p95_s: float
    p99_s: float
    errors: int
    error: str
    """The first error, if any."""


class StageRecord(TypedDict, total=False):
    """One stage of a system's ingest (``IngestRecord.stages``): its rows and time, and the storage after it."""

    rows: int
    commits: int
    seconds: float
    commit_ms_p50: float
    settle_s: float
    """Of ``seconds``, the wait until readers saw every commit (servers that apply writes asynchronously)."""
    objects: int | None
    bytes: int
    data_files: int
    data_bytes: int


class IngestRecord(TypedDict, total=False):
    """One system's ingest or maintenance: a line of a run file (``kind``), or of ``results/v0_ingest*.jsonl``."""

    kind: Literal["ingest", "maintain"]
    system: str
    storage: str
    phase: str
    """``ingest`` or ``maintain``."""
    window_minutes: int
    commits: int
    rows: int
    seconds: float
    """Wall-clock time of the phase (with pacing, mostly waiting between arrivals)."""
    commit_seconds: float
    """Time spent committing: the sum of all commits."""
    commit_ms_p50: float
    commit_ms_p95: float
    commit_ms_max: float
    objects: int | None
    bytes: int
    data_files: int
    data_bytes: int
    inline_compactions: int
    compaction_after_ingest_s: dict[str, int]
    target: str
    """The storage target when it differs from ``storage`` (e.g. ``s3x``, ``aws-s3``)."""
    machine: str
    requests: int
    """S3 requests during the phase (through the counting proxy)."""
    kinds: dict[str, int]
    """Those requests by kind, where the proxy reports them (AWS mode)."""
    status: str
    """``ok`` or ``error``; a failed system is recorded and the run goes on without it."""
    error: str
    usd: float
    """The phase's requests (and uploads) at the target's list prices (priced targets only)."""
    storage_usd_month: float
    """What keeping ``bytes`` costs a month at the target's list prices."""
    probed: bool
    """Fresh readers queried the tables during ingest (a ``live`` record follows), so ingest shared the machine."""
    stages: dict[str, StageRecord]
    """Per stage of the stream, when it has more than one (e.g. bulk history, then a live day)."""


ENGINE_ORDER = (
    "duckdb",
    "duckdb-iceberg",
    "duckdb-vortex",
    "datafusion",
    "datafusion-rs",
    "datafusion-rs-vortex",
    "polars",
    "v0-iceberg",
    "v0-s3tables",
    "v0-lance-duckdb",
    "v0-lance-df",
    "v0-timelake",
    "v0-timelake-duckdb",
)
"""The order engines appear in in reports; others go last."""


def order_engines(engines: Iterable[str]) -> list[str]:
    """Sorts engines into report order."""
    return sorted(engines, key=lambda e: ENGINE_ORDER.index(e) if e in ENGINE_ORDER else len(ENGINE_ORDER))


def utc_timestamp() -> str:
    """The current time for a record's ``ts``, e.g. ``2026-10-13T09:30:00+00:00``."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """All records of a JSONL file; none if it doesn't exist."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def append_jsonl(path: Path, record: Mapping[str, object]) -> None:
    """Appends one record, creating the file (and its directory) if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def load_ok(path: Path) -> dict[str, dict[str, QueryRecord]]:
    """Successful query records by engine, then query; a later record of the same query wins."""
    out: dict[str, dict[str, QueryRecord]] = {}
    for r in read_jsonl(path):
        if r["status"] == "ok":
            out.setdefault(r["engine"], {})[r["query"]] = cast("QueryRecord", r)
    return out


def geomean(xs: Iterable[float]) -> float:
    """Geometric mean, NaN if empty. Times under 0.1 ms count as 0.1 ms so one tiny value can't dominate."""
    clamped = [max(x, 1e-4) for x in xs]
    return statistics.geometric_mean(clamped) if clamped else float("nan")


def first_time(r: QueryRecord) -> float:
    """Seconds of the first run."""
    return r["times"][0]


def best_time(r: QueryRecord) -> float:
    """Seconds of the fastest repeat run (the first run if there were no repeats)."""
    return min(r["times"][1:] or r["times"])
