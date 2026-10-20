"""One benchmark run: a workload streamed into each system, then its queries on every reader, at each checkpoint.

The workload arrives in stages (by default one); a stage may end at a checkpoint, where every query runs (e.g. after
1 and 7 days of history, then ``ingested``). Then each system runs its maintenance, and the queries run again
(``maintained``). Optionally, fresh readers probe each system while it ingests live (``live.py``), and N dashboard
clients query it at once after the last checkpoint (``concurrency.py``). When the run includes the ``reference``
system, first, every answer is checked against its answer. The run writes ``results/runs/<run id>.jsonl`` as it goes.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import math
import os
import statistics
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np

from timebench import RUNS, meta
from timebench.concurrency import dashboards
from timebench.cost import phase_cost, query_cost, storage_month
from timebench.live import Prober
from timebench.results import (
    ConcurrencyRecord,
    EndRecord,
    IngestRecord,
    LiveRecord,
    QueryRecord,
    RunRecord,
    StageRecord,
    append_jsonl,
    best_time,
    first_time,
)
from timebench.s3 import PROXY, ProxyStats
from timebench.systems import System, systems
from timebench.systems import timelake as timelake_system
from timebench.targets import Place
from timebench.worker import command, request_kinds

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

    import pyarrow as pa

    from timebench.targets import Target
    from timebench.workloads import Batch, Query, Stage, Workload

SERVERS_ENV = "BENCH_SERVERS"
"""Path of ``infra/servers.sh``: when set, each database server runs only while its system does."""
REFERENCE = "reference"
"""The system whose answers the others' are checked against (``systems/reference.py``)."""


@dataclass(frozen=True)
class At:
    """A checkpoint, and the data's present there (epoch seconds) for queries with ``@AGO``."""

    checkpoint: str
    now: int | None


@dataclass
class RunSpec:
    """What one run covers."""

    workload: Workload
    target: Target
    systems: list[str]
    readers: list[str] | None = None
    """Only these readers (all of each system's by default)."""
    checkpoints: tuple[str, ...] | None = None
    """Only the query passes at these checkpoints (default: the workload's and ``maintained``; empty: none)."""
    queries: set[str] | None = None
    """Only these queries (by name)."""
    runs: int = 2
    """Repeat runs per query after the first run."""
    timeout: int = 600
    """Seconds per query process (all its runs)."""
    reference_timeout: int = 1800
    """``timeout`` for the reference system, whose answers the checks need."""
    skip_after_timeout: bool = True
    """A query that timed out on a system's reader is skipped at that system's later checkpoints (status
    ``skipped``), instead of waiting out the timeout again."""
    ttfb_ms: int | None = None
    """Latency the proxy adds per request (local object stores)."""
    mibps: int | None = None
    """Throughput cap per response in MiB/s the proxy applies, 0 for none (local object stores)."""
    label: str = ""
    reuse: bool = False
    """Query the tables an earlier run left instead of ingesting (``--checkpoints`` then picks the passes)."""
    live_interval: float = 0
    """Seconds between probe rounds while each system ingests; 0 = no probes."""
    concurrency: tuple[int, ...] = ()
    """Client counts for the concurrent-dashboards phase after the last checkpoint; empty = none."""
    concurrency_seconds: float = 30
    """How long each client count runs."""
    variant: str = ""
    """Names a kind of run of the same workload and target (e.g. ``bulk``), kept apart in reports and on the site."""
    tuning: str = "tuned"
    """Every system's configuration in this run (``systems.TUNINGS``, D23)."""
    pace: float = 0
    """At least this many seconds between arrivals (0 = as fast as the system commits), so the replay is closer to
    real time and probes have something to see."""
    _systems: dict[str, System] = field(default_factory=dict)

    def system(self, name: str) -> System:
        """The adapter of one system, created on first use."""
        if name not in self._systems:
            cls = systems()[name]
            place = Place(self.target, self.workload.dataset, name, self.tuning)
            self._systems[name] = cls(place, self.workload.namespace)
        return self._systems[name]

    def readers_of(self, name: str) -> list[str]:
        """The readers of a system in this run."""
        return [r for r in systems()[name].readers if self.readers is None or r in self.readers]

    def wants(self, checkpoint: str) -> bool:
        """Whether the queries run at a checkpoint."""
        return self.checkpoints is None or checkpoint in self.checkpoints

    def last_checkpoint(self) -> str | None:
        """The last checkpoint the queries run at, where the concurrency phase runs too."""
        every = [*self.workload.checkpoints(), "maintained"]
        return next((c for c in reversed(every) if self.wants(c)), None)


def run(spec: RunSpec) -> Path:
    """Runs everything and returns the run file."""
    if not spec.reuse:  # the tables exist; the data files may not
        spec.workload.prepare()
    if spec.target.s3 and spec.ttfb_ms is not None:
        set_proxy("ttfb", f"ms={spec.ttfb_ms}")
    if spec.target.s3 and spec.mibps is not None:
        set_proxy("mibps", f"v={spec.mibps}")
    started = dt.datetime.now(dt.timezone.utc)
    header = run_header(spec, started)
    path = RUNS / f"{header['id']}.jsonl"
    append_jsonl(path, header)
    print(f"run {header['id']} → {path}", flush=True)

    results: list[QueryRecord] = []
    now = spec.workload.stages()[-1].now  # the data's present once everything arrived
    if spec.reuse:
        for checkpoint in spec.checkpoints or (*spec.workload.checkpoints(), "maintained"):
            results += query_pass(spec, path, list(spec.systems), At(checkpoint, now))
        for s in spec.systems:
            run_dashboards(spec, path, s, now)
    else:
        # One system at a time, start to finish, so a database server has the machine to itself (``server``).
        for s in spec.systems:
            with server(spec.system(s), path) as up:
                if up:
                    results += run_system(spec, path, s, now, results)

    finished = dt.datetime.now(dt.timezone.utc)
    end: EndRecord = {
        "kind": "end",
        "finished": finished.isoformat(timespec="seconds"),
        "seconds": round((finished - started).total_seconds(), 1),
        "mismatches": mismatches(results),
    }
    if wrong := [
        f"{r['checkpoint']}/{r['query']}: {r['system']}+{r['reader']}" for r in results if r.get("correct") is False
    ]:
        end["wrong"] = wrong
    append_jsonl(path, end)
    for m in end["mismatches"]:
        print(f"ROW COUNT MISMATCH {m}", flush=True)
    return path


def run_system(spec: RunSpec, path: Path, name: str, now: int | None, earlier: list[QueryRecord]) -> list[QueryRecord]:
    """One system start to finish: ingest with its checkpoints, maintenance and its queries, concurrent dashboards."""
    results: list[QueryRecord] = []
    if not ingest_and_query(spec, path, name, results, earlier):
        return results
    if spec.wants("maintained"):
        if not record(path, maintain(spec.system(name))):
            return results
        results += query_pass(spec, path, [name], At("maintained", now), [*earlier, *results])
    run_dashboards(spec, path, name, now)
    return results


def run_dashboards(spec: RunSpec, path: Path, name: str, now: int | None) -> None:
    """Concurrent dashboards on one system at the last checkpoint, if the run asks for them."""
    last = spec.last_checkpoint()
    if spec.concurrency and spec.workload.dashboard_queries() and last and name != REFERENCE:
        reader = systems()[name].readers[0]
        queries = [
            {"name": q.name, "sql": q.sql_for(reader, now, spec.tuning)} for q in spec.workload.dashboard_queries()
        ]
        for clients in spec.concurrency:
            c = dashboards(spec.system(name), queries, last, clients, spec.concurrency_seconds)
            append_jsonl(path, c)
            print(concurrency_line(c), flush=True)


@contextlib.contextmanager
def server(system: System, path: Path) -> Generator[bool]:
    """Starts a database system's server before it runs and stops it after; yields whether the system can run.

    Only when ``BENCH_SERVERS`` names ``infra/servers.sh``. The server's versions are recorded once it's up.
    """
    script = os.environ.get(SERVERS_ENV)
    name = system.name
    managed = bool(script) and system.server and name != REFERENCE
    if managed:
        up = subprocess.run([str(script), "up", system.server_name()], capture_output=True, text=True, check=False)
        if up.returncode != 0:
            error = (up.stderr.strip().splitlines() or ["?"])[-1]
            record(path, {"kind": "ingest", "system": name, "status": "error", "error": f"server: {error}"[:300]})
            yield False
            return
        append_jsonl(path, {"kind": "versions", "system": name, "versions": system.versions()})
    try:
        yield True
    finally:
        if managed:
            subprocess.run([str(script), "down", system.server_name()], capture_output=True, check=False)


def run_header(spec: RunSpec, started: dt.datetime) -> RunRecord:
    """The first record of a run: what runs, where, and on which commit."""
    workload, target = spec.workload, spec.target
    versions: dict[str, str] = {}
    for name in spec.systems:
        if not (os.environ.get(SERVERS_ENV) and systems()[name].server and name != REFERENCE):  # recorded once up
            versions.update(spec.system(name).versions())
    header: RunRecord = {
        "kind": "run",
        "id": f"{started:%Y%m%dT%H%M%SZ}-{workload.dataset}-{target.name}"
        + (f"-{spec.variant}" if spec.variant else "")
        + ("" if spec.tuning == "tuned" else f"-{spec.tuning}"),
        "workload": workload.name,
        "scale": workload.scale,
        "params": {**workload.params(), "span_days": workload.span_days},
        "target": target.name,
        "storage": target.storage,
        "machine": meta.machine(),
        "commit": meta.commit(),
        "dirty": meta.dirty(),
        "label": spec.label,
        "started": started.isoformat(timespec="seconds"),
        "readers": {s: spec.readers_of(s) for s in spec.systems},
        "versions": versions,
        "runs": spec.runs,
        "tuning": spec.tuning,
    }
    if target.s3 and spec.ttfb_ms is not None:
        header["ttfb_ms"] = spec.ttfb_ms
    if target.s3 and spec.mibps is not None:
        header["mibps"] = spec.mibps
    if spec.live_interval:
        header["live_interval_s"] = spec.live_interval
    if spec.pace:
        header["pace_s"] = spec.pace
    if spec.variant:
        header["variant"] = spec.variant
    # What an A/B arm changed: the reader's session settings and storage options, if set.
    if settings := timelake_system.reader_settings():
        header["reader_settings"] = settings
    if options := timelake_system.reader_options():
        header["reader_options"] = options
    if spec.concurrency:
        header.update(concurrency=list(spec.concurrency), concurrency_seconds=spec.concurrency_seconds)
    return header


def ingest_and_query(
    spec: RunSpec, path: Path, name: str, results: list[QueryRecord], earlier: list[QueryRecord]
) -> bool:
    """Streams the workload into one system, querying it at each stage's checkpoint; whether it succeeded.

    ``earlier`` holds the systems that ran before, the reference's answers among them.
    """
    system = spec.system(name)

    def checkpoint(stage: Stage) -> None:
        if stage.checkpoint and spec.wants(stage.checkpoint):
            results.extend(query_pass(spec, path, [name], At(stage.checkpoint, stage.now), [*earlier, *results]))

    done, probes = ingest(spec, system, checkpoint)
    ok = record(path, done)
    if probes:
        append_jsonl(path, probes)
        print(live_line(probes), flush=True)
    return ok


def query_pass(
    spec: RunSpec, path: Path, systems_: list[str], at: At, earlier: list[QueryRecord] | None = None
) -> list[QueryRecord]:
    """Every query on every reader of the systems, each in a fresh process.

    Answers are checked against the reference system's at the same checkpoint, when it ran first.
    """
    results: list[QueryRecord] = []
    for s in systems_:
        for reader in spec.readers_of(s):
            for query in spec.workload.queries():
                if spec.queries and query.name not in spec.queries:
                    continue
                timed_out = next(
                    (
                        q["checkpoint"]
                        for q in [*(earlier or []), *results]
                        if (q["system"], q["reader"], q["query"], q["status"]) == (s, reader, query.name, "timeout")
                    ),
                    None,
                )
                if spec.skip_after_timeout and timed_out is not None:
                    r = query_record(s, reader, query, at)
                    r.update(status="skipped", error=f"timed out at {timed_out}")
                else:
                    r = measure(spec, spec.system(s), reader, query, at)
                expected = reference_answer([*(earlier or []), *results], at.checkpoint, query.name)
                if expected is not None and s != REFERENCE and r["status"] == "ok":
                    correct = same_answer(r, expected)
                    if correct is not None:
                        r["correct"] = correct
                append_jsonl(path, r)
                results.append(r)
                print(summary_line(r), flush=True)
    return results


def reference_answer(results: list[QueryRecord], checkpoint: str, query: str) -> QueryRecord | None:
    """The reference system's answer to a query at a checkpoint, if it has one."""
    return next(
        (
            r
            for r in results
            if r["system"] == REFERENCE
            and r["checkpoint"] == checkpoint
            and r["query"] == query
            and r["status"] == "ok"
        ),
        None,
    )


def same_answer(r: QueryRecord, expected: QueryRecord) -> bool | None:
    """Whether a result matches the reference: the same row count and, per column, the same sum (within 1e-6).

    None when a reader doesn't report its sums.
    """
    if r.get("rows") != expected.get("rows"):
        return False
    if not r.get("rows"):
        return True
    got, want = r.get("sums"), expected.get("sums")
    if got is None or want is None:
        return None
    if len(got) != len(want):
        return False
    return all(
        (a is None and b is None)
        or (a is not None and b is not None and math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-6))
        for a, b in zip(got, want, strict=True)
    )


def record(path: Path, r: IngestRecord) -> bool:
    """Appends an ingest or maintenance record; whether the system is still in the run."""
    append_jsonl(path, r)
    shown = {k: v for k, v in r.items() if k not in ("kind", "kinds")}
    print(f"{r['kind']:8} {shown}", flush=True)
    return r["status"] == "ok"


def counted(system: System) -> bool:
    """Whether the system's S3 traffic goes through the counting proxy."""
    return system.target.s3 and not system.server


def ingest(
    spec: RunSpec, system: System, checkpoint: Callable[[Stage], None]
) -> tuple[IngestRecord, LiveRecord | None]:
    """Creates the tables, loads the initial rows, then commits every arrival of the stream, stage by stage.

    After each stage, ``checkpoint`` runs its queries (not counted as ingest time). With ``live_interval`` set,
    fresh readers probe the tables during the live stage.
    """
    out: IngestRecord = {"kind": "ingest", "system": system.name}
    workload = spec.workload
    writer = Writer(spec, system)
    before = PROXY.stats() if counted(system) else None
    stages: dict[str, StageRecord] = {}
    try:
        system.reset()  # not timed: its cost depends on what the last run left
        start = time.perf_counter()
        for t in workload.tables:
            system.create(t, workload.schema(t.name))
        if system.tuned:
            system.tune(workload.tuning_statements(system.name))
        for table, data in workload.initial():
            system.append(table, data)
        setup = time.perf_counter() - start
        for stage in workload.stages():
            done = writer.stage(stage)
            system.place.save_state({**system.state(), "phase": stage.checkpoint or stage.name, "ingested_at": now()})
            stats = system.stats()
            done.update(
                objects=stats["objects"],
                bytes=stats["bytes"],
                data_files=stats["data_files"],
                data_bytes=stats["data_bytes"],
            )
            stages[stage.name] = done
            checkpoint(stage)
        latencies = writer.latencies
        out.update(
            status="ok",
            commits=len(latencies),
            rows=sum(s["rows"] for s in stages.values()),
            seconds=round(setup + sum(s["seconds"] for s in stages.values()), 2),
            commit_seconds=round(sum(latencies), 2),
            commit_ms_p50=round(statistics.median(latencies) * 1000, 1),
            commit_ms_p95=round(float(np.percentile(latencies, 95)) * 1000, 1),
            commit_ms_max=round(max(latencies) * 1000, 1),
        )
        if len(stages) > 1:
            out["stages"] = stages
        out.update(storage(system))
        out.update(system.ingest_extras())
    except Exception as e:  # noqa: BLE001 (a failing system is a result; the run goes on without it)
        out.update(status="error", error=f"{type(e).__name__}: {e}"[:300], commits=len(writer.latencies))
    probes = writer.finish_probes()
    if probes:
        out["probed"] = True
    return priced({**out, **proxy_delta(before)}, system), probes


class Writer:
    """Commits a system's stages: every arrival's batches, timed, and probes while the live stage arrives."""

    def __init__(self, spec: RunSpec, system: System) -> None:
        self.spec = spec
        self.system = system
        self.latencies: list[float] = []
        """Seconds per commit."""
        self.prober: Prober | None = None
        self.probes: LiveRecord | None = None

    def stage(self, stage: Stage) -> StageRecord:
        """Commits a stage's arrivals (paced and probed if it's live), then waits until readers can see them all."""
        start = time.perf_counter()
        rows, commits = 0, len(self.latencies)
        for i, arrival in enumerate(stage.arrivals):
            arrived = time.perf_counter()
            rows += self.commit(arrival)
            if i == 0 and stage.live:
                self.start_probes()
            if i % 100 == 0:
                print(f"  {self.system.name}: {stage.name} arrival {i}, {time.perf_counter() - start:.1f}s", flush=True)
            if self.spec.pace and stage.live:
                time.sleep(max(0.0, arrived + self.spec.pace - time.perf_counter()))
        settle = self.system.settle()  # rows a reader can't see yet haven't finished arriving
        seconds = time.perf_counter() - start
        if self.prober and self.probes is None:
            self.probes = self.prober.finish()  # before the checkpoint's queries, which would compete with them
        done: StageRecord = {"rows": rows, "commits": len(self.latencies) - commits, "seconds": round(seconds, 2)}
        if latencies := self.latencies[commits:]:
            done["commit_ms_p50"] = round(statistics.median(latencies) * 1000, 1)
        if settle:
            done["settle_s"] = round(settle, 2)
        return done

    def commit(self, arrival: list[Batch]) -> int:
        """Appends one arrival, a commit per table; the rows it held."""
        workload, rows = self.spec.workload, 0
        for table, data in arrival:
            t = time.perf_counter()
            self.system.append(table, data)
            self.latencies.append(time.perf_counter() - t)
            rows += data.num_rows
            if self.prober and table == workload.live_table:
                self.prober.committed(newest_second(data, workload.spec(table).time_column))
        return rows

    def start_probes(self) -> None:
        """Starts probing, once the tables exist (after the live stage's first arrival), if the run probes."""
        spec, system = self.spec, self.system
        if self.prober or not (spec.live_interval and spec.workload.live_table) or system.name == REFERENCE:
            return
        system.place.save_state({**system.state(), "phase": "ingesting"})  # readers can find the tables now
        self.prober = Prober(spec.workload, system, spec.live_interval)
        self.prober.start()

    def finish_probes(self) -> LiveRecord | None:
        """What the probes saw (None if there were none); stops them if a failure left them running."""
        if self.prober and self.probes is None and self.prober.thread.is_alive():
            self.probes = self.prober.finish()
        return self.probes


def newest_second(data: pa.Table, time_column: str | None) -> int:
    """The newest timestamp in a batch, in whole epoch seconds."""
    if not time_column or data.num_rows == 0:
        return 0
    newest = np.max(data[time_column].to_numpy())  # datetime64, in the column's unit
    return int(newest.astype("datetime64[s]").astype(np.int64))


def maintain(system: System) -> IngestRecord:
    """Runs the system's maintenance on what ingest left."""
    out: IngestRecord = {"kind": "maintain", "system": system.name}
    before = PROXY.stats() if counted(system) else None
    start = time.perf_counter()
    try:
        system.maintain()
        seconds = time.perf_counter() - start
        state = {**system.place.load_state(), **system.state(), "phase": "maintained"}
        system.place.save_state(state)
        out.update(status="ok", seconds=round(seconds, 2))
        out.update(storage(system))
        out.update(system.maintain_extras())
    except Exception as e:  # noqa: BLE001 (as in ingest)
        out.update(status="error", error=f"{type(e).__name__}: {e}"[:300])
    return priced({**out, **proxy_delta(before)}, system)


def storage(system: System) -> IngestRecord:
    """The objects and bytes the system's tables take now."""
    stats = system.stats()
    return {
        "objects": stats["objects"],
        "bytes": stats["bytes"],
        "data_files": stats["data_files"],
        "data_bytes": stats["data_bytes"],
    }


def priced(r: IngestRecord, system: System) -> IngestRecord:
    """Adds the phase's request cost and the storage's monthly cost at the target's list prices, where priced."""
    target = system.target.name
    if (usd := phase_cost(r, target)) is not None:
        r["usd"] = usd
    if (
        "bytes" in r
        and system.s3_storage_prices
        and not system.server
        and (month := storage_month(r["bytes"], target)) is not None
    ):
        r["storage_usd_month"] = month
    return r


def proxy_delta(before: ProxyStats | None) -> IngestRecord:
    """S3 requests since ``before`` (nothing if the traffic isn't counted)."""
    if before is None:
        return {}
    after = PROXY.stats()
    out: IngestRecord = {"requests": after["requests"] - before["requests"]}
    if kinds := request_kinds(before, after):
        out["kinds"] = kinds
    return out


def query_record(system: str, reader: str, query: Query, at: At) -> QueryRecord:
    """A query record's identifying fields."""
    return {
        "kind": "query",
        "system": system,
        "reader": reader,
        "checkpoint": at.checkpoint,
        "suite": query.suite,
        "query": query.name,
    }


def measure(spec: RunSpec, system: System, reader: str, query: Query, at: At) -> QueryRecord:
    """Runs one query in a fresh process: its record, with ``status`` ok, error or timeout."""
    out = query_record(system.name, reader, query, at)
    sql = query.sql_for(reader, at.now, system.tuning)
    place = system.place
    if reader in system.commands:
        cmd = type(system).reader_command(reader, place, place.load_state(), spec.runs)
    else:
        job = {
            "system": system.name,
            "reader": reader,
            "target": place.target.name,
            "dataset": place.dataset,
            "tuning": place.tuning,
        }
        cmd = command({**job, "runs": spec.runs})
    before = PROXY.stats() if counted(system) else None
    try:
        # A failing query is a result to record, not a reason to stop: no check=True.
        timeout = spec.reference_timeout if system.name == REFERENCE else spec.timeout
        proc = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        return {**out, "status": "timeout"}
    if proc.returncode != 0:
        lines = proc.stderr.strip().splitlines() or ["?"]
        error = next((line for line in reversed(lines) if "Error" in line or "Exception" in line), lines[-1])
        return {**out, "status": "error", "error": error[:300]}
    measured: QueryRecord = json.loads(proc.stdout.strip().splitlines()[-1])
    out.update(measured, status="ok")
    # Requests by kind over all runs, for readers that don't report them per run (the Timelake CLI).
    if before and "kinds" not in out and (total := request_kinds(before, PROXY.stats())):
        out["kinds_total"] = total
    if (usd := query_cost(out, spec.target.name)) is not None:
        out["usd"] = usd
    return out


def mismatches(results: list[QueryRecord]) -> list[str]:
    """Queries whose row counts differ between readers at the same checkpoint."""
    rows: dict[tuple[str, str], dict[str, int | None]] = {}
    for r in results:
        if r["status"] == "ok":
            rows.setdefault((r["checkpoint"], r["query"]), {})[f"{r['system']}+{r['reader']}"] = r["rows"]
    return [
        f"{checkpoint}/{query}: " + ", ".join(f"{k}={v}" for k, v in counts.items())
        for (checkpoint, query), counts in rows.items()
        if len(set(counts.values())) > 1
    ]


def summary_line(r: QueryRecord) -> str:
    """One line per query for the console."""
    prefix = f"{r['system']}+{r['reader']:11} {r['checkpoint']:10} {r['query']:36}"
    if r["status"] != "ok":
        return f"{prefix} {r['status'].upper()}: {r.get('error', '')[:120]}"
    reqs = f"  reqs {r['requests'][0]}/{min(r['requests'])}" if "requests" in r else ""
    return f"{prefix} first {first_time(r):8.3f}s  best {best_time(r):8.3f}s  rows {r['rows']}{reqs}"


def live_line(r: LiveRecord) -> str:
    """The probes of one system, for the console."""
    lags = r["lag_s"]
    stale = sum(1 for x in lags if x > 0)
    return (
        f"live     {r['system']}: {r['rounds']} rounds, {len(lags)} freshness probes, {stale} behind "
        f"(max {max(lags, default=0):.1f}s), {len(r['errors'])} errors"
    )


def concurrency_line(r: ConcurrencyRecord) -> str:
    """One client count of one system, for the console."""
    p50, p95 = r.get("p50_s"), r.get("p95_s")
    times = f"p50 {p50:.3f}s p95 {p95:.3f}s" if p50 is not None and p95 is not None else "no queries"
    return f"clients  {r['system']}+{r['reader']} x{r['clients']}: {r['qps']} q/s, {times}, {r['errors']} errors"


def set_proxy(setting: str, query: str) -> None:
    """Sets the counting proxy's ``ttfb`` (``ms=20``) or ``mibps`` (``v=50``)."""
    with urllib.request.urlopen(f"http://{PROXY.admin}/{setting}?{query}", data=b"", timeout=5):  # a POST
        pass


def now() -> str:
    """The current UTC time, ISO 8601."""
    return dt.datetime.now(dt.timezone.utc).isoformat()
