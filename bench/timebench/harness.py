"""One benchmark run: a workload streamed into each system, then its queries on every reader, at each checkpoint.

The workload arrives in stages; a stage may end at a checkpoint, where every query runs. Then each system runs its
maintenance and the queries run again (``maintained``). Optionally, fresh readers probe each system while it ingests
(``live.py``), and dashboard clients query it at once after the last checkpoint (``concurrency.py``). The run writes
``results/runs/<run id>.jsonl`` as it goes.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import itertools
import json
import math
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
    MAINTAINED,
    ConcurrencyRecord,
    EndRecord,
    IngestRecord,
    LiveRecord,
    QueryRecord,
    RunRecord,
    StageRecord,
    SystemMeta,
    append_jsonl,
    best_time,
    first_time,
)
from timebench.systems import System, systems
from timebench.systems.reference import Reference
from timebench.targets import Place, Tuning
from timebench.worker import command, failure, job_for, request_kinds

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

    import pyarrow as pa

    from timebench.config import BenchConfig
    from timebench.s3 import ProxyStats
    from timebench.targets import StorageStats, Target
    from timebench.workloads import Batch, Query, Stage, Workload

REFERENCE = Reference.name
"""The system whose answers the others' are checked against (``systems/reference.py``)."""


@dataclass(frozen=True)
class At:
    """A checkpoint, and the data's present there (epoch seconds) for queries with ``@AGO``."""

    checkpoint: str
    now: int | None


@dataclass
class RunSpec:
    """What one run covers; ``timebench run`` and profiles take their defaults from here."""

    workload: Workload
    target: Target
    systems: list[str]
    config: BenchConfig = field(kw_only=True)
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
    ttfb_ms: int = 0
    """Latency the proxy adds per request (local object stores)."""
    mibps: int = 0
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
    tuning: Tuning = Tuning.TUNED
    """Every system's configuration in this run (D23)."""
    pace: float = 0
    """At least this many seconds between arrivals (0 = as fast as the system commits), so the replay is closer to
    real time and probes have something to see."""
    _systems: dict[str, System] = field(default_factory=dict)

    def system(self, name: str) -> System:
        """The adapter of one system, created on first use."""
        if name not in self._systems:
            cls = systems()[name]
            place = Place(self.target, self.workload.dataset, name, self.tuning, config=self.config)
            self._systems[name] = cls(place, self.workload.namespace)
        return self._systems[name]

    def readers_of(self, name: str) -> list[str]:
        """The readers of a system in this run."""
        return [r for r in systems()[name].readers if self.readers is None or r in self.readers]

    def wants(self, checkpoint: str) -> bool:
        """Whether the queries run at a checkpoint."""
        return self.checkpoints is None or checkpoint in self.checkpoints

    def every_checkpoint(self) -> tuple[str, ...]:
        """The workload's checkpoints, then ``maintained``."""
        return (*self.workload.checkpoints(), MAINTAINED)

    def last_checkpoint(self) -> str | None:
        """The last checkpoint the queries run at, where the concurrency phase runs too."""
        return next((c for c in reversed(self.every_checkpoint()) if self.wants(c)), None)

    def managed(self, system: System) -> bool:
        """Whether the run starts and stops the system's server (``BenchConfig.servers``)."""
        return self.config.servers is not None and system.server


def run(spec: RunSpec) -> Path:
    """Runs everything and returns the run file."""
    if not spec.reuse:  # the tables exist; the data files may not
        spec.workload.prepare()
    configure_proxy(spec)
    started = dt.datetime.now(dt.timezone.utc)
    header = run_header(spec, started)
    path = RUNS / f"{header['id']}.jsonl"
    append_jsonl(path, header)
    print(f"run {header['id']} → {path}", flush=True)

    results: list[QueryRecord] = []
    now = spec.workload.stages()[-1].now  # the data's present once everything arrived
    # One system at a time, start to finish, so a database server has the machine to itself (``server``).
    for s in spec.systems:
        with server(spec, spec.system(s), path) as up:
            if up:
                results += (reuse_system if spec.reuse else run_system)(spec, path, s, now, results)

    append_jsonl(path, end_record(started, results))
    return path


def end_record(started: dt.datetime, results: list[QueryRecord]) -> EndRecord:
    """The last record of a run: how long it took, and the answers that disagree."""
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
    for m in end["mismatches"]:
        print(f"ROW COUNT MISMATCH {m}", flush=True)
    return end


def run_system(spec: RunSpec, path: Path, name: str, now: int | None, earlier: list[QueryRecord]) -> list[QueryRecord]:
    """One system start to finish: ingest with its checkpoints, maintenance and its queries, concurrent dashboards."""
    results: list[QueryRecord] = []
    if not ingest_and_query(spec, path, name, results, earlier):
        return results
    if spec.wants(MAINTAINED):
        if not record(path, maintain(spec.system(name))):
            return results
        results += query_pass(spec, path, [name], At(MAINTAINED, now), [*earlier, *results])
    run_dashboards(spec, path, name, now)
    return results


def reuse_system(
    spec: RunSpec, path: Path, name: str, now: int | None, earlier: list[QueryRecord]
) -> list[QueryRecord]:
    """One system's queries on the tables an earlier run left, at each checkpoint, then concurrent dashboards."""
    results: list[QueryRecord] = []
    for checkpoint in spec.checkpoints or spec.every_checkpoint():
        results += query_pass(spec, path, [name], At(checkpoint, now), [*earlier, *results])
    run_dashboards(spec, path, name, now)
    return results


def run_dashboards(spec: RunSpec, path: Path, name: str, now: int | None) -> None:
    """Concurrent dashboards on one system at the last checkpoint, if the run asks for them."""
    last = spec.last_checkpoint()
    if spec.concurrency and spec.workload.dashboard_queries() and last and name != REFERENCE:
        reader = systems()[name].readers[0]
        queries = [
            {"name": q.name, "sql": q.sql_for(reader, spec.tuning, now)} for q in spec.workload.dashboard_queries()
        ]
        for clients in spec.concurrency:
            c = dashboards(spec.system(name), queries, last, clients, spec.concurrency_seconds)
            append_jsonl(path, c)
            print(concurrency_line(c), flush=True)


@contextlib.contextmanager
def server(spec: RunSpec, system: System, path: Path) -> Generator[bool]:
    """Starts a database system's server before it runs and stops it after; yields whether the system can run.

    Only when the run manages servers (``BENCH_SERVERS``). The server's versions are recorded once it's up.
    """
    script = spec.config.servers
    if script is None or not spec.managed(system):
        yield True
        return
    up = subprocess.run([str(script), "up", system.server_name()], capture_output=True, text=True, check=False)
    try:
        if up.returncode != 0:
            error = f"server: {failure(up.stderr)}"[:300]
            record(path, {"kind": "ingest", "system": system.name, "status": "error", "error": error})
            yield False
            return
        append_jsonl(path, {"kind": "versions", "system": system.name, "versions": system.versions()})
        yield True
    finally:
        subprocess.run([str(script), "down", system.server_name()], capture_output=True, check=False)


def system_meta(system: type[System]) -> SystemMeta:
    """How a system names itself and which readers it has, for reports that don't know it."""
    return {"title": system.title, "readers": list(system.readers)}


def run_header(spec: RunSpec, started: dt.datetime) -> RunRecord:
    """The first record of a run: what runs, where, and on which commit."""
    workload, target, config = spec.workload, spec.target, spec.config
    versions: dict[str, str] = {}
    for name in spec.systems:
        if not spec.managed(spec.system(name)):  # a managed server's are recorded once it's up
            versions.update(spec.system(name).versions())
    header: RunRecord = {
        "kind": "run",
        "id": f"{started:%Y%m%dT%H%M%SZ}-{workload.dataset}-{target.name}"
        + (f"-{spec.variant}" if spec.variant else "")
        + ("" if spec.tuning == Tuning.TUNED else f"-{spec.tuning}"),
        "workload": workload.name,
        "scale": workload.scale,
        "params": {**workload.params(), "span_days": workload.span_days},
        "target": target.name,
        "storage": target.storage,
        "machine": meta.machine(config.machine),
        "commit": meta.commit(),
        "dirty": meta.dirty(),
        "label": spec.label,
        "started": started.isoformat(timespec="seconds"),
        "readers": {s: spec.readers_of(s) for s in spec.systems},
        "systems": {s: system_meta(systems()[s]) for s in spec.systems},
        "versions": versions,
        "runs": spec.runs,
        "tuning": spec.tuning,
    }
    if target.s3:
        header.update(ttfb_ms=spec.ttfb_ms, mibps=spec.mibps)
    if spec.live_interval:
        header["live_interval_s"] = spec.live_interval
    if spec.pace:
        header["pace_s"] = spec.pace
    if spec.variant:
        header["variant"] = spec.variant
    # What an A/B arm changed: the reader's session settings and storage options, if set.
    if config.timelake_settings:
        header["reader_settings"] = dict(config.timelake_settings)
    if config.timelake_options:
        header["reader_options"] = dict(config.timelake_options)
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
    return system.on_s3(system.target)


def ingest(
    spec: RunSpec, system: System, checkpoint: Callable[[Stage], None]
) -> tuple[IngestRecord, LiveRecord | None]:
    """Creates the tables, loads the initial rows, then commits every arrival of the stream, stage by stage.

    After each stage, ``checkpoint`` runs its queries (not counted as ingest time or requests). Reading the workload's
    data isn't timed either. With ``live_interval`` set, fresh readers probe the tables during the live stage, through
    the proxy's uncounted listener (``live.py``).
    """
    out: IngestRecord = {"kind": "ingest", "system": system.name}
    requests = Requests(system)
    writer = Writer(spec, system, requests)
    try:
        system.reset()  # not timed: its cost depends on what the last run left
        setup = create_tables(spec.workload, system, requests)
        stages: dict[str, StageRecord] = {}
        for stage in spec.workload.stages():
            with requests.counting():
                done = writer.stage(stage)
            system.place.save_state({**system.state(), "phase": stage.checkpoint or stage.name, "ingested_at": now()})
            stages[stage.name] = {**done, **system.stats()}
            checkpoint(stage)
        out.update(ingest_summary(setup, stages, writer.latencies))
        out.update(system.ingest_extras())
    except Exception as e:  # noqa: BLE001 (a failing system is a result; the run goes on without it)
        out.update(status="error", error=f"{type(e).__name__}: {e}"[:300], commits=len(writer.latencies))
    probes = writer.finish_probes()
    if probes:
        out["probed"] = True
    return priced({**out, **requests.total()}, system), probes


def create_tables(workload: Workload, system: System, requests: Requests) -> float:
    """Creates the workload's tables, tunes them and loads the initial rows; the seconds that took.

    The schemas, statements and rows are read first, so only the system's work is timed and counted.
    """
    schemas = {t.name: workload.schema(t.name) for t in workload.tables}
    statements = workload.tuning_statements(system.name) if system.tuned else []
    initial = list(workload.initial())
    with requests.counting():
        start = time.perf_counter()
        for t in workload.tables:
            system.create(t, schemas[t.name])
        system.tune(statements)
        for table, data in initial:
            system.append(table, data)
        return time.perf_counter() - start


def ingest_summary(setup: float, stages: dict[str, StageRecord], latencies: list[float]) -> IngestRecord:
    """An ingest record's totals over its stages, and the storage after the last one."""
    last = list(stages.values())[-1]
    out: IngestRecord = {
        "status": "ok",
        "commits": len(latencies),
        "rows": sum(s["rows"] for s in stages.values()),
        "seconds": round(setup + sum(s["seconds"] for s in stages.values()), 2),
        "commit_seconds": round(sum(latencies), 2),
        "commit_ms_p50": round(statistics.median(latencies) * 1000, 1),
        "commit_ms_p95": round(float(np.percentile(latencies, 95)) * 1000, 1),
        "commit_ms_max": round(max(latencies) * 1000, 1),
        "objects": last["objects"],
        "bytes": last["bytes"],
        "data_files": last["data_files"],
        "data_bytes": last["data_bytes"],
    }
    if len(stages) > 1:
        out["stages"] = stages
    return out


class Writer:
    """Commits a system's stages: every arrival's batches, timed, and probes while the live stage arrives."""

    def __init__(self, spec: RunSpec, system: System, requests: Requests) -> None:
        self.spec = spec
        self.system = system
        self.requests = requests
        self.latencies: list[float] = []
        """Seconds per commit."""
        self.prober: Prober | None = None
        self.probes: LiveRecord | None = None

    def stage(self, stage: Stage) -> StageRecord:
        """Commits a stage's arrivals (paced and probed if it's live), then waits until readers can see them all.

        Its seconds leave out reading the arrivals from the workload's files, except where that overlapped the wait
        for the next arrival's time.
        """
        start = time.perf_counter()
        rows, commits = 0, len(self.latencies)
        pace = self.spec.pace if stage.live else 0.0
        due = start  # when the next arrival may be committed
        reading = 0.0  # seconds spent reading arrivals beyond the waits they overlapped
        arrivals = iter(stage.arrivals)
        for i in itertools.count():
            before = time.perf_counter()
            arrival = next(arrivals, None)
            ready = time.perf_counter()
            if arrival is None:
                reading += ready - before
                break
            reading += max(ready, due) - max(before, due)
            time.sleep(max(0.0, due - ready))
            arrived = time.perf_counter()
            rows += self.commit(arrival)
            due = arrived + pace
            if i == 0 and stage.live:
                self.start_probes()
            if i % 100 == 0:
                print(f"  {self.system.name}: {stage.name} arrival {i}, {time.perf_counter() - start:.1f}s", flush=True)
        settle = self.system.settle()  # rows a reader can't see yet haven't finished arriving
        seconds = time.perf_counter() - start - reading
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
        with self.requests.paused():  # the harness's bookkeeping, not the system's ingest
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
    requests = Requests(system)
    try:
        start = time.perf_counter()
        with requests.counting():
            system.maintain()
        seconds = time.perf_counter() - start
        state = {**system.place.load_state(), **system.state(), "phase": MAINTAINED}
        system.place.save_state(state)
        out.update(status="ok", seconds=round(seconds, 2))
        out.update(storage(system.stats()))
        out.update(system.maintain_extras())
    except Exception as e:  # noqa: BLE001 (as in ingest)
        out.update(status="error", error=f"{type(e).__name__}: {e}"[:300])
    return priced({**out, **requests.total()}, system)


def storage(stats: StorageStats) -> IngestRecord:
    """The objects and bytes a system's tables take, as an ingest or maintenance record holds them."""
    return {
        "objects": stats["objects"],
        "bytes": stats["bytes"],
        "data_files": stats["data_files"],
        "data_bytes": stats["data_bytes"],
    }


def priced(r: IngestRecord, system: System) -> IngestRecord:
    """Adds the phase's request cost and the storage's monthly cost at the target's list prices, where priced.

    Storage is priced only where it is billed at the target's object-storage prices: not a database server's disk,
    S3 Tables, or the reference's local files. Reports show only what this priced.
    """
    target = system.target.name
    if (usd := phase_cost(r, target)) is not None:
        r["usd"] = usd
    if (
        "bytes" in r
        and system.s3_storage_prices
        and system.on_s3(system.target)
        and (month := storage_month(r["bytes"], target)) is not None
    ):
        r["storage_usd_month"] = month
    return r


class Requests:
    """The S3 requests of one phase through the counting proxy, where the system's traffic is counted."""

    def __init__(self, system: System) -> None:
        self.proxy = system.place.config.proxy if counted(system) else None
        self.spent: list[IngestRecord] = []
        self._since: ProxyStats | None = None
        """The counters when the open stretch of counting started (None when not counting)."""

    @contextlib.contextmanager
    def counting(self) -> Generator[None]:
        """Counts the requests made inside the block."""
        self._open()
        try:
            yield
        finally:
            self._close()

    @contextlib.contextmanager
    def paused(self) -> Generator[None]:
        """Leaves the requests made inside the block out, within ``counting``."""
        was_counting = self._since is not None
        self._close()
        try:
            yield
        finally:
            if was_counting:
                self._open()

    def total(self) -> IngestRecord:
        """The requests counted so far, with their kinds (nothing if the traffic isn't counted)."""
        if not self.spent:
            return {}
        out: IngestRecord = {"requests": sum(r.get("requests", 0) for r in self.spent)}
        kinds: dict[str, int] = {}
        for r in self.spent:
            for k, n in r.get("kinds", {}).items():
                kinds[k] = kinds.get(k, 0) + n
        if kinds:
            out["kinds"] = kinds
        return out

    def _open(self) -> None:
        self._since = self.proxy.stats() if self.proxy else None

    def _close(self) -> None:
        if self.proxy and self._since is not None:
            after = self.proxy.stats()
            spent: IngestRecord = {"requests": after["requests"] - self._since["requests"]}
            if kinds := request_kinds(self._since, after):
                spent["kinds"] = kinds
            self.spent.append(spent)
        self._since = None


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
    sql = query.sql_for(reader, system.tuning, at.now)
    place = system.place
    if reader in system.commands:
        cmd = type(system).reader_command(reader, place, place.load_state(), spec.runs)
    else:
        job = job_for(system, reader)
        job["runs"] = spec.runs
        cmd = command(job)
    proxy = spec.config.proxy if counted(system) else None
    before = proxy.stats() if proxy else None
    timeout = spec.reference_timeout if system.name == REFERENCE else spec.timeout
    try:
        # A failing query is a result to record, not a reason to stop: no check=True.
        proc = subprocess.run(
            cmd, input=sql, capture_output=True, text=True, timeout=timeout, check=False, env=spec.config.environ()
        )
    except subprocess.TimeoutExpired:
        return {**out, "status": "timeout"}
    if proc.returncode != 0:
        return {**out, "status": "error", "error": failure(proc.stderr)[:300]}
    measured: QueryRecord = json.loads(proc.stdout.strip().splitlines()[-1])
    out.update(measured, status="ok")
    # Requests by kind over all runs, for readers that don't report them per run (the Timelake CLI).
    if proxy and before and "kinds" not in out and (total := request_kinds(before, proxy.stats())):
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


def configure_proxy(spec: RunSpec) -> None:
    """Sets the counting proxy's latency and throughput cap for an S3 run: every one, since it keeps the last run's."""
    if spec.target.s3:
        set_proxy(spec, "ttfb", f"ms={spec.ttfb_ms}")
        set_proxy(spec, "mibps", f"v={spec.mibps}")


def set_proxy(spec: RunSpec, setting: str, query: str) -> None:
    """Sets the counting proxy's ``ttfb`` (``ms=20``) or ``mibps`` (``v=50``)."""
    with urllib.request.urlopen(f"http://{spec.config.proxy.admin}/{setting}?{query}", data=b"", timeout=5):  # a POST
        pass


def now() -> str:
    """The current UTC time, ISO 8601."""
    return dt.datetime.now(dt.timezone.utc).isoformat()
