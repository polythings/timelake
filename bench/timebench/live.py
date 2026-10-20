"""Probes while a system ingests: fresh readers run the dashboard queries and ask for the newest timestamp they see.

A probe round starts every ``interval`` seconds while the stream is being committed. Each query runs in a fresh
process with the system's own reader, as a dashboard opening during ingest would, through the proxy's uncounted
listener so that ingest's request counts stay the writer's. The freshness probe tells how far
behind a reader is: the seconds since the oldest commit it didn't see had finished (0 when it sees every finished
commit).
"""

from __future__ import annotations

import dataclasses
import json
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from timebench.s3 import PROBE_PROXY
from timebench.worker import command, failure, job_for

if TYPE_CHECKING:
    from timebench.results import LiveRecord
    from timebench.systems import System
    from timebench.workloads import Query, Workload

PROBE_TIMEOUT_S = 300


@dataclass(frozen=True)
class Commit:
    """One commit of the live table: when it finished (wall clock) and the newest timestamp in it (epoch seconds)."""

    finished: float
    newest: int


def lag(commits: list[Commit], probe_started: float, seen: int | None) -> float:
    """Seconds since the oldest commit finished before the probe whose rows the probe didn't see; 0 if it saw all."""
    missed = [c.finished for c in commits if c.finished <= probe_started and (seen is None or c.newest > seen)]
    return round(probe_started - min(missed), 3) if missed else 0.0


class Prober:
    """Runs probe rounds in a background thread while ``ingest`` commits."""

    def __init__(self, workload: Workload, system: System, interval: float) -> None:
        self.system = system
        self.reader = type(system).readers[0]
        self.interval = interval
        freshness = workload.freshness_query()
        self.freshness = freshness
        self.queries: list[Query] = [*([freshness] if freshness else []), *workload.dashboard_queries()]
        self.commits: list[Commit] = []
        self.lags: list[float] = []
        self.latencies: dict[str, list[float]] = {q.name: [] for q in workload.dashboard_queries()}
        self.errors: list[str] = []
        self.rounds = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def committed(self, newest: int) -> None:
        """Called by the writer after each commit of the live table."""
        self.commits.append(Commit(time.time(), newest))

    def start(self) -> None:
        """Starts probing."""
        self.thread.start()

    def finish(self) -> LiveRecord:
        """Stops probing (after the running probe) and returns what it saw."""
        self.stop.set()
        self.thread.join()
        return {
            "kind": "live",
            "system": self.system.name,
            "reader": self.reader,
            "interval_s": self.interval,
            "rounds": self.rounds,
            "lag_s": self.lags,
            "queries": {name: [round(t, 4) for t in ts] for name, ts in self.latencies.items()},
            "errors": self.errors[:20],
        }

    def loop(self) -> None:
        """Probe rounds until stopped."""
        while not self.stop.wait(self.interval):
            self.rounds += 1
            for query in self.queries:
                if self.stop.is_set():
                    return
                self.probe(query)

    def probe(self, query: Query) -> None:
        """Runs one query in a fresh process and records its time, or the lag for the freshness probe."""
        place = self.system.place
        job = job_for(self.system, self.reader)
        job.update(runs=0, count=False)  # the proxy's counters include the writer's requests
        env = dataclasses.replace(place.config, proxy=PROBE_PROXY).environ()
        started = time.time()
        commits = list(self.commits)
        now = max((c.newest for c in commits), default=None)  # the data's present, as far as the writer got
        try:
            proc = subprocess.run(
                command(job),
                input=query.sql_for(self.reader, place.tuning, now),
                capture_output=True,
                text=True,
                timeout=PROBE_TIMEOUT_S,
                check=False,
                env=env,
            )
        except subprocess.TimeoutExpired:
            self.errors.append(f"{query.name}: timeout")
            return
        if proc.returncode != 0:
            self.errors.append(f"{query.name}: {failure(proc.stderr)}"[:200])
            return
        out = json.loads(proc.stdout.strip().splitlines()[-1])
        if query is self.freshness:
            value = out.get("value")
            self.lags.append(lag(commits, started, int(value) if value is not None else None))
        else:
            self.latencies[query.name].append(out["times"][0])
