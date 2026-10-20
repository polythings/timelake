"""Runs one query, or one client of the concurrency phase, in a fresh process (started by ``timebench.harness``).

It prints its measurements as one JSON line.

    python -m timebench.worker '{"system": "iceberg", "reader": "duckdb", "target": "local", "dataset": ..., "runs": 2}'

The SQL comes on stdin. The query runs once ("first run": cold metadata, footers and data in this process) and then
``runs`` more times; every run fully materializes the result. Settings come from the environment (``BenchConfig``),
which the harness sets.
"""

from __future__ import annotations

import json
import sys
import time
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict

from timebench.config import BenchConfig
from timebench.systems import systems
from timebench.targets import Place, Target, Tuning

if TYPE_CHECKING:
    from timebench.results import QueryRecord
    from timebench.s3 import ProxyStats
    from timebench.systems import System


class Job(TypedDict):
    """What a worker process runs: a reader of one system's tables, and the query or client loop to run on it."""

    system: str
    reader: str
    target: str
    dataset: str
    tuning: Tuning
    runs: NotRequired[int]
    """Repeat runs after the first run (single queries)."""
    count: NotRequired[bool]
    """Whether to count the query's requests (default: yes, where the proxy counts the system's traffic)."""
    mode: NotRequired[Literal["client"]]
    """A dashboard client instead of a single query."""
    queries: NotRequired[list[dict[str, str]]]
    """A client's queries (``name``, ``sql``)."""
    seconds: NotRequired[float]
    """How long a client queries."""
    offset: NotRequired[int]
    """The query a client starts at."""


class ClientResult(TypedDict):
    """What a dashboard client prints."""

    setup_s: float
    latencies: list[tuple[str, float]]
    """Query name and seconds, of every query it finished."""
    errors: list[str]
    finished: int
    """Queries that finished within the seconds (the last one may run past them)."""


def job_for(system: System, reader: str) -> Job:
    """A job on a system's tables with one of its readers; add what to run."""
    place = system.place
    return {
        "system": system.name,
        "reader": reader,
        "target": place.target.name,
        "dataset": place.dataset,
        "tuning": place.tuning,
    }


def place_of(job: Job, config: BenchConfig) -> Place:
    """Where the job's tables are."""
    return Place(Target(job["target"]), job["dataset"], job["system"], Tuning(job["tuning"]), config=config)


def command(job: Job) -> list[str]:
    """The command that runs a job in a fresh worker process."""
    return [sys.executable, "-m", "timebench.worker", json.dumps(job)]


def failure(stderr: str) -> str:
    """Why a process failed, from its stderr: the last line that names an error, else the last line."""
    lines = stderr.strip().splitlines() or ["?"]
    return next((line for line in reversed(lines) if "Error" in line or "Exception" in line), lines[-1])


def request_kinds(before: ProxyStats, after: ProxyStats) -> dict[str, int]:
    """Requests by kind between two proxy snapshots (kinds that didn't change are left out)."""
    old = before.get("kinds", {})
    return {k: n - old.get(k, 0) for k, n in after.get("kinds", {}).items() if n != old.get(k, 0)}


def single(job: Job, sql: str, config: BenchConfig) -> QueryRecord:
    """One query: a first run, then ``runs`` repeats, with request counts when the proxy counts them."""
    place = place_of(job, config)
    system = systems()[job["system"]]
    counted = system.on_s3(place.target) and job.get("count", True)
    setup_start = time.perf_counter()
    run = system.open_reader(job["reader"], place, place.load_state())
    setup_s = time.perf_counter() - setup_start
    times: list[float] = []
    requests: list[int] = []
    nbytes: list[int] = []
    kinds: list[dict[str, int]] = []
    result = None
    for _ in range(job.get("runs", 0) + 1):
        before = config.proxy.stats() if counted else None
        start = time.perf_counter()
        result = run(sql)
        times.append(time.perf_counter() - start)
        if before:
            after = config.proxy.stats()
            requests.append(after["requests"] - before["requests"])
            nbytes.append(after["bytes"] - before["bytes"])
            if "kinds" in after:  # AWS mode: requests by kind (GET, HEAD, LIST, PUT, ...)
                kinds.append(request_kinds(before, after))
    out: QueryRecord = {"times": times, "rows": result.rows if result else None, "setup_s": setup_s}
    if result and isinstance(result.value, (int, float)):
        out["value"] = result.value
    if result and result.sums is not None:
        out["sums"] = result.sums
    if counted:
        out.update(requests=requests, bytes=nbytes)
        if kinds:
            out["kinds"] = kinds
    return out


def client(job: Job, config: BenchConfig) -> ClientResult:
    """A dashboard backend: one reader kept open, the queries in turn (starting at ``offset``) for ``seconds``.

    Once its reader is open it prints ``ready`` and waits for the start time (epoch seconds) on stdin, which every
    client of the run gets (``concurrency.py``), so they all query over the same seconds.

    A query that fails is counted once and left out from then on (a dashboard panel showing an error isn't retried in
    a loop), so a failure that comes back at once doesn't spin the client.
    """
    place = place_of(job, config)
    setup_start = time.perf_counter()
    run = systems()[job["system"]].open_reader(job["reader"], place, place.load_state())
    setup_s = time.perf_counter() - setup_start
    queries: list[tuple[str, str]] = [(q["name"], q["sql"]) for q in job.get("queries", [])]
    latencies: list[tuple[str, float]] = []
    errors: list[str] = []
    finished = 0
    print("ready", flush=True)
    start_at = float(sys.stdin.readline())
    time.sleep(max(0.0, start_at - time.time()))
    deadline = time.monotonic() + start_at + job.get("seconds", 0) - time.time()
    i = job.get("offset", 0)
    while queries and time.monotonic() < deadline:
        i %= len(queries)
        name, sql = queries[i]
        start = time.perf_counter()
        try:
            run(sql)
        except Exception as e:  # noqa: BLE001 (a failed query is counted, and the client goes on without it)
            errors.append(f"{name}: {type(e).__name__}: {e}"[:200])
            queries.pop(i)
            continue
        i += 1
        latencies.append((name, time.perf_counter() - start))
        finished += time.monotonic() <= deadline
    return {"setup_s": setup_s, "latencies": latencies, "errors": errors, "finished": finished}


def main() -> None:
    """Runs one job and prints its measurements as one JSON line."""
    job: Job = json.loads(sys.argv[1])
    config = BenchConfig.from_env()
    out = client(job, config) if job.get("mode") == "client" else single(job, sys.stdin.read(), config)
    print(json.dumps(out))


if __name__ == "__main__":
    main()
