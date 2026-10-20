"""Runs one query, or one client of the concurrency phase, in a fresh process (started by ``timebench.harness``).

It prints its measurements as one JSON line.

    python -m timebench.worker '{"system": "iceberg", "reader": "duckdb", "target": "local", "dataset": ..., "runs": 2}'

The SQL comes on stdin. The query runs once ("first run": cold metadata, footers and data in this process) and then
``runs`` more times; every run fully materializes the result.
"""

from __future__ import annotations

import json
import sys
import time
from typing import TYPE_CHECKING, Any

from timebench.s3 import PROXY, ProxyStats
from timebench.systems import systems
from timebench.targets import Place, Target

if TYPE_CHECKING:
    from timebench.results import QueryRecord


def command(job: dict[str, Any]) -> list[str]:
    """The command that runs a job in a fresh worker process."""
    return [sys.executable, "-m", "timebench.worker", json.dumps(job)]


def request_kinds(before: ProxyStats, after: ProxyStats) -> dict[str, int]:
    """Requests by kind between two proxy snapshots (kinds that didn't change are left out)."""
    old = before.get("kinds", {})
    return {k: n - old.get(k, 0) for k, n in after.get("kinds", {}).items() if n != old.get(k, 0)}


def single(job: dict[str, Any], sql: str) -> QueryRecord:
    """One query: a first run, then ``runs`` repeats, with request counts when the proxy counts them."""
    place = Place(Target(job["target"]), job["dataset"], job["system"], job.get("tuning", "tuned"))
    system = systems()[job["system"]]
    counted = place.target.s3 and not system.server and job.get("count", True)
    setup_start = time.perf_counter()
    run = system.open_reader(job["reader"], place, place.load_state())
    setup_s = time.perf_counter() - setup_start
    times: list[float] = []
    requests: list[int] = []
    nbytes: list[int] = []
    kinds: list[dict[str, int]] = []
    result = None
    for _ in range(job["runs"] + 1):
        before = PROXY.stats() if counted else None
        start = time.perf_counter()
        result = run(sql)
        times.append(time.perf_counter() - start)
        if before:
            after = PROXY.stats()
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


def client(job: dict[str, Any]) -> dict[str, Any]:
    """A dashboard backend: one reader kept open, the queries in turn (starting at ``offset``) until the deadline.

    A query that fails is counted once and left out from then on (a dashboard panel showing an error isn't retried in
    a loop), so a failure that comes back at once doesn't spin the client. Prints each query's name and seconds, and
    the errors.
    """
    place = Place(Target(job["target"]), job["dataset"], job["system"], job.get("tuning", "tuned"))
    setup_start = time.perf_counter()
    run = systems()[job["system"]].open_reader(job["reader"], place, place.load_state())
    setup_s = time.perf_counter() - setup_start
    queries: list[tuple[str, str]] = [(q["name"], q["sql"]) for q in job["queries"]]
    latencies: list[tuple[str, float]] = []
    errors: list[str] = []
    deadline = time.monotonic() + job["seconds"]
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
    return {"setup_s": setup_s, "latencies": latencies, "errors": errors}


def main() -> None:
    """Runs one job and prints its measurements as one JSON line."""
    job = json.loads(sys.argv[1])
    out = client(job) if job.get("mode") == "client" else single(job, sys.stdin.read())
    print(json.dumps(out))


if __name__ == "__main__":
    main()
