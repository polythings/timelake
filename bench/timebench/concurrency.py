"""Concurrent dashboards: N clients at once, each keeping a reader open and running the dashboard queries in turn.

Each client is its own process, like a dashboard backend; it opens the system's own reader once and loops over the
queries (starting at a different one per client) for a fixed time. The clients start querying together, once every
one has opened its reader, so they overlap for the whole time. Reported: queries per second over all clients, latency
percentiles, and errors.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
from typing import TYPE_CHECKING

import numpy as np

from timebench.worker import command

if TYPE_CHECKING:
    from timebench.results import ConcurrencyRecord
    from timebench.systems import System

SETUP_GRACE_S = 300
"""How long a client may take to start and open its reader, and to finish its last query after the measured
seconds."""
START_DELAY_S = 0.2
"""From the last client being ready to the shared start, so that each has read the start time by then."""


def dashboards(
    system: System, queries: list[dict[str, str]], checkpoint: str, clients: int, seconds: float
) -> ConcurrencyRecord:
    """Runs ``clients`` dashboard clients for ``seconds`` and summarizes their latencies.

    ``queries`` are the dashboard's queries (``name``, ``sql``) in the system's own reader's SQL.
    """
    reader = type(system).readers[0]
    place = system.place
    job = {
        "mode": "client",
        "system": system.name,
        "reader": reader,
        "target": place.target.name,
        "dataset": place.dataset,
        "tuning": place.tuning,
        "queries": queries,
        "seconds": seconds,
    }
    procs = [
        subprocess.Popen(
            command({**job, "offset": i}),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for i in range(clients)
    ]
    ready = opened(procs, SETUP_GRACE_S)
    start_at = time.time() + START_DELAY_S
    latencies: list[float] = []
    errors: list[str] = []
    for proc, is_ready in zip(procs, ready, strict=True):
        if not is_ready:
            proc.kill()  # if it's still opening its reader
            _, stderr = proc.communicate()
            errors.append((stderr.strip().splitlines() or ["client didn't open its reader in time"])[-1][:200])
            continue
        if proc.stdin:
            proc.stdin.write(f"{start_at}\n")
            proc.stdin.flush()
    for proc, is_ready in zip(procs, ready, strict=True):
        if not is_ready:
            continue
        try:
            stdout, stderr = proc.communicate(timeout=start_at - time.time() + seconds + SETUP_GRACE_S)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            errors.append("client timed out")
            continue
        if proc.returncode != 0:
            errors.append((stderr.strip().splitlines() or ["?"])[-1][:200])
            continue
        out = json.loads(stdout.strip().splitlines()[-1])
        latencies += [t for _, t in out["latencies"]]
        errors += out["errors"]
    record: ConcurrencyRecord = {
        "kind": "concurrency",
        "system": system.name,
        "reader": reader,
        "checkpoint": checkpoint,
        "clients": clients,
        "seconds": seconds,
        "queries": len(latencies),
        "qps": round(len(latencies) / seconds, 2),
        "errors": len(errors),
    }
    if latencies:
        p50, p95, p99 = np.percentile(latencies, [50, 95, 99])
        record.update(p50_s=round(float(p50), 4), p95_s=round(float(p95), 4), p99_s=round(float(p99), 4))
    if errors:
        record["error"] = errors[0]
    return record


def opened(procs: list[subprocess.Popen[str]], timeout: float) -> list[bool]:
    """Waits until each client has opened its reader (it prints ``ready``), at most ``timeout`` seconds in all."""
    lines = [""] * len(procs)

    def read(i: int, proc: subprocess.Popen[str]) -> None:
        if proc.stdout:
            lines[i] = proc.stdout.readline()

    threads = [threading.Thread(target=read, args=(i, p), daemon=True) for i, p in enumerate(procs)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    return [line.strip() == "ready" for line in lines]
