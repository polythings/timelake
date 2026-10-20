"""Concurrent dashboards: N clients at once, each keeping a reader open and running the dashboard queries in turn.

Each client is its own process, like a dashboard backend; it opens the system's own reader once and loops over the
queries (starting at a different one per client) for a fixed time. Reported: queries per second over all clients,
latency percentiles, and errors.
"""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import numpy as np

from timebench.worker import command

if TYPE_CHECKING:
    from timebench.results import ConcurrencyRecord
    from timebench.systems import System

SETUP_GRACE_S = 300
"""How long a client may take to start and open its reader, beyond the measured seconds."""


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
        subprocess.Popen(command({**job, "offset": i}), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for i in range(clients)
    ]
    latencies: list[float] = []
    errors: list[str] = []
    for proc in procs:
        try:
            stdout, stderr = proc.communicate(timeout=seconds + SETUP_GRACE_S)
        except subprocess.TimeoutExpired:
            proc.kill()
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
