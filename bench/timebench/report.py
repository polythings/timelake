"""Reading run files back: one run as Markdown, and the history of runs as a table."""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import TYPE_CHECKING, cast

from timebench import RUNS
from timebench.results import (
    MAINTAINED,
    ConcurrencyRecord,
    EndRecord,
    IngestRecord,
    LiveRecord,
    QueryRecord,
    RunRecord,
    best_time,
    first_time,
    geomean,
    read_jsonl,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path


class Run:
    """One run file, parsed."""

    def __init__(self, path: Path) -> None:
        records = read_jsonl(path)
        self.path = path
        self.header = cast("RunRecord", records[0])
        self.end = next((cast("EndRecord", r) for r in records if r.get("kind") == "end"), None)
        self.ingest = [cast("IngestRecord", r) for r in records if r.get("kind") == "ingest"]
        self.maintain = [cast("IngestRecord", r) for r in records if r.get("kind") == "maintain"]
        self.queries = [cast("QueryRecord", r) for r in records if r.get("kind") == "query"]
        self.live = [cast("LiveRecord", r) for r in records if r.get("kind") == "live"]
        self.concurrency = [cast("ConcurrencyRecord", r) for r in records if r.get("kind") == "concurrency"]

    @property
    def id(self) -> str:
        """The run id."""
        return str(self.header["id"])

    def engines(self) -> list[str]:
        """``system+reader`` pairs, in the order they ran."""
        return list(dict.fromkeys(f"{q['system']}+{q['reader']}" for q in self.queries))

    def checkpoints(self) -> list[str]:
        """Checkpoints with queries, in order."""
        return list(dict.fromkeys(q["checkpoint"] for q in self.queries))

    def ok(self, checkpoint: str) -> dict[str, dict[str, QueryRecord]]:
        """Successful queries at a checkpoint: engine -> query -> record."""
        out: dict[str, dict[str, QueryRecord]] = defaultdict(dict)
        for q in self.queries:
            if q["checkpoint"] == checkpoint and q["status"] == "ok":
                out[f"{q['system']}+{q['reader']}"][q["query"]] = q
        return out

    def summary(self, checkpoint: str) -> dict[str, dict[str, float]]:
        """Per engine: geometric means of first and best run, over the queries every engine completed."""
        ok = self.ok(checkpoint)
        if not ok:
            return {}
        common = set.intersection(*(set(qs) for qs in ok.values()))
        out = {}
        for engine, qs in ok.items():
            rs = [qs[q] for q in sorted(common)]
            out[engine] = {"first": geomean(map(first_time, rs)), "best": geomean(map(best_time, rs))}
            if rs and all("requests" in r for r in rs):
                out[engine]["requests"] = geomean(r["requests"][0] for r in rs)
            out[engine]["queries"] = len(rs)
        return out


def runs(workload: str | None = None, label: str | None = None) -> list[Run]:
    """All runs, oldest first, optionally of one workload or with one label."""
    found = [Run(p) for p in sorted(RUNS.glob("*.jsonl"))]
    return [
        r
        for r in found
        if (workload is None or r.header["workload"] == workload) and (label is None or r.header.get("label") == label)
    ]


def markdown(run: Run) -> str:
    """One run as Markdown: where it ran, ingest and maintenance, query times."""
    h = run.header
    lines = [
        f"# {h['workload']} ({h['scale']}) on {h['target']}",
        "",
        f"Run `{run.id}`: commit `{h['commit'][:10]}`{' (with local changes)' if h.get('dirty') else ''}, "
        f"{h['machine']}, started {h['started']}{duration(run)}.",
        "",
        "| Phase | System | Seconds | Commits | Commit p50 | Commit p95 | Objects | Bytes | Requests | Status |",
        "|---|---|--:|--:|--:|--:|--:|--:|--:|---|",
    ]
    lines.extend(
        f"| {r['kind']} | {r['system']} | {cell(r, 'seconds')} | {cell(r, 'commits')} | "
        f"{cell(r, 'commit_ms_p50')} | {cell(r, 'commit_ms_p95')} | {cell(r, 'objects')} | "
        f"{cell(r, 'bytes')} | {cell(r, 'requests')} | {r['status']} |"
        for r in run.ingest + run.maintain
    )
    for checkpoint in run.checkpoints():
        lines += ["", f"## Queries, {checkpoint}", ""]
        summary = run.summary(checkpoint)
        lines += ["| Engine | Queries | First run (geomean) | Best repeat (geomean) | Requests (first run) |"]
        lines += ["|---|--:|--:|--:|--:|"]
        for engine, s in sorted(summary.items(), key=lambda kv: kv[1]["first"]):
            reqs = f"{s['requests']:.0f}" if "requests" in s else ""
            lines.append(f"| {engine} | {s['queries']:.0f} | {s['first']:.3f} s | {s['best']:.3f} s | {reqs} |")
        failed = [q for q in run.queries if q["checkpoint"] == checkpoint and q["status"] != "ok"]
        if failed:
            lines += [
                "",
                "Failed: " + ", ".join(f"{q['system']}+{q['reader']} {q['query']} ({q['status']})" for q in failed),
            ]
    lines += live_section(run) + concurrency_section(run) + cost_section(run)
    if run.end and run.end.get("mismatches"):
        lines += ["", "Row counts differ: " + "; ".join(run.end["mismatches"])]
    return "\n".join(lines) + "\n"


def duration(run: Run) -> str:
    """How long the run took, for the line after ``started``; ``(incomplete)`` without an end record."""
    if run.end is None:
        return " (incomplete)"
    return f", {run.end['seconds'] / 60:.0f} min" if "seconds" in run.end else ""


def history(found: list[Run], checkpoint: str = MAINTAINED) -> str:
    """One line per run: first-run geometric mean per engine at a checkpoint."""
    engines = list(dict.fromkeys(e for r in found for e in r.engines()))
    lines = [
        "| Run | Commit | Target | " + " | ".join(engines) + " |",
        "|---|---|---|" + "--:|" * len(engines),
    ]
    for r in found:
        summary = r.summary(checkpoint)
        cells = [f"{summary[e]['first']:.3f}" if e in summary else "" for e in engines]
        lines.append(f"| {r.id} | {r.header['commit'][:7]} | {r.header['target']} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def cell(record: Mapping[str, object], key: str) -> str:
    """A table cell: the value, or nothing when it's missing or null."""
    value = record.get(key)
    return "" if value is None else str(value)


def live_section(run: Run) -> list[str]:
    """Probes during ingest: how far behind fresh readers were, and the dashboard queries' latency."""
    if not run.live:
        return []
    lines = [
        "",
        "## While ingesting",
        "",
        "Fresh readers every few seconds during ingest. Behind: probes that missed a finished commit.",
        "",
        "| System | Rounds | Behind | Lag p50 | Lag max | Dashboard query p50 | Errors |",
        "|---|--:|--:|--:|--:|--:|--:|",
    ]
    for r in run.live:
        lags = r["lag_s"] or [0.0]
        times = [t for ts in r["queries"].values() for t in ts]
        p50 = f"{statistics.median(times):.3f} s" if times else ""
        behind = sum(1 for x in r["lag_s"] if x > 0)
        lines.append(
            f"| {r['system']} | {r['rounds']} | {behind}/{len(r['lag_s'])} | {statistics.median(lags):.2f} s | "
            f"{max(lags):.2f} s | {p50} | {len(r['errors'])} |"
        )
    return lines


def concurrency_section(run: Run) -> list[str]:
    """Concurrent dashboards: throughput and latency by client count."""
    if not run.concurrency:
        return []
    lines = [
        "",
        "## Concurrent dashboards",
        "",
        "| System | Clients | Queries/s | p50 | p95 | p99 | Errors |",
        "|---|--:|--:|--:|--:|--:|--:|",
    ]
    lines += [
        f"| {r['system']}+{r['reader']} | {r['clients']} | {r['qps']} | {cell(r, 'p50_s')} | {cell(r, 'p95_s')} | "
        f"{cell(r, 'p99_s')} | {r['errors']} |"
        for r in run.concurrency
    ]
    return lines


def cost_section(run: Run) -> list[str]:
    """Dollars at the target's list prices: ingest and storage per system, and queries per engine."""
    target = run.header["target"]
    span_days = run.header.get("params", {}).get("span_days")
    span = span_days if isinstance(span_days, (int, float)) and span_days else 3
    month = 30 / span

    def money(x: float | None, digits: int = 2) -> str:
        return "" if x is None else f"${x:.{digits}f}"

    # Only what the run priced (``harness.priced``): servers' disks and S3 Tables have no price here.
    rows = []
    for r in run.ingest:
        ingest = r.get("usd")
        maintained = next((m for m in run.maintain if m["system"] == r["system"]), r)
        stored = maintained.get("storage_usd_month")
        if ingest is None and stored is None:
            continue
        per_month = None if ingest is None else ingest * month
        rows.append(f"| {r['system']} | {money(ingest)} | {money(per_month)} | {money(stored, 4)} |")
    by_engine: dict[str, list[float]] = defaultdict(list)
    for q in run.queries:
        c = q.get("usd")
        if q["status"] == "ok" and q["checkpoint"] == MAINTAINED and c is not None:
            by_engine[f"{q['system']}+{q['reader']}"].append(c)
    if not rows and not by_engine:
        return []
    lines = [
        "",
        f"## Cost on {target}",
        "",
        f"Requests and storage at list prices; {span:g} days of data. Compute is not included.",
    ]
    if rows:
        lines += [
            "",
            "| System | Ingest requests | Per month at this rate | Storage per month |",
            "|---|--:|--:|--:|",
            *rows,
        ]
    if by_engine:
        lines += ["", "| Engine | $ per 1M queries (first run, mean) |", "|---|--:|"]
        lines += [f"| {e} | ${statistics.mean(cs) * 1e6:.2f} |" for e, cs in sorted(by_engine.items())]
    return lines
