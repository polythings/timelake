"""``timebench``: run benchmarks and read their history (bench/README.md).

uv run timebench list
uv run timebench run tsbs-iot --scale small --systems timelake,iceberg
uv run timebench run tsbs-iot --target s3 --ttfb-ms 20        # local RustFS behind tools/s3proxy
uv run timebench run tsbs-iot --pace 1 --live-interval 15 --concurrency 1,4,16   # probes during ingest; dashboards
uv run timebench profile weekly                                # bench/profiles/weekly.toml
uv run timebench report latest
uv run timebench history --workload tsbs-iot
"""

from __future__ import annotations

import argparse

import tomllib

from timebench import BENCH, RUNS
from timebench.harness import RunSpec, run
from timebench.report import Run, history, markdown, runs
from timebench.systems import systems
from timebench.targets import Target
from timebench.workloads import Workload, workloads

PROFILES = BENCH / "profiles"


def make_workload(name: str, scale: str | None, params: dict[str, object]) -> Workload:
    """A workload by name, with its scale and parameters."""
    if name not in workloads():
        msg = f"unknown workload {name!r} (workloads: {', '.join(workloads())})"
        raise SystemExit(msg)
    return workloads()[name](scale, **params)


def parse_params(pairs: list[str]) -> dict[str, object]:
    """``key=value`` pairs; integer values become ints."""
    out: dict[str, object] = {}
    for pair in pairs:
        key, _, value = pair.partition("=")
        out[key] = int(value) if value.isdigit() else value
    return out


def split(value: str | None) -> list[str] | None:
    """A comma-separated list, None if not given."""
    return value.split(",") if value else None


def cmd_list(_: argparse.Namespace) -> None:
    """Prints the workloads with their scales, and the systems with their readers."""
    print("Workloads:")
    for w in workloads().values():
        print(f"  {w.name}: {w.title}; suites {', '.join(w.suites)}")
        for scale, about in w.scales.items():
            print(f"    {scale:9} {about}{' (default)' if scale == w.default_scale else ''}")
    print("Systems (readers):")
    for s in systems().values():
        print(f"  {s.name:12} {s.title} ({', '.join(s.readers)}){'; a database server' if s.server else ''}")


def cmd_run(args: argparse.Namespace) -> None:
    """Runs one workload on some systems."""
    workload = make_workload(args.workload, args.scale, parse_params(args.param))
    names = split(args.systems) or [s for s in systems() if s != "s3tables" and not systems()[s].server]
    unknown = set(names) - set(systems())
    if unknown:
        msg = f"unknown systems {sorted(unknown)} (systems: {', '.join(systems())})"
        raise SystemExit(msg)
    spec = RunSpec(
        workload=workload,
        target=Target(args.target),
        systems=names,
        readers=split(args.readers),
        checkpoints=None if args.checkpoints is None else tuple(c for c in args.checkpoints.split(",") if c),
        queries=set(split(args.queries) or []) or None,
        runs=args.runs,
        timeout=args.timeout,
        reference_timeout=args.reference_timeout,
        skip_after_timeout=args.skip_after_timeout,
        ttfb_ms=args.ttfb_ms,
        mibps=args.mibps,
        label=args.label,
        reuse=args.reuse,
        live_interval=args.live_interval,
        concurrency=tuple(int(n) for n in split(args.concurrency) or []),
        concurrency_seconds=args.concurrency_seconds,
        pace=args.pace,
        variant=args.variant,
    )
    print(markdown(Run(run(spec))))


def cmd_profile(args: argparse.Namespace) -> None:
    """Runs every entry of a profile (``profiles/<name>.toml``)."""
    profile = tomllib.loads((PROFILES / f"{args.name}.toml").read_text())
    for entry in profile["run"]:
        if args.target and entry["target"] != args.target:
            continue
        spec = RunSpec(
            workload=make_workload(entry["workload"], entry.get("scale"), entry.get("params", {})),
            target=Target(entry["target"]),
            systems=entry["systems"],
            runs=entry.get("runs", profile.get("runs", 2)),
            timeout=entry.get("timeout", profile.get("timeout", 600)),
            reference_timeout=entry.get("reference_timeout", profile.get("reference_timeout", 1800)),
            skip_after_timeout=entry.get("skip_after_timeout", profile.get("skip_after_timeout", True)),
            label=profile.get("label", args.name),
            live_interval=entry.get("live_interval", profile.get("live_interval", 0)),
            concurrency=tuple(entry.get("concurrency", profile.get("concurrency", []))),
            concurrency_seconds=entry.get("concurrency_seconds", profile.get("concurrency_seconds", 30)),
            pace=entry.get("pace", profile.get("pace", 0)),
            variant=entry.get("variant", ""),
            checkpoints=tuple(entry["checkpoints"]) if "checkpoints" in entry else None,
        )
        try:
            print(markdown(Run(run(spec))))
        except Exception as e:  # noqa: BLE001 (one failed entry is reported; the others still run)
            print(f"PROFILE ENTRY FAILED {entry['workload']} {entry['target']}: {type(e).__name__}: {e}", flush=True)


def cmd_report(args: argparse.Namespace) -> None:
    """Prints one run as Markdown."""
    if args.run == "latest":
        found = sorted(RUNS.glob("*.jsonl"))
        if not found:
            msg = "no runs yet"
            raise SystemExit(msg)
        path = found[-1]
    else:
        path = RUNS / f"{args.run.removesuffix('.jsonl')}.jsonl"
    print(markdown(Run(path)))


def cmd_history(args: argparse.Namespace) -> None:
    """Prints the history of runs."""
    print(history(runs(args.workload, args.label), args.checkpoint))


def main() -> None:
    """Parses the command line and runs the command."""
    p = argparse.ArgumentParser(
        prog="timebench", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = p.add_subparsers(dest="command", required=True)

    commands.add_parser("list", help="workloads, scales, systems and readers").set_defaults(func=cmd_list)

    r = commands.add_parser("run", help="run one workload")
    r.add_argument("workload")
    r.add_argument("--scale", help="default: the workload's default scale")
    r.add_argument("--systems", help="comma-separated (default: every lake system)")
    r.add_argument("--readers", help="comma-separated (default: every reader of each system)")
    r.add_argument("--target", default="local", help="local, s3 (RustFS via the proxy), or an AWS target name")
    r.add_argument(
        "--checkpoints",
        help="comma-separated: the workload's (ingested, or e.g. 1d, 7d, ingested) and maintained "
        "(default: all; empty: ingest only)",
    )
    r.add_argument("--queries", help="comma-separated query names (default: all)")
    r.add_argument("--runs", type=int, default=2, help="repeat runs per query after the first run")
    r.add_argument("--timeout", type=int, default=600, help="seconds per query, all runs")
    r.add_argument("--reference-timeout", type=int, default=1800, help="--timeout for the reference system")
    r.add_argument(
        "--skip-after-timeout",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="skip a query at a system's later checkpoints once it timed out there",
    )
    r.add_argument("--ttfb-ms", type=int, help="latency the proxy adds per request (local object stores)")
    r.add_argument("--mibps", type=int, help="throughput cap per response in MiB/s the proxy applies, 0 for none")
    r.add_argument("--param", action="append", default=[], help="workload parameter key=value (repeatable)")
    r.add_argument("--label", default="", help="free text recorded with the run")
    r.add_argument("--variant", default="", help="names a kind of run kept apart from the others, e.g. bulk")
    r.add_argument("--reuse", action="store_true", help="query the tables an earlier run left (no ingest)")
    r.add_argument(
        "--live-interval", type=float, default=0, help="seconds between probe rounds during ingest (0 = no probes)"
    )
    r.add_argument("--concurrency", help="client counts for concurrent dashboards, e.g. 1,4,16 (default: none)")
    r.add_argument("--concurrency-seconds", type=float, default=30, help="how long each client count runs")
    r.add_argument("--pace", type=float, default=0, help="at least this many seconds between arrivals (0 = no pacing)")
    r.set_defaults(func=cmd_run)

    pr = commands.add_parser("profile", help="run a set of runs (profiles/<name>.toml)")
    pr.add_argument("name")
    pr.add_argument("--target", help="only the entries with this target")
    pr.set_defaults(func=cmd_profile)

    rep = commands.add_parser("report", help="one run as Markdown")
    rep.add_argument("run", help="a run id, or latest")
    rep.set_defaults(func=cmd_report)

    h = commands.add_parser("history", help="first-run geometric means of every run")
    h.add_argument("--workload")
    h.add_argument("--label")
    h.add_argument("--checkpoint", default="maintained")
    h.set_defaults(func=cmd_history)

    args = p.parse_args()
    args.func(args)
