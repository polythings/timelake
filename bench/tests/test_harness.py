"""The benchmark harness end to end on a tiny workload, on the local disk (no network, no generator)."""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING, ClassVar, override

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from timebench import harness, targets
from timebench.cost import PRICES, phase_cost, query_cost, requests_cost, storage_month
from timebench.live import Commit, Prober, lag
from timebench.queries import apply_dialect
from timebench.report import Run, markdown
from timebench.results import QueryRecord, append_jsonl, read_jsonl
from timebench.s3 import PROBE_PROXY, PROXY
from timebench.systems import DIALECTS, System
from timebench.systems.iceberg import Iceberg
from timebench.systems.timescaledb import csv_chunks, pg_type
from timebench.targets import Place, Target
from timebench.workloads import Batch, Query, TableSpec, Workload
from timebench.workloads.tsbs import stream_windows, windows

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator
    from pathlib import Path

START = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
SCHEMA = pa.schema([("time", pa.timestamp("us", tz="UTC")), ("device", pa.int32()), ("value", pa.float64())])


class Tiny(Workload):
    """Two devices, one reading a minute for two days, arriving an hour at a time."""

    name = "tiny"
    title = "Tiny"
    scales: ClassVar[dict[str, str]] = {"one": "2 devices, 2 days"}
    default_scale = "one"
    namespace = "tiny"
    tables = (TableSpec("devices"), TableSpec("readings", "time", ("device",)))
    suites = ()
    dashboard_suite = "tiny"
    live_table = "readings"
    span_days = 2
    hours = 48
    arrival_pause = 0.0
    """Seconds reading each arrival takes, so probes have time to run."""

    @override
    def prepare(self) -> None:
        pass

    @override
    def schema(self, table: str) -> pa.Schema:
        return pa.schema([("id", pa.int32()), ("name", pa.string())]) if table == "devices" else SCHEMA

    @override
    def initial(self) -> Iterator[Batch]:
        yield "devices", pa.table({"id": pa.array([1, 2], pa.int32()), "name": ["a", "b"]})

    @override
    def stream(self) -> Iterator[list[Batch]]:
        for hour in range(self.hours):
            time.sleep(self.arrival_pause)
            times = [START + dt.timedelta(hours=hour, minutes=m) for m in range(60)]
            yield [
                (
                    "readings",
                    pa.table(
                        {
                            "time": pa.array(times * 2, pa.timestamp("us", tz="UTC")),
                            "device": pa.array([1] * 60 + [2] * 60, pa.int32()),
                            "value": [float(hour)] * 120,
                        },
                        schema=SCHEMA,
                    ),
                )
            ]

    @override
    def queries(self) -> list[Query]:
        return [
            Query("tiny", "count", "SELECT count(*) AS n FROM readings"),
            Query(
                "tiny",
                "per-device",
                "SELECT d.name, avg(r.value) FROM readings r JOIN devices d ON d.id = r.device GROUP BY d.name",
            ),
            Query("tiny", "last-hour", "SELECT * FROM readings WHERE time >= @TS('2026-01-02 23:00:00')"),
        ]


@pytest.fixture
def bench_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run files and tables under a temporary directory."""
    monkeypatch.setattr(targets, "LAKES", tmp_path / "lakes")
    monkeypatch.setenv("BENCH_LAKES", str(tmp_path / "lakes"))  # for the query processes
    monkeypatch.setattr(harness, "RUNS", tmp_path / "runs")
    return tmp_path


@pytest.mark.usefixtures("bench_dirs")
@pytest.mark.parametrize("system", ["timelake", "iceberg", "lance", "parquet"])
def test_a_run_ingests_maintains_and_queries(system: str) -> None:
    spec = harness.RunSpec(
        workload=Tiny(), target=Target("local"), systems=[system], readers=["duckdb", "datafusion"], runs=1
    )
    records = read_jsonl(harness.run(spec))
    kinds = [r["kind"] for r in records]
    assert kinds[0] == "run"
    assert kinds[-1] == "end"
    header = records[0]
    assert header["workload"] == "tiny"
    assert header["target"] == "local"
    assert len(header["commit"]) == 40
    ingest = next(r for r in records if r["kind"] == "ingest")
    assert ingest["status"] == "ok", ingest
    assert ingest["commits"] == 48
    assert ingest["rows"] == 48 * 120
    assert next(r for r in records if r["kind"] == "maintain")["status"] == "ok"
    queries = [r for r in records if r["kind"] == "query"]
    assert {q["checkpoint"] for q in queries} == {"ingested", "maintained"}
    assert all(q["status"] == "ok" for q in queries), [q.get("error") for q in queries]
    counts = {(q["checkpoint"], q["query"], q["reader"]): q["rows"] for q in queries}
    assert all(n == 120 for (_, name, _), n in counts.items() if name == "last-hour")
    assert records[-1]["mismatches"] == []
    report = markdown(Run(harness.RUNS / f"{header['id']}.jsonl"))
    assert "## Queries, maintained" in report


@pytest.mark.usefixtures("bench_dirs")
@pytest.mark.parametrize("skip", [True, False])
def test_a_timed_out_query_is_skipped_at_later_checkpoints(skip: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    measured: list[tuple[str, str]] = []

    def measure(_spec: harness.RunSpec, _system: object, reader: str, query: Query, at: harness.At) -> QueryRecord:
        measured.append((at.checkpoint, query.name))
        r = harness.query_record("parquet", reader, query, at)
        r.update(status="timeout" if query.name == "count" else "ok", times=[0.1], rows=1)
        return r

    monkeypatch.setattr(harness, "measure", measure)
    spec = harness.RunSpec(
        workload=Tiny(), target=Target("local"), systems=["parquet"], readers=["duckdb"], skip_after_timeout=skip
    )
    path = harness.RUNS / "run.jsonl"
    earlier = harness.query_pass(spec, path, ["parquet"], harness.At("ingested", None))
    later = harness.query_pass(spec, path, ["parquet"], harness.At("maintained", None), earlier)
    count = next(r for r in later if r["query"] == "count")
    if skip:
        assert count["status"] == "skipped"
        assert count["error"] == "timed out at ingested"
        assert ("maintained", "count") not in measured
    else:
        assert count["status"] == "timeout"
        assert ("maintained", "count") in measured
    assert all(r["status"] == "ok" for r in later if r["query"] != "count")


@pytest.mark.usefixtures("bench_dirs")
def test_a_reuse_run_starts_each_server_and_skips_after_a_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Querying the tables an earlier run left needs the same servers, and a timeout carries across checkpoints."""
    measured: list[tuple[str, str]] = []
    served: list[str] = []

    def measure(_spec: harness.RunSpec, _system: object, reader: str, query: Query, at: harness.At) -> QueryRecord:
        measured.append((at.checkpoint, query.name))
        r = harness.query_record("parquet", reader, query, at)
        r.update(status="timeout" if query.name == "count" else "ok", times=[0.1], rows=1)
        return r

    @contextlib.contextmanager
    def server(system: System, _path: Path) -> Generator[bool]:
        served.append(system.name)
        yield True

    monkeypatch.setattr(harness, "measure", measure)
    monkeypatch.setattr(harness, "server", server)
    spec = harness.RunSpec(
        workload=Tiny(),
        target=Target("local"),
        systems=["parquet"],
        readers=["duckdb"],
        checkpoints=("ingested", "maintained"),
        reuse=True,
    )
    records = read_jsonl(harness.run(spec))
    assert served == ["parquet"]
    assert ("maintained", "count") not in measured
    count = next(r for r in records if r.get("query") == "count" and r["checkpoint"] == "maintained")
    assert count["status"] == "skipped"


@pytest.mark.usefixtures("bench_dirs")
def test_iceberg_maintenance_rewrites_each_day_into_one_file() -> None:
    start = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    data = pa.table(
        {
            "time": pa.array([start + dt.timedelta(hours=h) for h in range(60)], pa.timestamp("us", tz="UTC")),
            "v": range(60),
        }
    )
    place = Place(Target("local"), "days", "iceberg")
    place.clear()
    iceberg = Iceberg(place, "bench")
    iceberg.create(TableSpec("x", "time", ()), data.schema)
    for i in range(0, 60, 6):
        iceberg.append("x", data.slice(i, 6))
    iceberg.maintain()
    table = iceberg.catalog.load_table("bench.x")
    assert table.inspect.partitions()["file_count"].to_pylist() == [1, 1, 1]
    assert sorted(table.scan().to_arrow()["v"].to_pylist()) == list(range(60))


@pytest.mark.usefixtures("bench_dirs")
def test_parquet_files_go_to_the_day_of_their_time_in_any_unit() -> None:
    from timebench.systems.parquet import ParquetFiles

    place = Place(Target("local"), "days", "parquet")
    place.clear()
    parquet = ParquetFiles(place, "bench")
    parquet.create(TableSpec("x", "time", ()), pa.schema([("time", pa.timestamp("ns", tz="UTC"))]))
    times = [START + dt.timedelta(hours=h) for h in (23, 25)]
    parquet.append("x", pa.table({"time": pa.array(times, pa.timestamp("ns", tz="UTC"))}))
    assert sorted(d.name for d in (place.local_dir / "lake" / "x").iterdir()) == ["day=2026-01-01", "day=2026-01-02"]


@pytest.mark.usefixtures("bench_dirs")
def test_questdb_maintenance_closes_the_idle_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    from timebench.systems.questdb import QuestDB

    class Connection:
        closed = False

        def close(self) -> None:
            self.closed = True

    questdb = QuestDB(Place(Target("local"), "tiny", "questdb"), "tiny")
    monkeypatch.setattr(questdb, "execute", lambda *_: [(0,)])
    idle = Connection()
    monkeypatch.setattr(questdb, "_conn", idle)  # the connection ingest left open
    questdb.maintain()
    assert idle.closed


def test_mismatches_name_the_readers_that_disagree() -> None:
    def record(system: str, rows: int) -> QueryRecord:
        return {
            "system": system,
            "reader": "duckdb",
            "checkpoint": "ingested",
            "query": "q",
            "status": "ok",
            "rows": rows,
        }

    assert harness.mismatches([record("a", 1), record("b", 1)]) == []
    assert harness.mismatches([record("a", 1), record("b", 2)]) == ["ingested/q: a+duckdb=1, b+duckdb=2"]


def test_pg_types() -> None:
    assert pg_type(pa.timestamp("us", tz="UTC")) == "timestamptz"
    assert pg_type(pa.int32()) == "integer"
    with pytest.raises(TypeError):
        pg_type(pa.list_(pa.int32()))


def test_csv_for_copy_round_trips() -> None:
    """NULL vs empty string, and a comma inside a value, survive the CSV that COPY reads."""
    table = pa.table(
        {
            "time": pa.array([START] * 3, pa.timestamp("us", tz="UTC")),
            "name": ["", "b,c", None],
            "value": [1.5, None, 3.0],
        }
    )
    csv = b"".join(csv_chunks(table, batch_rows=2))
    # As PostgreSQL reads CSV: an unquoted empty field is NULL, a quoted one an empty string.
    read = pacsv.ReadOptions(column_names=table.column_names)
    convert = pacsv.ConvertOptions(strings_can_be_null=True, quoted_strings_can_be_null=False)
    assert pacsv.read_csv(pa.py_buffer(csv), read_options=read, convert_options=convert).cast(table.schema) == table


@pytest.mark.usefixtures("bench_dirs")
def test_probes_during_ingest_and_concurrent_dashboards() -> None:
    workload = Tiny()
    workload.arrival_pause = 0.1  # about 5 s of ingest
    spec = harness.RunSpec(
        workload=workload,
        target=Target("local"),
        systems=["parquet"],
        checkpoints=("maintained",),
        runs=0,
        live_interval=0.5,
        concurrency=(1, 2),
        concurrency_seconds=1,
    )
    records = read_jsonl(harness.run(spec))
    assert records[0]["live_interval_s"] == 0.5
    assert next(r for r in records if r["kind"] == "ingest")["probed"]
    live = next(r for r in records if r["kind"] == "live")
    assert live["rounds"] >= 1
    assert live["lag_s"], live["errors"]
    assert all(x >= 0 for x in live["lag_s"])
    assert not live["errors"]
    concurrency = [r for r in records if r["kind"] == "concurrency"]
    assert [c["clients"] for c in concurrency] == [1, 2]
    assert all(c["queries"] > 0 and c["errors"] == 0 and c["p50_s"] > 0 for c in concurrency), concurrency


@pytest.mark.parametrize("reader", ["duckdb", "datafusion", "postgres"])
def test_the_freshness_probe_is_fully_translated(reader: str) -> None:
    probe = Tiny().freshness_query()
    assert probe is not None
    assert "@" not in apply_dialect(probe.sql, DIALECTS[reader])


def test_lag_counts_from_the_oldest_commit_the_reader_missed() -> None:
    commits = [Commit(10.0, 100), Commit(11.0, 200), Commit(12.0, 300), Commit(20.0, 400)]
    assert lag(commits, 15.0, 300) == 0.0  # saw everything finished before the probe
    assert lag(commits, 15.0, 200) == 3.0  # missed the commit that finished at 12
    assert lag(commits, 15.0, 100) == 4.0
    assert lag(commits, 15.0, None) == 5.0  # an empty table: missed them all


def test_costs_at_list_prices() -> None:
    s3 = PRICES["aws-s3"]
    assert requests_cost({"PUT": 1000, "GET": 1000, "DELETE": 5, "HTTP 404": 3}, 0, s3) == pytest.approx(0.0054)
    first: QueryRecord = {"kinds": [{"GET": 10, "HEAD": 2}], "bytes": [1000], "requests": [12]}
    assert query_cost(first, "aws-s3") == pytest.approx(12 * 0.0004 / 1000)
    # Counts over all runs, scaled to the first run's request count.
    split: QueryRecord = {"kinds_total": {"GET": 30, "LIST": 10}, "bytes": [0], "requests": [4]}
    assert query_cost(split, "aws-s3") == pytest.approx(3 * 0.0004 / 1000 + 1 * 0.005 / 1000)
    assert query_cost(first, "local") is None
    assert phase_cost({"kind": "maintain", "kinds": {"PUT": 2000}}, "aws-s3") == pytest.approx(0.01)
    assert storage_month(1024**3, "aws-s3") == pytest.approx(0.023)


def test_the_report_prices_only_what_the_run_priced(tmp_path: Path) -> None:
    """A database server's disk or S3 Tables isn't billed at S3 Standard's storage price."""
    path = tmp_path / "run.jsonl"
    header = {"kind": "run", "id": "r", "workload": "tiny", "scale": "one", "target": "aws-s3", "params": {}}
    append_jsonl(path, {**header, "commit": "0" * 40, "machine": "m", "started": "2026-10-20T00:00:00+00:00"})
    append_jsonl(path, {"kind": "ingest", "system": "questdb", "status": "ok", "bytes": 10 * 1024**3})
    append_jsonl(path, {"kind": "ingest", "system": "parquet", "status": "ok", "bytes": 1024**3})
    append_jsonl(path, {"kind": "maintain", "system": "parquet", "status": "ok", "storage_usd_month": 0.023})
    report = markdown(Run(path))
    assert "| parquet |  |  | $0.0230 |" in report
    assert "| questdb |" not in report.split("## Cost")[1]


@pytest.mark.parametrize("ordered", [True, False])
def test_streamed_windows_match_the_in_memory_split(tmp_path: Path, *, ordered: bool) -> None:
    """Windows read batch by batch from a time-ordered file are the ones ``windows`` makes; out of order, the same."""
    minutes = [0, 1, 2, 7, 8, 13, 14, 15, 29, 31] * 3
    if ordered:
        minutes.sort()
    times = [START + dt.timedelta(minutes=m) for m in minutes]
    table = pa.table({"time": pa.array(times, pa.timestamp("us", tz="UTC")), "v": list(range(len(times)))})
    path = tmp_path / "t.parquet"
    pq.write_table(table, path, row_group_size=4)  # windows span row groups
    expected = windows(table, 5)
    assert [(w, t.to_pylist()) for w, t in stream_windows(str(path), 5)] == [(w, t.to_pylist()) for w, t in expected]


def test_tsbs_arrivals_hold_each_window_of_every_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A table without rows in a window doesn't shift the other tables' windows into the wrong arrival."""
    from timebench import workloads
    from timebench.workloads.tsbs import TsbsIot

    monkeypatch.setattr(workloads, "DATA", tmp_path)
    iot = TsbsIot("small")
    iot.data_dir.mkdir(parents=True)
    for table, minutes in (("readings", [0, 5, 15]), ("diagnostics", [1, 10, 16])):
        times = pa.array([START + dt.timedelta(minutes=m) for m in minutes], pa.timestamp("us", tz="UTC"))
        pq.write_table(pa.table({"time": times, "tags_id": [1, 2, 3]}), iot.path(table))
    arrivals = [[(name, data["tags_id"].to_pylist()) for name, data in arrival] for arrival in iot.stream()]
    assert arrivals == [
        [("readings", [1]), ("diagnostics", [1])],
        [("readings", [2])],
        [("diagnostics", [2])],
        [("readings", [3]), ("diagnostics", [3])],
    ]


@pytest.mark.usefixtures("bench_dirs")
def test_ingest_seconds_leave_out_resetting_the_last_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """How long deleting the previous run's tables takes depends on what that run left, not on this system."""
    spec = harness.RunSpec(workload=Tiny(), target=Target("local"), systems=["parquet"], checkpoints=())
    system = spec.system("parquet")
    reset = system.reset

    def slow_reset() -> None:
        time.sleep(2)
        reset()

    monkeypatch.setattr(system, "reset", slow_reset)
    done, _ = harness.ingest(spec, system, lambda _: None)
    assert done["status"] == "ok", done
    assert done["seconds"] < 2


@pytest.mark.usefixtures("bench_dirs")
def test_ingest_seconds_leave_out_reading_the_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loading and splitting the generated files is the harness's work, not the system's."""
    workload = Tiny()
    workload.arrival_pause = 0.05  # 2.4 s over 48 arrivals
    initial = workload.initial

    def slow_initial() -> Iterator[Batch]:
        time.sleep(1)
        yield from initial()

    monkeypatch.setattr(workload, "initial", slow_initial)
    spec = harness.RunSpec(workload=workload, target=Target("local"), systems=["parquet"], checkpoints=())
    done, _ = harness.ingest(spec, spec.system("parquet"), lambda _: None)
    assert done["status"] == "ok", done
    assert done["seconds"] < 1.5


@pytest.mark.usefixtures("bench_dirs")
def test_paced_arrivals_read_their_data_while_they_wait() -> None:
    """With ``--pace``, arrivals are ``pace`` seconds apart; reading the next one fits in that wait."""
    workload = Tiny()
    workload.hours, workload.arrival_pause = 6, 0.1
    spec = harness.RunSpec(workload=workload, target=Target("local"), systems=["parquet"], checkpoints=(), pace=0.2)
    done, _ = harness.ingest(spec, spec.system("parquet"), lambda _: None)
    assert done["status"] == "ok", done
    assert 0.95 <= done["seconds"] < 1.4  # 5 gaps of 0.2 s between the 6 arrivals, and their commits


class FakeProxy:
    """Counters like the proxy's /stats, bumped by hand."""

    def __init__(self) -> None:
        self.requests = 0

    def stats(self) -> dict[str, object]:
        return {"requests": self.requests, "bytes": 0, "kinds": {"GET": self.requests}}


@pytest.mark.usefixtures("bench_dirs")
def test_ingest_requests_leave_out_the_checkpoint_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy = FakeProxy()
    monkeypatch.setattr(harness, "PROXY", proxy)
    monkeypatch.setattr(harness, "counted", lambda _: True)
    spec = harness.RunSpec(workload=Tiny(), target=Target("local"), systems=["parquet"])
    system = spec.system("parquet")
    append = system.append

    def counted_append(table: str, data: pa.Table) -> None:
        proxy.requests += 1
        append(table, data)

    def queries(_stage: object) -> None:
        proxy.requests += 1000

    monkeypatch.setattr(system, "append", counted_append)
    done, _ = harness.ingest(spec, system, queries)
    assert done["status"] == "ok", done
    assert done["requests"] == 1 + 48  # the devices, then each arrival
    assert done["kinds"] == {"GET": 49}


@pytest.mark.usefixtures("bench_dirs")
def test_maintenance_requests_leave_out_reading_the_state_and_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Listing the tables or loading their metadata afterwards is the harness's bookkeeping, not maintenance."""
    proxy = FakeProxy()
    monkeypatch.setattr(harness, "PROXY", proxy)
    monkeypatch.setattr(harness, "counted", lambda _: True)
    spec = harness.RunSpec(workload=Tiny(), target=Target("local"), systems=["parquet"], checkpoints=())
    system = spec.system("parquet")
    harness.ingest(spec, system, lambda _: None)
    for method, requests in (("maintain", 1), ("state", 100), ("stats", 10_000)):
        original = getattr(system, method)

        def bumped(original: Callable[[], object] = original, requests: int = requests) -> object:
            proxy.requests += requests
            return original()

        monkeypatch.setattr(system, method, bumped)
    done = harness.maintain(system)
    assert done["status"] == "ok", done
    assert done["requests"] == 1


@pytest.mark.usefixtures("bench_dirs")
def test_ingest_requests_leave_out_saving_the_state_for_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy = FakeProxy()
    monkeypatch.setattr(harness, "PROXY", proxy)
    monkeypatch.setattr(harness, "counted", lambda _: True)

    class NoProbes:
        def __init__(self, *_: object) -> None:
            self.thread = threading.Thread()

        def start(self) -> None:
            pass

        def committed(self, _newest: int) -> None:
            pass

        def finish(self) -> None:
            return None

    monkeypatch.setattr(harness, "Prober", NoProbes)
    spec = harness.RunSpec(workload=Tiny(), target=Target("local"), systems=["parquet"], live_interval=1)
    system = spec.system("parquet")
    state = system.state

    def counted_state() -> dict[str, object]:
        proxy.requests += 100
        return state()

    monkeypatch.setattr(system, "state", counted_state)
    done, _ = harness.ingest(spec, system, lambda _: None)
    assert done["status"] == "ok", done
    assert done.get("requests", 0) == 0


def test_probe_readers_go_through_the_uncounted_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probes run while a system ingests; through the counting listener, their requests would count as ingest's."""
    envs: list[dict[str, str]] = []

    def run(*_args: object, env: dict[str, str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        envs.append(env)
        return subprocess.CompletedProcess([], 0, '{"times": [0.1], "value": 1}', "")

    spec = harness.RunSpec(workload=Tiny(), target=Target("s3"), systems=["parquet"])
    prober = Prober(spec.workload, spec.system("parquet"), 1)
    with monkeypatch.context() as m:
        m.setattr(subprocess, "run", run)
        prober.probe(spec.workload.dashboard_queries()[0])
    assert len(envs) == 1
    # What the probe's worker process connects to.
    address = subprocess.check_output(
        [sys.executable, "-c", "from timebench.s3 import PROXY; print(PROXY.address)"], env=envs[0], text=True
    )
    assert address.strip() == PROBE_PROXY.address != PROXY.address


@pytest.mark.usefixtures("bench_dirs")
def test_s3_tables_maintenance_fails_when_compaction_never_ran() -> None:
    """Otherwise the run records maintenance as ok and queries uncompacted tables as ``maintained``."""
    from timebench.systems.iceberg import S3Tables

    class NoRuns:
        def get_table_maintenance_job_status(self, **_: object) -> dict[str, object]:
            return {"status": {"icebergCompaction": {"status": "Not_Yet_Run"}}}

    s3tables = object.__new__(S3Tables)
    s3tables.place = Place(Target("aws-s3"), "days", "s3tables")
    s3tables.place.save_state({"ingested_at": harness.now()})
    s3tables.namespace, s3tables.arn = "bench", "arn:aws:s3tables:::bucket/bench"
    s3tables.client, s3tables.wait_s, s3tables.poll_seconds = NoRuns(), 0, 0
    s3tables.time_series, s3tables.compaction = ["readings"], {}
    with pytest.raises(TimeoutError, match="compaction"):
        s3tables.maintain()


FAKE_CLIENT = """
import json, sys, time
job = json.loads(sys.argv[1])
time.sleep(0.4 * job["offset"])  # opening the reader: slower for some clients
print("ready", flush=True)
start_at = float(sys.stdin.readline())
# A query that finished after the measured seconds: a latency, but not part of the throughput.
print(json.dumps({"setup_s": 0, "latencies": [["q", start_at], ["q", 0.1]], "errors": [], "finished": 1}))
"""


def test_concurrent_clients_start_together_once_all_are_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """qps counts every client's queries over the same seconds, so the clients must run at the same time."""
    from timebench import concurrency

    monkeypatch.setattr(concurrency, "command", lambda job: [sys.executable, "-c", FAKE_CLIENT, json.dumps(job)])
    started = time.time()
    record = concurrency.dashboards(
        harness.RunSpec(workload=Tiny(), target=Target("local"), systems=["parquet"]).system("parquet"),
        [{"name": "q", "sql": "SELECT 1"}],
        "ingested",
        3,
        1,
    )
    assert record["errors"] == 0, record.get("error")
    assert record["p95_s"] == record["p99_s"]  # each fake client reports the start it was given: the same one
    assert record["p95_s"] >= started + 0.8  # after the slowest client had opened its reader
    assert (record["queries"], record["qps"]) == (3, 3.0)


def test_a_client_counts_only_the_queries_that_finished_in_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """qps is queries per second over the measured seconds: one still running at the end isn't in them."""
    import io

    from timebench import worker

    class Fake:
        @classmethod
        def open_reader(cls, *_: object) -> object:
            return lambda _sql: time.sleep(0.2)

    monkeypatch.setattr(worker, "systems", lambda: {"fake": Fake})
    monkeypatch.setattr(Place, "load_state", lambda _: {})
    monkeypatch.setattr(sys, "stdin", io.StringIO(f"{time.time()}\n"))
    job = {
        "target": "local",
        "dataset": "d",
        "system": "fake",
        "reader": "r",
        "seconds": 0.3,
        "queries": [{"name": "q", "sql": "x"}],
    }
    out = worker.client(job)
    assert len(out["latencies"]) == 2  # started at 0 and 0.2 s
    assert out["finished"] == 1


def test_a_client_runs_its_queries_from_the_shared_start_for_the_given_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    import io

    from timebench import worker

    ran: list[float] = []

    class Fake:
        @classmethod
        def open_reader(cls, *_: object) -> object:
            return lambda _sql: (ran.append(time.time()), time.sleep(0.01))

    monkeypatch.setattr(worker, "systems", lambda: {"fake": Fake})
    monkeypatch.setattr(Place, "load_state", lambda _: {})
    start_at = time.time() + 0.5
    monkeypatch.setattr(sys, "stdin", io.StringIO(f"{start_at}\n"))
    job = {
        "target": "local",
        "dataset": "d",
        "system": "fake",
        "reader": "r",
        "seconds": 0.3,
        "queries": [{"name": "q", "sql": "x"}],
    }
    out = worker.client(job)
    assert ran
    assert min(ran) >= start_at
    assert max(ran) <= start_at + 0.3
    assert len(out["latencies"]) == len(ran)


@pytest.mark.usefixtures("bench_dirs")
def test_questdb_commits_reuse_one_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """A new client per commit puts a handshake in every commit's time, which the other databases don't pay."""
    import types

    from timebench.systems.questdb import QuestDB

    clients: list[dict[str, object]] = []

    class Client:
        def __init__(self) -> None:
            self.state: dict[str, object] = {"frames": 0, "closed": False}
            clients.append(self.state)

        def dataframe(self, *_: object, **__: object) -> None:
            self.state["frames"] = int(str(self.state["frames"])) + 1

        def close(self) -> None:
            self.state["closed"] = True

        def __enter__(self) -> Client:
            return self

        def __exit__(self, *_: object) -> None:
            self.close()

    monkeypatch.setitem(sys.modules, "questdb", types.SimpleNamespace(connect=lambda _conf: Client()))
    questdb = QuestDB(Place(Target("local"), "tiny", "questdb"), "tiny")
    monkeypatch.setattr(questdb, "execute", lambda *_: [(0,)])
    questdb.specs["readings"] = TableSpec("readings", "time", ("device",))
    data = Tiny().stream().__next__()[0][1]
    for _ in range(3):
        questdb.append("readings", data)
    questdb.settle()
    assert clients == [{"frames": 3, "closed": True}]


def test_an_s3_run_sets_and_records_every_proxy_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    """The proxy keeps what the last run set: a run that doesn't name a setting gets none, not the last run's."""
    settings: list[tuple[str, str]] = []
    monkeypatch.setattr(harness, "set_proxy", lambda setting, query: settings.append((setting, query)))
    spec = harness.RunSpec(workload=Tiny(), target=Target("s3"), systems=[], ttfb_ms=20)
    harness.configure_proxy(spec)
    assert settings == [("ttfb", "ms=20"), ("mibps", "v=0")]
    header = harness.run_header(spec, dt.datetime.now(dt.UTC))
    assert (header["ttfb_ms"], header["mibps"]) == (20, 0)


@pytest.mark.usefixtures("bench_dirs")
def test_timelake_reader_settings_reach_the_python_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    """Probes and dashboard clients read through the Python binding: an A/B arm's settings must apply there too."""
    from timebench.systems.timelake import Timelake

    place = Place(Target("local"), "tiny", "timelake")
    place.clear()
    system = Timelake(place, "tiny")
    system.create(TableSpec("readings", "time", ("device",), dedup=True), SCHEMA)
    batch = next(Tiny().stream())[0][1]
    system.append("readings", batch)
    system.append("readings", batch)  # arrives twice
    place.save_state(system.state())
    count = "SELECT count(*) FROM readings"
    assert Timelake.open_reader("timelake", place, place.load_state())(count).value == 120
    monkeypatch.setenv("TIMEBENCH_TIMELAKE_SETTINGS", "read_dedup=false")
    assert Timelake.open_reader("timelake", place, place.load_state())(count).value == 240
