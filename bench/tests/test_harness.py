"""The benchmark harness end to end on a tiny workload, on the local disk (no network, no generator)."""

from __future__ import annotations

import datetime as dt
import time
from typing import TYPE_CHECKING, ClassVar, override

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from timebench import harness, targets
from timebench.cost import PRICES, phase_cost, query_cost, requests_cost, storage_month
from timebench.live import Commit, lag
from timebench.queries import apply_dialect
from timebench.report import Run, markdown
from timebench.results import QueryRecord, read_jsonl
from timebench.systems import DIALECTS
from timebench.systems.iceberg import Iceberg
from timebench.systems.timescaledb import csv_chunks, pg_type
from timebench.targets import Place, Target
from timebench.workloads import Batch, Query, TableSpec, Workload
from timebench.workloads.tsbs import stream_windows, windows

if TYPE_CHECKING:
    from collections.abc import Iterator
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
    arrival_pause = 0.0
    """Seconds between arrivals, so probes have time to run."""

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
        for hour in range(48):
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
    assert [t.to_pylist() for t in stream_windows(str(path), 5)] == [t.to_pylist() for t in expected]


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
