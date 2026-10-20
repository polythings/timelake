"""The IIoT workload (generator, stages, queries) and the harness pieces it added: relative times, answer checks."""

from __future__ import annotations

import datetime as dt
import decimal
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timebench import harness, targets, workloads
from timebench.queries import DUCKDB, apply_dialect
from timebench.results import QueryRecord, read_jsonl
from timebench.systems import DIALECTS, arrow_result, column_sum, rows_result
from timebench.systems.clickhouse import ch_type
from timebench.systems.influxdb3 import line_protocol
from timebench.systems.questdb import qdb_type
from timebench.targets import Target
from timebench.workloads import TableSpec
from timebench.workloads.iiot import Iiot
from timebench.workloads.plant import DAY, START, TAGS_PER_MACHINE, generate, plan

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Generated workloads, run files and tables under a temporary directory."""
    monkeypatch.setattr(workloads, "DATA", tmp_path / "data")
    monkeypatch.setattr(targets, "LAKES", tmp_path / "lakes")
    monkeypatch.setenv("BENCH_LAKES", str(tmp_path / "lakes"))  # for the query processes
    monkeypatch.setattr(harness, "RUNS", tmp_path / "runs")
    return tmp_path


def test_ago_is_a_time_before_now() -> None:
    now = START + 3600
    assert apply_dialect("time >= @AGO(15 minutes)", DUCKDB, now) == "time >= TIMESTAMPTZ '2026-01-01 00:45:00'"
    assert apply_dialect("@AGO(1 day)", DUCKDB, now) == "TIMESTAMPTZ '2025-12-31 01:00:00'"
    with pytest.raises(ValueError, match="now"):
        apply_dialect("@AGO(1 hour)", DUCKDB)


def test_a_reader_gets_its_own_version_of_a_query(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workloads, "raw_queries", lambda _: iter([("q", "SELECT 1"), ("q [clickhouse]", "SELECT 2")]))
    [query] = Iiot().queries()
    assert query.sql_for("duckdb") == "SELECT 1"
    assert query.sql_for("clickhouse") == "SELECT 2"


def test_a_tuned_run_reads_the_tuned_version(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = iter([("q", "SELECT 1"), ("q [timelake tuned]", "SELECT 2")])
    monkeypatch.setattr(workloads, "raw_queries", lambda _: rows)
    [query] = Iiot().queries()
    assert query.sql_for("timelake", tuning="tuned") == "SELECT 2"
    assert query.sql_for("timelake", tuning="default") == "SELECT 1"
    assert query.sql_for("duckdb", tuning="tuned") == "SELECT 1"


def test_tuning_statements() -> None:
    [rollup] = Iiot().tuning_statements("timelake")
    assert rollup.startswith("CREATE MATERIALIZED ROLLUP readings_1h"), rollup
    assert Iiot().tuning_statements("iceberg") == []


@pytest.mark.parametrize("reader", sorted(set(DIALECTS) - {"polars"}))
def test_every_iiot_query_is_translated(reader: str) -> None:
    for query in Iiot().queries():
        assert "@" not in query.sql_for(reader, START + 30 * DAY), query.name


def record(rows: int, sums: list[float | None] | None) -> QueryRecord:
    r: QueryRecord = {"system": "s", "reader": "r", "checkpoint": "c", "query": "q", "status": "ok", "rows": rows}
    if sums is not None:
        r["sums"] = sums
    return r


def test_answers_match_on_rows_and_column_sums() -> None:
    expected = record(3, [None, 10.0, 1767225600.0])
    assert harness.same_answer(record(3, [None, 10.0 + 1e-9, 1767225600.0]), expected)
    assert harness.same_answer(record(4, [None, 10.0, 1767225600.0]), expected) is False
    assert harness.same_answer(record(3, [None, 10.5, 1767225600.0]), expected) is False
    assert harness.same_answer(record(0, []), record(0, [0.0, 0.0])), "empty results agree whatever their columns"
    assert harness.same_answer(record(3, None), expected) is None, "a reader without sums can't be checked"


def test_arrow_and_row_results_sum_the_same() -> None:
    times = [dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(hours=h) for h in range(5000)]
    table = pa.table(
        {
            "tag": ["a"] * 5000,
            "time": pa.array(times, pa.timestamp("ns", tz="UTC")),  # the sum in nanoseconds overflows 64 bits
            "day": pa.array([t.date() for t in times], pa.date32()),
            "day64": pa.array([t.date() for t in times], pa.date64()),
            "n": pa.array(range(5000), pa.int64()),
            "ok": [True, False] * 2500,
        }
    )
    rows = [(r["tag"], r["time"], r["day"], r["day64"], decimal.Decimal(r["n"]), r["ok"]) for r in table.to_pylist()]
    from_arrow, from_rows = arrow_result(table), rows_result(rows)
    assert from_arrow.rows == from_rows.rows == 5000
    assert from_arrow.sums == pytest.approx(from_rows.sums)
    assert from_arrow.sums is not None
    assert from_arrow.sums[0] is None
    assert from_arrow.sums[1] == pytest.approx(sum(t.timestamp() for t in times))
    assert column_sum(pa.chunked_array([pa.array(["x"])])) is None


def test_the_generator_is_deterministic_and_delivers_at_least_once(tmp_path: Path) -> None:
    one, two = tmp_path / "one", tmp_path / "two"
    one.mkdir()
    two.mkdir()
    counts = generate(one, 12, 2, seed=1)
    assert generate(two, 12, 2, seed=1) == counts
    files = sorted(p.relative_to(one) for p in one.rglob("*.parquet"))
    assert files == sorted(p.relative_to(two) for p in two.rglob("*.parquet"))
    assert all(pq.read_table(one / f).equals(pq.read_table(two / f)) for f in files)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    readings = f"read_parquet('{one}/readings/*/*.parquet')"
    total, unique = con.execute(f"SELECT count(*), count(DISTINCT (tag, time)) FROM {readings}").fetchone() or (0, 0)
    assert total == counts["copies"]
    assert unique == counts["rows"]
    assert 0.01 < total / unique - 1 < 0.03, "about 1.5% of rows arrive twice"
    # Each arrival hour's rows arrived in that hour's 5-minute windows.
    for directory in (one / "readings").iterdir():
        windows = pq.read_table(directory, columns=["window"])["window"].to_pylist()
        assert {w // 12 for w in windows} == {int(directory.name[1:])}
    tags = pq.read_table(one / "tags.parquet")
    assert tags.num_rows == 12 * TAGS_PER_MACHINE
    assert set(tags["line"].to_pylist()) == {"L0000", "L0001"}


def test_every_tenth_gateway_is_wired() -> None:
    p = plan(machines=300, days=30, seed=1)
    assert len(p.outages.owner) > 0
    assert all(g % 10 != 0 for g in p.outages.owner.tolist())


@pytest.mark.usefixtures("data_dir")
def test_iiot_stages_load_history_in_bulk_then_a_live_day() -> None:
    workload = Iiot("small", machines=2, days=9)
    workload.prepare()
    stages = workload.stages()
    assert [s.name for s in stages] == ["history-1d", "history-7d", "history", "live"]
    assert workload.checkpoints() == ["1d", "7d", "ingested"]
    assert [s.now for s in stages] == [START + d * DAY for d in (1, 7, 8, 9)]
    assert not stages[0].live
    assert stages[-1].live
    live = list(stages[-1].arrivals)
    assert len(live) == 288, "a commit per 5-minute window"
    history = list(stages[1].arrivals)
    assert len(history) == 6 * 24, "a commit per hour"


@pytest.mark.usefixtures("data_dir")
def test_answers_are_checked_against_the_reference() -> None:
    spec = harness.RunSpec(
        workload=Iiot("small", machines=8, days=9),  # trend-30d reads machine 7
        target=Target("local"),
        systems=["reference", "parquet"],
        readers=["duckdb"],
        queries={"trend-30d", "latest-area"},
        runs=0,
    )
    records = read_jsonl(harness.run(spec))
    queries = [r for r in records if r["kind"] == "query"]
    assert {q["checkpoint"] for q in queries} == {"1d", "7d", "ingested", "maintained"}
    assert all(q["status"] == "ok" for q in queries), [q.get("error") for q in queries]
    assert all("correct" not in q for q in queries if q["system"] == "reference")
    parquet = {(q["checkpoint"], q["query"]): q["correct"] for q in queries if q["system"] == "parquet"}
    # Plain Parquet keeps the rows that arrived twice: sample counts are off.
    assert not parquet[("ingested", "trend-30d")]
    assert not parquet[("maintained", "trend-30d")]
    assert records[-1]["wrong"]
    ingest = next(r for r in records if r["kind"] == "ingest" and r["system"] == "parquet")
    assert list(ingest["stages"]) == ["history-1d", "history-7d", "history", "live"]


def test_line_protocol() -> None:
    spec = TableSpec("readings", "time", ("tag",), dedup=True)
    data = pa.table(
        {
            "time": pa.array([START * 1_000_000, START * 1_000_000 + 1], pa.timestamp("us", tz="UTC")),
            "tag": ["M00001.pressure", "a b,c=d"],
            "value": [6.25, None],
            "quality": pa.array([192, 64], pa.int16()),
        }
    )
    assert line_protocol("readings", data, spec).to_pylist() == [
        f"readings,tag=M00001.pressure value=6.25,quality=192i {START * 1_000_000}",
        f"readings,tag=a\\ b\\,c\\=d quality=64i {START * 1_000_000 + 1}",
    ]
    # A dimension table: its first column is the tag (one series per row), the rest fields, at the epoch.
    dims = pa.table({"tag": ["t"], "unit": ['say "hi"'], "hi_limit": pa.array([None], pa.float64())})
    assert line_protocol("tags", dims, TableSpec("tags")).to_pylist() == ['tags,tag=t unit="say \\"hi\\"" 0']


def test_server_types() -> None:
    spec = TableSpec("readings", "time", ("tag",), dedup=True)
    assert ch_type(pa.field("tag", pa.string()), spec, "time") == "LowCardinality(String)"
    assert ch_type(pa.field("time", pa.timestamp("us", tz="UTC")), spec, "time") == "DateTime64(6, 'UTC')"
    assert ch_type(pa.field("value", pa.float64()), spec, "time") == "Nullable(Float64)"
    # A dimension table: its first column orders it, the rest may be null.
    assert ch_type(pa.field("tag", pa.string()), TableSpec("tags"), "tag") == "String"
    assert ch_type(pa.field("hi", pa.float64()), TableSpec("tags"), "tag") == "Nullable(Float64)"
    assert qdb_type(pa.field("tag", pa.string()), spec) == "SYMBOL"
    assert qdb_type(pa.field("quality", pa.int16()), spec) == "SHORT"
