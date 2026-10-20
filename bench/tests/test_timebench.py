"""The shared benchmark helpers."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import pytest
import tomllib

from timebench import BENCH, queries
from timebench.queries import (
    CLICKBENCH_COMMIT,
    DATAFUSION,
    DUCKDB,
    POLARS,
    TIMESCALEDB,
    Dialect,
    apply_dialect,
    clickbench_queries,
    raw_queries,
)
from timebench.results import append_jsonl, best_time, geomean, load_ok, order_engines, read_jsonl
from timebench.systems import TUNINGS, systems
from timebench.targets import Place, Target
from timebench.workloads import workloads

if TYPE_CHECKING:
    from pathlib import Path


def test_geomean() -> None:
    assert math.isclose(geomean([1.0, 4.0]), 2.0)


def test_geomean_clamps_tiny_times() -> None:
    assert math.isclose(geomean([0.0, 1e-4]), 1e-4)


def test_geomean_of_nothing_is_nan() -> None:
    assert math.isnan(geomean([]))


def test_best_time_skips_the_first_run() -> None:
    assert best_time({"times": [0.1, 0.5, 0.4]}) == 0.4


def test_best_time_of_a_single_run() -> None:
    assert best_time({"times": [0.3]}) == 0.3


def test_order_engines_puts_unknown_last() -> None:
    assert order_engines(["mystery", "v0-timelake", "duckdb"]) == ["duckdb", "v0-timelake", "mystery"]


def test_jsonl_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "out" / "r.jsonl"
    append_jsonl(path, {"a": 1})
    append_jsonl(path, {"a": 2})
    assert read_jsonl(path) == [{"a": 1}, {"a": 2}]


def test_read_jsonl_of_missing_file_is_empty(tmp_path: Path) -> None:
    assert read_jsonl(tmp_path / "missing.jsonl") == []


def test_load_ok_skips_failures(tmp_path: Path) -> None:
    path = tmp_path / "r.jsonl"
    append_jsonl(path, {"engine": "duckdb", "query": "q1", "status": "ok", "times": [1.0]})
    append_jsonl(path, {"engine": "duckdb", "query": "q2", "status": "timeout"})
    assert list(load_ok(path)["duckdb"]) == ["q1"]


def test_apply_dialect_duckdb() -> None:
    assert apply_dialect("SELECT @B1H(time)", DUCKDB) == "SELECT time_bucket(INTERVAL '1 hour', time)"


def test_apply_dialect_datafusion_timestamp() -> None:
    assert apply_dialect("@TS('2026-01-01')", DATAFUSION) == "CAST('2026-01-01' AS TIMESTAMP)"


@pytest.mark.parametrize("suite", ["tsbs_iot", "iot_point", "tsbs_devops"])
@pytest.mark.parametrize("dialect", [DUCKDB, DATAFUSION, POLARS, TIMESCALEDB])
def test_every_tsbs_query_is_fully_translated(suite: str, dialect: Dialect) -> None:
    assert all("@" not in apply_dialect(sql, dialect) for _, sql in raw_queries(suite))


def test_tsbs_devops_has_the_15_query_types() -> None:
    # Other readers' and tunings' versions (`-- name: q [reader]`) are versions of these, not more queries.
    assert len([n for n, _ in raw_queries("tsbs_devops") if " [" not in n]) == 15


def test_s3_tables_live_in_the_bench_bucket() -> None:
    assert Place(Target("aws-s3"), "tsbs-iot-small", "timelake").root == "s3://bench/lakes/tsbs-iot-small/timelake"


def test_local_target_keeps_tables_on_disk() -> None:
    place = Place(Target("local"), "tsbs-iot-small", "lance")
    assert place.root.endswith("lakes/local/tsbs-iot-small/lance/lake")
    assert not place.target.s3


def test_a_default_system_keeps_its_tables_apart_from_the_tuned_one() -> None:
    tuned = Place(Target("aws-s3"), "iiot-small", "lance")
    default = Place(Target("aws-s3"), "iiot-small", "lance", "default")
    assert tuned.root == "s3://bench/lakes/iiot-small/lance"
    assert default.root == "s3://bench/lakes/iiot-small/lance-default"
    assert default.local_dir.name == "lance-default"


def test_every_system_is_tuned_and_tunings_are_known() -> None:
    for system in systems().values():
        assert "tuned" in system.tunings
        assert set(system.tunings) <= set(TUNINGS)
    assert "default" in systems()["influxdb3"].tunings


def test_clickbench_queries_come_from_the_download_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(queries, "DATA", tmp_path)
    cached = tmp_path / "clickbench" / "sql" / CLICKBENCH_COMMIT[:12] / "duckdb-parquet-partitioned" / "queries.sql"
    cached.parent.mkdir(parents=True)
    cached.write_text("SELECT 1;\nSELECT 2;\n")
    assert clickbench_queries("duckdb-parquet-partitioned") == [("q00", "SELECT 1;"), ("q01", "SELECT 2;")]


@pytest.mark.parametrize("name", ["weekly", "iiot"])
def test_the_weekly_profiles_name_real_workloads_systems_and_scales(name: str) -> None:
    profile = tomllib.loads((BENCH / "profiles" / f"{name}.toml").read_text())
    assert set(profile.get("tunings", ["tuned"])) <= set(TUNINGS)
    for entry in profile["run"]:
        workload = workloads()[entry["workload"]](entry.get("scale"), **entry.get("params", {}))
        assert set(entry["systems"]) <= set(systems())
        assert set(entry.get("checkpoints", [])) <= {"ingested", "maintained"}
        assert workload.dataset
