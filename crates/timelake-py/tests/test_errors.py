"""Engine errors surface as the closest built-in Python exception."""

from pathlib import Path

import pyarrow as pa
import pytest
from conftest import SCHEMA, readings

import timelake


def test_missing_table_raises_key_error(lake: timelake.Lake) -> None:
    with pytest.raises(KeyError):
        lake.table("iot.missing")


def test_existing_table_raises_file_exists_error(lake: timelake.Lake, table: timelake.Table) -> None:
    with pytest.raises(FileExistsError):
        lake.create_table(table.name, SCHEMA, time_column="time")


def test_invalid_table_name_raises_value_error(lake: timelake.Lake) -> None:
    with pytest.raises(ValueError, match="name"):
        lake.create_table("iot.bad name", SCHEMA)


def test_unknown_column_raises_value_error(table: timelake.Table) -> None:
    data = readings().append_column("extra", pa.array([0] * 300))
    with pytest.raises(ValueError, match="extra"):
        table.append(data)


def test_missing_required_column_raises_value_error(lake: timelake.Lake) -> None:
    schema = pa.schema([pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False), ("device", pa.int32())])
    table = lake.create_table("iot.strict", schema, time_column="time")
    with pytest.raises(ValueError, match="time"):
        table.append(pa.table({"device": pa.array([1], pa.int32())}))


def test_uncastable_column_raises_value_error(table: timelake.Table) -> None:
    data = readings(minutes=1, devices=1).set_column(2, "value", pa.array([[1.0]]))
    with pytest.raises(ValueError, match="be written to the table"):
        table.append(data)


def test_bad_retention_raises_value_error(lake: timelake.Lake) -> None:
    with pytest.raises(ValueError, match="duration"):
        lake.create_table("iot.r", SCHEMA, time_column="time", retention="90 parsecs")


def test_bad_chunk_raises_value_error(lake: timelake.Lake) -> None:
    with pytest.raises(ValueError, match="fortnight"):
        lake.create_table("iot.r", SCHEMA, time_column="time", chunk="fortnight")


def test_bad_property_value_raises_value_error(lake: timelake.Lake) -> None:
    with pytest.raises(ValueError, match="row-group-rows"):
        lake.create_table("iot.r", SCHEMA, time_column="time", properties={"timelake.row-group-rows": "many"})


def test_creation_only_setting_raises_value_error(table: timelake.Table) -> None:
    with pytest.raises(ValueError, match="fixed when the table is created"):
        table.set_properties({"chunk-interval": "hour"})


def test_unknown_setting_raises_value_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown setting"):
        timelake.Lake(str(tmp_path), settings={"read_dedupe": "false"})


def test_conflict_error_is_a_runtime_error() -> None:
    assert issubclass(timelake.ConflictError, RuntimeError)


def test_conflict_error_is_named_in_the_package() -> None:
    assert timelake.ConflictError.__module__ == "timelake"
