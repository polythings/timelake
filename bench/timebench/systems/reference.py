"""The reference answers: every arrival kept as a local Parquet file, read by DuckDB without duplicate rows.

A workload whose rows may arrive twice (at-least-once delivery) has one right answer per query: the one over each
row once. When a run includes this system, first, every other system's answers are checked against it
(``harness.same_answer``). It keeps its files on the local disk whatever the run's target, and isn't ranked.
"""

from __future__ import annotations

import shutil
import uuid
from typing import TYPE_CHECKING, Any, override

import pyarrow.parquet as pq

from timebench.meta import version
from timebench.systems import TUNINGS, Runner, System, duckdb_connection, duckdb_runner
from timebench.targets import Target

if TYPE_CHECKING:
    import pyarrow as pa

    from timebench.targets import Place, StorageStats
    from timebench.workloads import TableSpec


class Reference(System):
    """One Parquet file per commit; time-series tables are read through ``SELECT DISTINCT *``."""

    name = "reference"
    title = "Reference"
    readers = ("duckdb",)
    tunings = TUNINGS  # the answers every tuning is checked against
    server = True  # local files, whatever the target
    s3_storage_prices = False

    def __init__(self, place: Place, namespace: str) -> None:
        super().__init__(place, namespace)
        self.specs: dict[str, TableSpec] = {}

    @override
    def reset(self) -> None:
        shutil.rmtree(self.place.local_dir, ignore_errors=True)

    def dir(self, table: str) -> str:
        """A table's directory."""
        return str(self.place.local_dir / "lake" / table)

    @override
    def create(self, spec: TableSpec, schema: pa.Schema) -> None:
        self.specs[spec.name] = spec
        (self.place.local_dir / "lake" / spec.name).mkdir(parents=True, exist_ok=True)

    @override
    def append(self, table: str, data: pa.Table) -> None:
        pq.write_table(data, f"{self.dir(table)}/{uuid.uuid4().hex}.parquet", compression="zstd")

    @override
    def maintain(self) -> None:
        pass  # nothing to maintain: the answers stay the same

    @override
    def state(self) -> dict[str, Any]:
        specs = {n: s.time_column for n, s in self.specs.items()} or self.place.load_state()["time_columns"]
        return {"time_columns": specs, "tables": {n: self.dir(n) for n in specs}}

    @override
    def stats(self) -> StorageStats:
        files = list((self.place.local_dir / "lake").rglob("*.parquet"))
        size = sum(f.stat().st_size for f in files)
        return {"objects": len(files), "bytes": size, "data_files": len(files), "data_bytes": size}

    @override
    @classmethod
    def version(cls) -> dict[str, str]:
        return {"duckdb": version("duckdb") or "?"}

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        con = duckdb_connection(Target("local"))
        for t, location in state["tables"].items():
            distinct = "DISTINCT " if state["time_columns"][t] else ""
            con.execute(f"CREATE VIEW {t} AS SELECT {distinct}* FROM read_parquet('{location}/*.parquet')")
        return duckdb_runner(con)
