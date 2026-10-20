"""Plain Parquet files without a table format, as a Firehose or Kafka Connect S3 sink writes them.

Read by DuckDB and DataFusion over a directory listing.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, override

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from timebench.s3 import BUCKET
from timebench.systems import (
    Runner,
    System,
    datafusion_runner,
    duckdb_connection,
    duckdb_runner,
    package_versions,
)
from timebench.targets import Tuning

if TYPE_CHECKING:
    from timebench.targets import Place


class ParquetFiles(System):
    """A new zstd Parquet file per commit, under ``<table>/day=<date>/`` (Hive-style) for time-series tables.

    Default: no maintenance, the files as the sink wrote them. Tuned: maintenance rewrites each day into one file
    sorted by series and time; readers list the directory, so a reader running during the rewrite could see both
    copies (the race a table format exists to prevent).
    """

    name = "parquet"
    title = "Parquet files"
    readers = ("duckdb", "datafusion")
    tunings = (Tuning.TUNED, Tuning.DEFAULT)

    @property
    def fs(self) -> pafs.FileSystem:
        """The filesystem the files go to: S3 through the counting proxy, or the local disk."""
        return self.place.s3.pyarrow_fs() if self.place.s3 else pafs.LocalFileSystem()

    def dir(self, table: str) -> str:
        """A table's directory as the filesystem names it."""
        if self.target.s3:
            return f"{BUCKET}/{self.place.prefix}/{table}"
        return f"{self.place.root}/{table}"

    @override
    def append(self, table: str, data: pa.Table) -> None:
        spec = self.specs[table]
        if not spec.time_column:
            self.write(f"{self.dir(table)}/{uuid.uuid4().hex}.parquet", data)
            return
        days = data[spec.time_column].to_numpy().astype("datetime64[D]")  # from the column's own unit
        for day in np.unique(days):
            part = data.filter(pa.array(days == day))
            self.write(f"{self.dir(table)}/day={day}/{uuid.uuid4().hex}.parquet", part)

    def write(self, path: str, data: pa.Table) -> None:
        """Writes one file, atomically: readers listing the directory never see it half-written."""
        fs = self.fs
        if self.target.s3:
            pq.write_table(data, path, filesystem=fs, compression="zstd")  # a PUT is atomic
            return
        fs.create_dir(path.rsplit("/", 1)[0], recursive=True)
        pq.write_table(data, f"{path}.tmp", filesystem=fs, compression="zstd")  # readers list *.parquet only
        fs.move(f"{path}.tmp", path)

    @override
    def maintain(self) -> None:
        if not self.tuned:
            return
        fs = self.fs
        for table, spec in self.saved_specs().items():
            if not spec["time_column"]:
                continue
            days = fs.get_file_info(pafs.FileSelector(self.dir(table)))
            for day in (d for d in days if d.type == pafs.FileType.Directory):
                files = [f.path for f in fs.get_file_info(pafs.FileSelector(day.path)) if f.path.endswith(".parquet")]
                if len(files) < 2:
                    continue
                data = pq.ParquetDataset(files, filesystem=fs, partitioning=None).read()
                keys = [(c, "ascending") for c in [*spec["series_key"], spec["time_column"]]]
                self.write(f"{day.path}/{uuid.uuid4().hex}.parquet", data.sort_by(keys))
                for f in files:
                    fs.delete_file(f)

    @override
    def state(self) -> dict[str, Any]:
        specs = self.saved_specs()
        root = f"s3://{BUCKET}/{self.place.prefix}" if self.target.s3 else self.place.root
        return {"root": root, "specs": specs, "tables": {n: f"{root}/{n}" for n in specs}}

    @override
    @classmethod
    def version(cls, place: Place) -> dict[str, str]:
        return package_versions("pyarrow", "duckdb", "datafusion")

    @override
    @classmethod
    def open_reader(cls, reader: str, place: Place, state: dict[str, Any]) -> Runner:
        if reader == "duckdb":
            con = duckdb_connection(place.s3)
            for t, location in state["tables"].items():
                files = f"{location}/**/*.parquet"
                con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{files}', hive_partitioning = false)")
            return duckdb_runner(con)

        from datafusion import SessionContext

        ctx = SessionContext()
        if place.s3:
            ctx.register_object_store("s3://", place.s3.datafusion_store(), BUCKET)
        for t, location in state["tables"].items():
            days = [("day", "string")] if state["specs"][t]["time_column"] else []
            ctx.register_parquet(t, f"{location}/", table_partition_cols=days)
        return datafusion_runner(ctx)
