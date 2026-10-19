"""Write the TSBS IoT Parquet tables as Iceberg tables (plain-lake baseline, spec 44).

Layout: readings/diagnostics partitioned by day(time) and sorted by (tags_id, time). This is the layout a team
would get from S3 Tables + a sort-compaction Glue job. Writes data/tsbs/iceberg[-s3]/metadata.json mapping
table -> current metadata file, for DuckDB's iceberg_scan.

    python tsbs_to_iceberg.py        # local filesystem warehouse
    python tsbs_to_iceberg.py s3     # s3://bench/tsbs/iceberg through the uncounted proxy (bench README)

BENCH_ICEBERG_S3_DIR keeps catalogs for different S3 targets apart (e.g. RustFS vs AWS).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil

import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import DayTransform, IdentityTransform

from timebench import DATA, TSBS_TABLES
from timebench.s3 import BUCKET, SETUP_PROXY

TSBS = DATA / "tsbs"


def main(target: str, s3_dir: str) -> None:
    """Writes the tables to a fresh warehouse and the metadata locations to metadata.json."""
    out = TSBS / (s3_dir if target == "s3" else "iceberg")
    shutil.rmtree(out, ignore_errors=True)
    (out / "warehouse").mkdir(parents=True)
    catalog = SqlCatalog(
        "bench",
        uri=f"sqlite:///{out}/catalog.db",
        warehouse=f"s3://{BUCKET}/tsbs/iceberg" if target == "s3" else f"file://{out}/warehouse",
        **(SETUP_PROXY.pyiceberg_properties() if target == "s3" else {}),
    )
    catalog.create_namespace("tsbs")
    locations = {}
    for name in TSBS_TABLES:
        table = pq.read_table(TSBS / "parquet" / f"{name}.parquet")
        if name == "tags":
            ice = catalog.create_table(f"tsbs.{name}", schema=table.schema)
        else:
            table = table.sort_by([("tags_id", "ascending"), ("time", "ascending")])
            ice = catalog.create_table(f"tsbs.{name}", schema=table.schema)
            with ice.update_spec() as spec:
                spec.add_field("time", DayTransform(), "time_day")
            ice = catalog.load_table(f"tsbs.{name}")
            with ice.transaction() as tx:
                tx.update_sort_order().asc("tags_id", IdentityTransform()).asc("time", IdentityTransform()).commit()
        ice.append(table)
        ice = catalog.load_table(f"tsbs.{name}")
        locations[name] = ice.metadata_location
        files = list(ice.scan().plan_files())
        print(f"{name}: {table.num_rows} rows, {len(files)} data files")
    (out / "metadata.json").write_text(json.dumps(locations, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("target", nargs="?", default="local", choices=["local", "s3"])
    main(p.parse_args().target, os.environ.get("BENCH_ICEBERG_S3_DIR", "iceberg-s3"))
