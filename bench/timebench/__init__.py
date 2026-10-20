"""Code shared by the benchmark scripts: paths, S3 settings, query loading, result records.

Installed (editable) into the root uv environment by ``uv sync``, so every script imports it the same way
wherever it lives: ``from timebench.results import geomean``.
"""

from pathlib import Path

BENCH = Path(__file__).resolve().parent.parent
"""The ``bench/`` directory."""
DATA = BENCH / "data"
"""Generated datasets (git-ignored)."""
QUERIES = BENCH / "queries"
TUNING_SQL = BENCH / "tunings"
"""Each workload's statements for a tuned run of a system: ``tunings/<workload>/<system>.sql`` (D23)."""
RESULTS = BENCH / "results"
RUNS = RESULTS / "runs"
"""One file per ``timebench`` run: the benchmark history."""

TPCH_TABLES = ("lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region")
TSBS_TABLES = ("tags", "readings", "diagnostics")
TSBS_TIME_SERIES = ("readings", "diagnostics")
"""The TSBS tables with a time column (``tags`` is a dimension table)."""
