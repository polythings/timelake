"""Query suites (``queries/``) and the SQL dialect placeholders of the TSBS suites."""

from __future__ import annotations

import datetime as dt
import re
import urllib.request
from collections.abc import Iterator, Mapping

from timebench import DATA, QUERIES

SUITES = ("tpch", "clickbench", "tsbs_iot", "iot_point")
"""The suites ``run.py`` (the Phase 0 engine comparison) runs."""
TSBS_SUITES = ("tsbs_iot", "iot_point")
"""Suites over the TSBS tables, written with dialect placeholders."""

Dialect = Mapping[str, str]
"""Placeholder name -> format string with the placeholder's argument as ``{0}``."""

DUCKDB: Dialect = {
    "B1M": "time_bucket(INTERVAL '1 minute', {})",
    "B10M": "time_bucket(INTERVAL '10 minutes', {})",
    "B1D": "time_bucket(INTERVAL '24 hours', {})",  # '1 day' takes DuckDB's ~130x slower calendar path on timestamptz
    "B1H": "time_bucket(INTERVAL '1 hour', {})",
    "EPOCH": "epoch({})",
    "TS": "TIMESTAMPTZ {}",
}
DATAFUSION: Dialect = {
    "B1M": "date_bin(INTERVAL '1 minute', {})",
    "B10M": "date_bin(INTERVAL '10 minutes', {})",
    "B1D": "date_bin(INTERVAL '24 hours', {})",
    "B1H": "date_bin(INTERVAL '1 hour', {})",
    "EPOCH": "date_part('epoch', {})",
    "TS": "CAST({} AS TIMESTAMP)",
}
POLARS: Dialect = {
    # Polars SQL has no time bucketing and can't compare tz-aware timestamps with literals, so `time` is
    # exposed as naive UTC (run.setup_polars) and buckets are integer arithmetic on epoch microseconds.
    "B1M": "(CAST({0} AS BIGINT) - CAST({0} AS BIGINT) % 60000000)",
    "B10M": "(CAST({0} AS BIGINT) - CAST({0} AS BIGINT) % 600000000)",
    "B1D": "(CAST({0} AS BIGINT) - CAST({0} AS BIGINT) % 86400000000)",
    "B1H": "(CAST({0} AS BIGINT) - CAST({0} AS BIGINT) % 3600000000)",
    "EPOCH": "(CAST({0} AS BIGINT) / 1000000.0)",
    "TS": "CAST({0} AS TIMESTAMP)",
}
TIMESCALEDB: Dialect = {
    # timebench/systems/timescaledb.py; the session runs with TimeZone = UTC.
    "B1M": "time_bucket('1 minute', {})",
    "B10M": "time_bucket('10 minutes', {})",
    "B1D": "time_bucket('1 day', {})",
    "B1H": "time_bucket('1 hour', {})",
    "EPOCH": "extract(epoch from {})",
    "TS": "TIMESTAMPTZ {}",
}

CLICKHOUSE: Dialect = {
    "B1M": "toStartOfMinute({})",
    "B10M": "toStartOfInterval({}, INTERVAL 10 MINUTE)",
    "B1D": "toStartOfDay({})",
    "B1H": "toStartOfHour({})",
    "EPOCH": "(toUnixTimestamp64Micro(toDateTime64({}, 6, 'UTC')) / 1000000.0)",  # buckets can be DateTime
    "TS": "toDateTime64({}, 6, 'UTC')",
}
QUESTDB: Dialect = {
    "B1M": "timestamp_floor('m', {})",
    "B10M": "timestamp_floor('10m', {})",
    "B1D": "timestamp_floor('d', {})",
    "B1H": "timestamp_floor('h', {})",
    "EPOCH": "(CAST({} AS LONG) / 1000000.0)",  # microseconds
    "TS": "to_timestamp({}, 'yyyy-MM-dd HH:mm:ss')",
}

_PLACEHOLDER = re.compile(r"@(B1M|B10M|B1D|B1H|EPOCH|TS)\(([^()]*)\)")  # arguments contain no nested parentheses
_AGO = re.compile(r"@AGO\((\d+) (second|minute|hour|day)s?\)")
_SECONDS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}


def apply_dialect(sql: str, dialect: Dialect, now: int | None = None) -> str:
    """Replaces the placeholders (``@B1H(time)``, ...) with one engine's SQL.

    ``@AGO(15 minutes)`` is a time before ``now`` (epoch seconds; the data's present, which moves as it arrives):
    it becomes a ``@TS`` literal first.
    """
    if now is not None:
        sql = _AGO.sub(lambda m: f"@TS('{ts_literal(now - int(m.group(1)) * _SECONDS[m.group(2)])}')", sql)
    elif _AGO.search(sql):
        msg = "@AGO needs the time the query runs at (now)"
        raise ValueError(msg)
    return _PLACEHOLDER.sub(lambda m: dialect[m.group(1)].format(m.group(2)), sql)


def ts_literal(epoch: int) -> str:
    """``2026-01-03 12:00:00`` (UTC) for epoch seconds, as the ``@TS`` placeholder takes it."""
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def raw_queries(suite: str) -> Iterator[tuple[str, str]]:
    """``(name, sql)`` of a ``-- name:``-delimited suite, with comments removed and placeholders kept."""
    return ((name, sql) for name, _, sql in described_queries(suite))


SITE_SUITES = ("tsbs_iot", "iot_point", "tsbs_devops", "iiot")
"""Suites whose queries the website describes (``timebench queries --site``)."""


def described_queries(suite: str) -> Iterator[tuple[str, str, str]]:
    """``(name, description, sql)`` of a suite: the description is the comment lines right after ``-- name:``."""
    text = (QUERIES / suite / "queries.sql").read_text()
    for block in re.split(r"^-- name: ", text, flags=re.MULTILINE)[1:]:
        name, _, rest = block.partition("\n")
        lines = rest.splitlines()
        described = [line.removeprefix("--").strip() for line in _leading_comments(lines)]
        sql = "\n".join(line for line in lines if not line.startswith("--")).strip().rstrip(";")
        yield name.strip(), " ".join(described), sql


def _leading_comments(lines: list[str]) -> Iterator[str]:
    for line in lines:
        if not line.startswith("--"):
            return
        yield line


def site_sql(sql: str) -> str:
    """A query as the website shows it: in the Timelake reader's dialect, times relative to the data's present."""
    sql = _AGO.sub(lambda m: f"now() - INTERVAL '{m.group(1)} {m.group(2)}s'", sql)
    return _PLACEHOLDER.sub(lambda m: DATAFUSION[m.group(1)].format(m.group(2)), sql)


CLICKBENCH_COMMIT = "f02259ad000e6a7963366fd262fccfa466fd3173"
"""ClickBench revision the SQL comes from. Its license (CC BY-NC-SA 4.0) doesn't fit this repository, so the files are
downloaded, not committed."""


def clickbench_sql(variant: str, name: str) -> str:
    """ClickBench's ``<variant>/<name>.sql`` (``queries`` or ``create``), downloaded once to ``data/clickbench``."""
    path = DATA / "clickbench" / "sql" / CLICKBENCH_COMMIT[:12] / variant / f"{name}.sql"
    if not path.exists():
        url = f"https://raw.githubusercontent.com/ClickHouse/ClickBench/{CLICKBENCH_COMMIT}/{variant}/{name}.sql"
        with urllib.request.urlopen(url, timeout=30) as response:
            body = response.read()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    return path.read_text()


def clickbench_queries(variant: str) -> list[tuple[str, str]]:
    """ClickBench queries named ``q00``... from one engine variant's file (one query per line)."""
    lines = clickbench_sql(variant, "queries").strip().splitlines()
    return [(f"q{i:02d}", q) for i, q in enumerate(lines)]
