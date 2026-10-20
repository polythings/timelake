"""What timebench reads from the environment, read once by each entry point (``cli.main``, ``worker.main``).

A run's worker processes get the same settings through their environment (``BenchConfig.environ``).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from timebench import DATA
from timebench.s3 import PROXY, S3Endpoint

if TYPE_CHECKING:
    from collections.abc import Mapping


class ConfigError(RuntimeError):
    """A setting a system needs is missing or wrong; a run records it as that system's failure and goes on."""


LAKES_ENV = "BENCH_LAKES"
MACHINE_ENV = "BENCH_MACHINE"
SERVERS_ENV = "BENCH_SERVERS"
PROXY_ENV = "TIMEBENCH_PROXY"
TIMESCALEDB_ENV = "BENCH_TIMESCALEDB_DSN"
CLICKHOUSE_ENV = "BENCH_CLICKHOUSE_URL"
QUESTDB_HTTP_ENV = "BENCH_QUESTDB_HTTP"
QUESTDB_PG_ENV = "BENCH_QUESTDB_PG"
INFLUXDB_ENV = "BENCH_INFLUXDB_URL"
INFLUXDB_DEFAULTS_ENV = "BENCH_INFLUXDB_DEFAULTS_URL"
AWS_REGION_ENV = "AWS_REGION"
TABLE_BUCKET_ENV = "BENCH_TABLE_BUCKET_ARN"
S3TABLES_WAIT_ENV = "BENCH_S3TABLES_WAIT_S"
TIMELAKE_OPTIONS_ENV = "TIMEBENCH_TIMELAKE_OPTIONS"
TIMELAKE_SETTINGS_ENV = "TIMEBENCH_TIMELAKE_SETTINGS"


@dataclass(frozen=True)
class BenchConfig:
    """Where a run keeps its tables, the servers it talks to, and the settings an A/B comparison changes.

    The defaults are what ``infra/servers.sh`` and ``tools/s3proxy`` use on this machine.
    """

    lakes: Path = DATA / "lakes"
    """The tables and state of each system in a run (local targets), by target, dataset and system."""
    machine: str = ""
    """How run records describe the machine (e.g. ``c8gd.2xlarge``); empty: its CPU and core count."""
    servers: Path | None = None
    """``infra/servers.sh``: when set, each database server runs only while its system does."""
    proxy: S3Endpoint = PROXY
    """The counting proxy S3 readers and writers go through (probes use its uncounted listener)."""
    timescaledb_dsn: str = ""
    """A TimescaleDB server used only for benchmarks; empty: none."""
    clickhouse_url: str = "http://default:bench@localhost:58123"
    questdb_http: str = "localhost:59900"
    """QuestDB's HTTP and QWP address."""
    questdb_pg: str = "localhost:58812"
    """QuestDB's PGWire address."""
    influxdb_url: str = "http://localhost:58181"
    """The InfluxDB 3 Core server of tuned runs (``--query-file-limit`` raised)."""
    influxdb_defaults_url: str = "http://localhost:58182"
    """The InfluxDB 3 Core server of default runs, as shipped."""
    aws_region: str = ""
    table_bucket_arn: str = ""
    """The S3 Tables table bucket (AWS targets)."""
    s3tables_wait_s: int = 6 * 3600
    """How long S3 Tables maintenance waits for its managed compaction."""
    timelake_options: Mapping[str, str] = field(default_factory=dict)
    """Extra storage options for Timelake's readers, for comparing settings such as ``timelake_cache=off``."""
    timelake_settings: Mapping[str, str] = field(default_factory=dict)
    """Session settings for Timelake's readers, for comparing settings such as ``timelake.filter_pushdown=true``."""

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> BenchConfig:
        """The settings in ``env`` (this process's environment by default); defaults for the ones not set."""
        env = os.environ if env is None else env
        default = cls()
        return cls(
            lakes=Path(env.get(LAKES_ENV) or default.lakes),
            machine=env.get(MACHINE_ENV, ""),
            servers=Path(env[SERVERS_ENV]) if env.get(SERVERS_ENV) else None,
            proxy=S3Endpoint(env.get(PROXY_ENV) or default.proxy.address, admin=default.proxy.admin),
            timescaledb_dsn=env.get(TIMESCALEDB_ENV, ""),
            clickhouse_url=env.get(CLICKHOUSE_ENV) or default.clickhouse_url,
            questdb_http=env.get(QUESTDB_HTTP_ENV) or default.questdb_http,
            questdb_pg=env.get(QUESTDB_PG_ENV) or default.questdb_pg,
            influxdb_url=(env.get(INFLUXDB_ENV) or default.influxdb_url).rstrip("/"),
            influxdb_defaults_url=(env.get(INFLUXDB_DEFAULTS_ENV) or default.influxdb_defaults_url).rstrip("/"),
            aws_region=env.get(AWS_REGION_ENV, ""),
            table_bucket_arn=env.get(TABLE_BUCKET_ENV, ""),
            s3tables_wait_s=int(env.get(S3TABLES_WAIT_ENV) or default.s3tables_wait_s),
            timelake_options=key_values(env.get(TIMELAKE_OPTIONS_ENV, "")),
            timelake_settings=key_values(env.get(TIMELAKE_SETTINGS_ENV, "")),
        )

    def to_env(self) -> dict[str, str]:
        """The variables ``from_env`` reads these settings back from (the ones that aren't empty)."""
        env = {
            LAKES_ENV: str(self.lakes),
            MACHINE_ENV: self.machine,
            SERVERS_ENV: str(self.servers or ""),
            PROXY_ENV: self.proxy.address,
            TIMESCALEDB_ENV: self.timescaledb_dsn,
            CLICKHOUSE_ENV: self.clickhouse_url,
            QUESTDB_HTTP_ENV: self.questdb_http,
            QUESTDB_PG_ENV: self.questdb_pg,
            INFLUXDB_ENV: self.influxdb_url,
            INFLUXDB_DEFAULTS_ENV: self.influxdb_defaults_url,
            AWS_REGION_ENV: self.aws_region,
            TABLE_BUCKET_ENV: self.table_bucket_arn,
            S3TABLES_WAIT_ENV: str(self.s3tables_wait_s),
            TIMELAKE_OPTIONS_ENV: ",".join(f"{k}={v}" for k, v in self.timelake_options.items()),
            TIMELAKE_SETTINGS_ENV: ",".join(f"{k}={v}" for k, v in self.timelake_settings.items()),
        }
        return {k: v for k, v in env.items() if v}

    def environ(self) -> dict[str, str]:
        """The environment of a worker process: this process's, with these settings."""
        return {**os.environ, **self.to_env()}


def key_values(raw: str) -> dict[str, str]:
    """``key=value,...`` as a dict; items without ``=`` are left out."""
    return dict(kv.split("=", 1) for kv in raw.split(",") if "=" in kv)
