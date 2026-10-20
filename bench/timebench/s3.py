"""S3 endpoints of the benchmark, and their settings in each client's own option format.

Every S3 read and write goes through ``tools/s3proxy`` on this machine, with fixed dummy keys: to a local
RustFS, or in AWS mode (``bench/infra``) to a real bucket that the proxy re-signs for. Clients always see
``s3://bench/...``.
"""

from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    import pyarrow.fs as pafs
    from datafusion.object_store import AmazonS3

BUCKET = "bench"
"""The bucket name clients use; the proxy maps it to the real bucket."""


class ProxyStats(TypedDict, total=False):
    """Counters of ``tools/s3proxy`` (``GET /stats`` on its admin port), cumulative since it started."""

    requests: int
    bytes: int
    ttfb_ms: int
    kinds: dict[str, int]
    """Requests by kind (GET, HEAD, LIST, PUT, ...); AWS mode only."""


@dataclass(frozen=True)
class S3Endpoint:
    """One S3-compatible endpoint: an address plus the dummy credentials the proxy accepts."""

    address: str
    """``host:port``, plain HTTP."""
    admin: str | None = None
    """Admin address of the proxy (request counters), if it has one."""
    key: str = "benchkey"
    secret: str = "benchsecret123"  # noqa: S105 (a fixed dummy key the proxy and RustFS accept)
    region: str = "us-east-1"

    @property
    def url(self) -> str:
        """The endpoint as an ``http://`` URL."""
        return f"http://{self.address}"

    def duckdb_secret(self) -> str:
        """A DuckDB ``CREATE SECRET`` statement for httpfs."""
        return (
            f"CREATE SECRET (TYPE s3, KEY_ID '{self.key}', SECRET '{self.secret}', REGION '{self.region}', "
            f"ENDPOINT '{self.address}', URL_STYLE 'path', USE_SSL false)"
        )

    def pyiceberg_properties(self) -> dict[str, str]:
        """PyIceberg catalog properties (its ``s3.*`` FileIO settings)."""
        return {
            "s3.endpoint": self.url,
            "s3.access-key-id": self.key,
            "s3.secret-access-key": self.secret,
            "s3.region": self.region,
        }

    def object_store_options(self) -> dict[str, str]:
        """Options of Rust's ``object_store`` crate, as Timelake takes them."""
        return {
            "aws_endpoint": self.url,
            "aws_access_key_id": self.key,
            "aws_secret_access_key": self.secret,
            "aws_region": self.region,
            "aws_allow_http": "true",
        }

    def lance_options(self) -> dict[str, str]:
        """Lance ``storage_options`` (object_store keys, but its own name for allowing HTTP)."""
        return {
            "aws_endpoint": self.url,
            "aws_access_key_id": self.key,
            "aws_secret_access_key": self.secret,
            "aws_region": self.region,
            "allow_http": "true",
        }

    def polars_options(self) -> dict[str, str]:
        """Polars ``storage_options``."""
        return {
            "aws_access_key_id": self.key,
            "aws_secret_access_key": self.secret,
            "aws_region": self.region,
            "aws_endpoint_url": self.url,
            "aws_allow_http": "true",
        }

    def environment(self) -> dict[str, str]:
        """``AWS_*`` environment variables, for clients that read only those (the DuckDB Vortex extension)."""
        return {
            "AWS_ACCESS_KEY_ID": self.key,
            "AWS_SECRET_ACCESS_KEY": self.secret,
            "AWS_REGION": self.region,
            "AWS_ENDPOINT": self.url,
            "AWS_ALLOW_HTTP": "true",
        }

    def datafusion_store(self) -> AmazonS3:
        """A DataFusion object store for the benchmark bucket."""
        from datafusion.object_store import AmazonS3

        return AmazonS3(
            bucket_name=BUCKET,
            region=self.region,
            access_key_id=self.key,
            secret_access_key=self.secret,
            endpoint=self.url,
            allow_http=True,
        )

    def pyarrow_fs(self) -> pafs.S3FileSystem:
        """A pyarrow filesystem; paths start with the bucket (``bench/...``)."""
        import pyarrow.fs as pafs

        return pafs.S3FileSystem(
            access_key=self.key,
            secret_key=self.secret,
            region=self.region,
            endpoint_override=self.url,
            scheme="http",
        )

    def stats(self) -> ProxyStats:
        """The proxy's request counters."""
        if self.admin is None:
            msg = f"{self.address} has no admin endpoint"
            raise ValueError(msg)
        with urllib.request.urlopen(f"http://{self.admin}/stats") as response:
            return cast("ProxyStats", json.load(response))


PROXY_ENV = "TIMEBENCH_PROXY"
"""Overrides ``PROXY``'s address in a process: probe readers set it to ``PROBE_PROXY``'s."""
PROXY = S3Endpoint(os.environ.get(PROXY_ENV, "localhost:59100"), admin="localhost:59101")
"""The counting proxy that measured runs go through."""
PROBE_PROXY = S3Endpoint("localhost:59102")
"""The counting proxy's uncounted listener (``-listen-uncounted``): the same latency, but requests that run beside a
measured phase, such as probes while a system ingests, stay out of its counts."""
SETUP_PROXY = S3Endpoint("localhost:59000")
"""Setup work (loading data, listing sizes) that must not show up in the counts: an uncounted proxy on AWS,
RustFS itself locally."""
