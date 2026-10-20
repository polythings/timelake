"""Storage targets: where the systems of a run keep their tables, and the state a system leaves for its readers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

from timebench import LAKES
from timebench.s3 import BUCKET, SETUP_PROXY

if TYPE_CHECKING:
    from pathlib import Path


class StorageStats(TypedDict):
    """Objects and bytes a system's tables take."""

    objects: int | None
    """None where storage can't be listed (S3 Tables, a database server)."""
    bytes: int
    data_files: int
    data_bytes: int


@dataclass(frozen=True)
class Target:
    """Where tables live: the local disk, or S3 through ``tools/s3proxy`` (a local RustFS or a real bucket).

    The name tells S3 targets apart in results (``s3`` = RustFS on this machine, ``aws-s3``, ``aws-s3x``, ...);
    every name but ``local`` is S3. Clients always see ``s3://bench/...``; the proxy maps it to the real bucket.
    """

    name: str

    @property
    def storage(self) -> str:
        """``local`` or ``s3``."""
        return "local" if self.name == "local" else "s3"

    @property
    def s3(self) -> bool:
        """Whether the tables are on S3."""
        return self.storage == "s3"


@dataclass(frozen=True)
class Place:
    """One system's tables for one dataset on one target."""

    target: Target
    dataset: str
    """The workload and scale, e.g. ``tsbs-iot-small``."""
    system: str
    tuning: str = "tuned"
    """``tuned`` or ``default`` (``systems.TUNINGS``): a default system's tables live apart from the tuned one's."""

    @property
    def folder(self) -> str:
        """The system's folder: its name, with ``-default`` for its default tuning."""
        return self.system if self.tuning == "tuned" else f"{self.system}-{self.tuning}"

    @property
    def local_dir(self) -> Path:
        """Local files of the system: its state, catalogs, and its tables when the target is the local disk."""
        return LAKES / self.target.name / self.dataset / self.folder

    @property
    def prefix(self) -> str:
        """The key prefix of the system's tables in the bucket (S3 targets)."""
        return f"lakes/{self.dataset}/{self.folder}"

    @property
    def root(self) -> str:
        """Where the system's tables live."""
        return f"s3://{BUCKET}/{self.prefix}" if self.target.s3 else str(self.local_dir / "lake")

    def load_state(self) -> dict[str, Any]:
        """What ingest left for the readers: table locations, the phase, when ingest finished."""
        return json.loads((self.local_dir / "state.json").read_text())

    def save_state(self, state: dict[str, Any]) -> None:
        """Writes the system's state for the readers."""
        self.local_dir.mkdir(parents=True, exist_ok=True)
        (self.local_dir / "state.json").write_text(json.dumps(state, indent=2))

    def storage_stats(self) -> StorageStats:
        """Objects and bytes under the system's root: all files, and the data files among them."""
        import pyarrow.fs as pafs

        if self.target.s3:
            fs, base = SETUP_PROXY.pyarrow_fs(), f"{BUCKET}/{self.prefix}"
        else:
            fs, base = pafs.LocalFileSystem(), self.root
        infos = fs.get_file_info(pafs.FileSelector(base, recursive=True, allow_not_found=True))
        files = [i for i in infos if i.type == pafs.FileType.File]
        data = [i for i in files if i.path.endswith((".parquet", ".lance")) and "/metadata/" not in i.path]
        return {
            "objects": len(files),
            "bytes": sum(i.size for i in files),
            "data_files": len(data),
            "data_bytes": sum(i.size for i in data),
        }

    def clear(self) -> None:
        """Removes what an earlier run left: local files and, on S3, the system's objects."""
        import shutil

        shutil.rmtree(self.local_dir, ignore_errors=True)
        (self.local_dir / "lake").mkdir(parents=True)
        if self.target.s3:
            SETUP_PROXY.pyarrow_fs().delete_dir_contents(f"{BUCKET}/{self.prefix}", missing_dir_ok=True)
