"""What a run records about where it ran: the commit, the machine, and package versions."""

from __future__ import annotations

import importlib.metadata
import os
import platform
import subprocess

from timebench import BENCH


def git(*args: str) -> str:
    """Output of a git command in the repository, empty if git fails."""
    # Fixed git commands; nothing comes from outside.
    proc = subprocess.run(["git", *args], cwd=BENCH, capture_output=True, text=True, check=False)
    return proc.stdout.strip() if proc.returncode == 0 else ""


def commit() -> str:
    """The checked-out commit."""
    return git("rev-parse", "HEAD")


def dirty() -> bool:
    """Whether tracked files outside ``bench/results`` have uncommitted changes (the run isn't that commit)."""
    return bool(git("status", "--porcelain", "--untracked-files=no", "--", "..", ":!results", ":!../dashboard"))


def machine(described: str = "") -> str:
    """The machine as ``described`` (``BenchConfig.machine``, e.g. ``c8gd.2xlarge``), else its CPU and core count."""
    return described or f"{platform.machine()} {os.cpu_count()} cores"


def version(package: str) -> str | None:
    """An installed Python package's version, None if it isn't installed."""
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None
