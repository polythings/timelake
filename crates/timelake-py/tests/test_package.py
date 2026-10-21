"""The package itself: its docstring example, version, exports, and that the type stub matches the module."""

from __future__ import annotations

import ast
import doctest
import importlib.metadata
import inspect
import subprocess
import sys
from importlib.resources import files
from typing import TYPE_CHECKING

import pytest

import timelake
from timelake import _timelake

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def test_docstring_example_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)  # the example creates ./warehouse
    assert doctest.testmod(timelake, raise_on_error=True).attempted > 0


def test_version_matches_the_distribution() -> None:
    assert timelake.__version__ == importlib.metadata.version("timelake")


def test_all_names_exist() -> None:
    assert [name for name in timelake.__all__ if not hasattr(timelake, name)] == []


def stub_classes() -> dict[str, ast.ClassDef]:
    tree = ast.parse(files(timelake).joinpath("_timelake.pyi").read_text())
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def stub_signature(func: ast.FunctionDef) -> list[tuple[str, str | None]]:
    """Parameter names and default values (as source) of a stub function, without ``self`` or ``cls``."""
    args = func.args.args[1:]
    defaults = [None] * (len(args) - len(func.args.defaults)) + [ast.unparse(d) for d in func.args.defaults]
    return [(a.arg, d) for a, d in zip(args, defaults, strict=True)]


def runtime_signature(obj: Callable[..., object]) -> list[tuple[str, str | None]]:
    params = [p for p in inspect.signature(obj).parameters.values() if p.name != "self"]
    return [(p.name, None if p.default is p.empty else repr(p.default)) for p in params]


def stub_members() -> list[tuple[str, str]]:
    return [
        (cls, item.name)
        for cls, node in stub_classes().items()
        for item in node.body
        if isinstance(item, ast.FunctionDef)
    ]


def test_stub_declares_every_class() -> None:
    runtime = {name for name, obj in vars(_timelake).items() if isinstance(obj, type)}
    assert set(stub_classes()) == runtime


@pytest.mark.parametrize("cls", ["Lake", "Table"])
def test_stub_declares_every_public_member(cls: str) -> None:
    runtime = {name for name in vars(getattr(_timelake, cls)) if not name.startswith("_")}
    assert {name for c, name in stub_members() if c == cls and not name.startswith("_")} == runtime


@pytest.mark.parametrize(("cls", "name"), [m for m in stub_members() if m[1] != "__new__"])
def test_stub_member_matches_runtime(cls: str, name: str) -> None:
    func = next(f for f in stub_classes()[cls].body if isinstance(f, ast.FunctionDef) and f.name == name)
    member = inspect.getattr_static(getattr(_timelake, cls), name)
    is_property = any(ast.unparse(d) == "property" for d in func.decorator_list)
    if is_property:
        assert inspect.isdatadescriptor(member)
    else:
        assert stub_signature(func) == runtime_signature(member)


def test_stub_constructor_matches_runtime() -> None:
    new = next(f for f in stub_classes()["Lake"].body if isinstance(f, ast.FunctionDef) and f.name == "__new__")
    assert stub_signature(new) == runtime_signature(_timelake.Lake)


def test_version_hint_is_written_before_exit(tmp_path: Path) -> None:
    """Commits update the version hint in the background; the package waits for it at exit."""
    script = f"""
import pyarrow as pa, timelake
lake = timelake.Lake({str(tmp_path)!r})
table = lake.create_table("iot.r", pa.schema([("v", pa.int64())]))
table.append(pa.table({{"v": [1]}}))
"""
    subprocess.run([sys.executable, "-c", script], check=True)  # noqa: S603 (this interpreter, a fixed script)
    assert (tmp_path / "iot" / "r" / "metadata" / "version-hint.text").read_text() == "2"
