"""Write what is derived from the front matter of the specs and milestones.

That is each file's header (title and status line), the index in spec/README.md, the roadmap in
spec/milestones/README.md, and the website's lists and pages of them.

uv run python scripts/spec_index.py           # rewrite them
uv run python scripts/spec_index.py --check   # fail if one is out of date (a pre-commit hook)
"""

import json
import re
import sys
from pathlib import Path
from typing import Any, NoReturn

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "spec"
MILESTONES = SPEC / "milestones"
LIB = ROOT / "dashboard" / "lib"
STATUS = {"done": "Done", "in-progress": "In progress", "planned": "Planned", "dropped": "Dropped"}
MILESTONE_STATUS = {"in-progress": "In progress", "planned": "Planned", "exploring": "Exploring", "done": "Done"}
AREAS = [
    "Tables and storage",
    "Writing",
    "Maintenance",
    "Reading",
    "SQL functions",
    "Integrations",
    "Services",
    "Benchmarks",
]
VERSION = re.compile(r"\d+\.\d+\.\d+|unreleased")
NOT_PAGES = {"README.md", "TEMPLATE.md"}

Fields = dict[str, Any]
Page = dict[str, Any]


def fail(message: str) -> NoReturn:
    """Stop with `message`."""
    sys.exit(message)


def split(path: Path) -> tuple[Fields, str]:
    """A file's front matter (`key: value` lines; `[a, b]` lists, `"…"` strings) and the text after it."""
    match = re.match(r"---\n(.*?)\n---\n", path.read_text(), re.DOTALL)
    if not match:
        fail(f"{path.relative_to(ROOT)}: no front matter")
    fields: Fields = {}
    for line in match.group(1).splitlines():
        key, value = line.split(": ", 1)
        if value.startswith("["):
            fields[key] = [v.strip() for v in value.strip("[]").split(",") if v.strip()]
        else:
            fields[key] = json.loads(value) if value.startswith('"') else value
    return fields, path.read_text()[match.end() :]


def need(path: Path, fields: Fields, keys: tuple[str, ...]) -> None:
    """Fail unless the front matter has every key."""
    for key in keys:
        if key not in fields:
            fail(f"{path.relative_to(ROOT)}: no `{key}` in the front matter")


def section(path: Path, body: str, name: str) -> str:
    """A `## name` section's first paragraph as one line."""
    match = re.search(rf"^## {name}\n\n(.*?)\n\n", body, re.DOTALL | re.MULTILINE)
    if not match:
        fail(f"{path.relative_to(ROOT)}: no {name} section")
    return " ".join(match.group(1).split())


def specs() -> list[Page]:
    """Every spec, by number."""
    found = []
    for path in SPEC.glob("[0-9][0-9][0-9][0-9]-*.md"):
        fields, body = split(path)
        need(path, fields, ("number", "title", "area", "status", "updated"))
        fields.setdefault("versions", [])
        if fields["status"] not in STATUS:
            fail(f"{path.name}: status must be one of {', '.join(STATUS)}, not {fields['status']!r}")
        if fields["area"] not in AREAS:
            fail(f"{path.name}: area must be one of {', '.join(AREAS)}, not {fields['area']!r}")
        if bad := [v for v in fields["versions"] if not VERSION.fullmatch(v)]:
            fail(f"{path.name}: versions are releases (0.5.0) or `unreleased`, not {', '.join(bad)}")
        found.append(
            {
                "number": int(fields["number"]),
                "slug": path.stem,
                "title": fields["title"],
                "area": fields["area"],
                "status": fields["status"],
                "updated": fields["updated"],
                "versions": fields["versions"],
                "summary": section(path, body, "Summary"),
                "related": [int(n) for n in fields.get("related", [])],
                "issues": [int(n) for n in fields.get("issues", [])],
                "milestones": [],
            }
        )
    return sorted(found, key=lambda s: s["number"])


def milestones(specs_: list[Page]) -> list[Page]:
    """Every milestone, by number, each spec also listing the milestones it is part of."""
    numbers = {s["number"]: s for s in specs_}
    found: list[Page] = []
    for path in MILESTONES.glob("*.md"):
        if path.name in NOT_PAGES:
            continue
        fields, body = split(path)
        need(path, fields, ("number", "title", "status", "updated", "description", "specs"))
        if fields["status"] not in MILESTONE_STATUS:
            fail(f"{path.name}: status must be one of {', '.join(MILESTONE_STATUS)}, not {fields['status']!r}")
        listed = [int(n) for n in fields["specs"]]
        if missing := [n for n in listed if n not in numbers]:
            fail(f"{path.name}: no spec {', '.join(map(str, missing))}")
        found.append(
            {
                "number": int(fields["number"]),
                "slug": path.stem,
                "title": fields["title"],
                "status": fields["status"],
                "versions": fields.get("versions", ""),
                "updated": fields["updated"],
                "description": fields["description"],
                "summary": section(path, body, "Summary"),
                "specs": listed,
                "issues": [int(n) for n in fields.get("issues", [])],
            }
        )
    found.sort(key=lambda m: m["number"])
    for m in found:
        for n in m["specs"]:
            numbers[n]["milestones"].append(m["slug"])
    if orphans := [s["number"] for s in specs_ if not s["milestones"] and s["status"] != "dropped"]:
        fail(f"specs {', '.join(map(str, orphans))} are in no milestone: add them to one in spec/milestones/")
    return found


def background() -> list[Page]:
    """The background pages: spec/*.md that aren't specs."""
    found = []
    for path in sorted(SPEC.glob("[a-z]*.md")):
        if path.name in NOT_PAGES:
            continue
        fields, body = split(path)
        need(path, fields, ("title", "updated"))
        summary = section(path, body, "Summary") if "## Summary" in body else ""
        found.append({"slug": path.stem, "title": fields["title"], "updated": fields["updated"], "summary": summary})
    return found


def versions_text(versions: list[str] | str) -> str:
    """Releases in words: `0.5.0, 0.7.0`, or `since 0.5.0` for a milestone's open range `0.5.0–`."""
    if isinstance(versions, str):
        return f"since {versions[:-1]}" if versions.endswith("–") else versions
    return ", ".join(versions)


def spec_header(spec: Page, by_slug: dict[str, Page], by_number: dict[int, Page]) -> str:
    """A spec's title and status line, as the file shows them on GitHub."""
    parts = [f"**{STATUS[spec['status']]}**", f"updated {spec['updated']}"]
    if spec["versions"]:
        parts.append(f"versions {versions_text(spec['versions'])}")
    links = ", ".join(f"[{by_slug[m]['title']}](milestones/{m}.md)" for m in spec["milestones"])
    parts.append(f"milestone{'s' if len(spec['milestones']) > 1 else ''}: {links}")
    related = [f"[spec {n}]({by_number[n]['slug']}.md)" for n in spec["related"] if n in by_number]
    line = " · ".join(parts) + ("\n· related: " + ", ".join(related) if related else "")
    return f"# {spec['number']}. {spec['title']}\n\n{line}\n\n"


def milestone_header(m: Page) -> str:
    """A milestone's title and status line."""
    parts = [f"**{MILESTONE_STATUS[m['status']]}**"]
    if m["versions"]:
        parts.append(f"versions {versions_text(m['versions'])}")
    parts.append(f"updated {m['updated']}")
    return f"# {m['title']}\n\n{' · '.join(parts)}\n\n"


def spec_table(listed: list[Page], prefix: str, columns: tuple[str, ...]) -> list[str]:
    """A Markdown table of specs, linked through `prefix`, with the given columns."""
    head = {"number": "#", "title": "Spec", "status": "Status", "versions": "Versions", "updated": "Updated"}
    rule = "|".join("---:" if c == "number" else "---" for c in columns)
    out = ["| " + " | ".join(head[c] for c in columns) + " |", f"|{rule}|"]
    for s in listed:
        cell = {
            "number": str(s["number"]),
            "title": f"[{s['title']}]({prefix}{s['slug']}.md)",
            "status": STATUS[s["status"]],
            "versions": versions_text(s["versions"]) or "–",
            "updated": s["updated"],
        }
        out.append("| " + " | ".join(cell[c] for c in columns) + " |")
    return out


def with_header(path: Path, header: str) -> str:
    """The file with everything between its front matter and `## Summary` replaced by `header`."""
    text = path.read_text()
    end = text.index("\n---\n", 4) + 5
    return text[:end] + "\n" + header + text[text.index("## Summary") :]


def between(text: str, start: str, end: str, content: str, name: str) -> str:
    """`text` with what is between the `start` and `end` markers replaced by `content`."""
    if start not in text or end not in text:
        fail(f"{name}: no {start} … {end} markers")
    before, rest = text.split(start, 1)
    _, after = rest.split(end, 1)
    return f"{before}{start}\n{content}{end}{after}"


def index(specs_: list[Page], background_: list[Page]) -> str:
    """spec/README.md's index: counts by status, a table per area, and the background pages."""
    out = []
    counts = ", ".join(f"{n} {STATUS[s].lower()}" for s in STATUS if (n := sum(x["status"] == s for x in specs_)))
    out += [f"{len(specs_)} specs: {counts}. Milestones group them on the [roadmap](milestones/README.md).", ""]
    for area in AREAS:
        out += [f"### {area}", ""]
        listed = [s for s in specs_ if s["area"] == area]
        out += spec_table(listed, "", ("number", "title", "status", "versions", "updated"))
        out.append("")
    out += ["### Background", ""]
    out += [f"- [{b['title']}]({b['slug']}.md)" + (f": {b['summary']}" if b["summary"] else "") for b in background_]
    return "\n" + "\n".join(out) + "\n\n"


def roadmap(specs_: list[Page], milestones_: list[Page]) -> str:
    """spec/milestones/README.md: the milestones by status, each with its specs."""
    by_number = {s["number"]: s for s in specs_}
    out = []
    for status, label in MILESTONE_STATUS.items():
        listed = [m for m in milestones_ if m["status"] == status]
        if not listed:
            continue
        out += [f"## {label}", ""]
        for m in listed:
            versions = f" · versions {versions_text(m['versions'])}" if m["versions"] else ""
            out += [f"### [{m['title']}]({m['slug']}.md)", "", f"**{label}**{versions}. {m['description']}", ""]
            specs_line = ", ".join(
                f"[{by_number[n]['title']}](../{by_number[n]['slug']}.md) ({STATUS[by_number[n]['status']].lower()})"
                for n in m["specs"]
            )
            out += [f"Specs: {specs_line}.", ""]
    return "\n" + "\n".join(out)


def json_list(entries: list[Page]) -> str:
    """A list of flat objects, formatted as oxfmt would format it."""
    blocks = []
    for entry in entries:
        lines = [f"    {json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}" for k, v in entry.items()]
        blocks.append("  {\n" + ",\n".join(lines) + "\n  }")
    return "[\n" + ",\n".join(blocks) + "\n]\n"


def ts_key(name: str) -> str:
    """An object key as oxfmt writes it: quoted only where it must be."""
    return name if name.isidentifier() else f'"{name}"'


def pages_ts(specs_: list[Page], milestones_: list[Page], background_: list[Page]) -> str:
    """The website's imports of each page's Markdown, rendered at build time."""

    def record(name: str, paths: list[tuple[str, str]]) -> str:
        imports = "\n".join(f'  {ts_key(slug)}: () => import("@spec/{path}"),' for slug, path in paths)
        return f"export const {name}: Record<string, () => Promise<{{ default: MDXContent }}>> = {{\n{imports}\n}}\n"

    pages = [(s["slug"], f"{s['slug']}.md") for s in specs_] + [(b["slug"], f"{b['slug']}.md") for b in background_]
    return (
        "// Generated by scripts/spec_index.py: each page's Markdown (`@spec/*` is ../spec), rendered at build time.\n"
        'import type { MDXContent } from "mdx/types"\n\n'
        "/** Specs and background pages, at /spec/<slug>. */\n"
        + record("SPEC_PAGES", pages)
        + "\n/** Milestones, at /roadmap/<slug>. */\n"
        + record("MILESTONE_PAGES", [(m["slug"], f"milestones/{m['slug']}.md") for m in milestones_])
    )


def outputs() -> dict[Path, str]:
    """Every derived file and what it should hold."""
    specs_ = specs()
    milestones_ = milestones(specs_)
    background_ = background()
    by_slug = {m["slug"]: m for m in milestones_}
    by_number = {s["number"]: s for s in specs_}
    out: dict[Path, str] = {}
    for s in specs_:
        path = SPEC / f"{s['slug']}.md"
        out[path] = with_header(path, spec_header(s, by_slug, by_number))
    for m in milestones_:
        path = MILESTONES / f"{m['slug']}.md"
        rows = spec_table([by_number[n] for n in m["specs"]], "../", ("number", "title", "status", "versions"))
        table = "\n## Specs\n\n" + "\n".join(rows) + "\n\n"
        text = with_header(path, milestone_header(m))
        out[path] = between(text, "<!-- specs:start -->", "<!-- specs:end -->", table, path.name)
    readme = SPEC / "README.md"
    listing = index(specs_, background_)
    out[readme] = between(readme.read_text(), "<!-- index:start -->", "<!-- index:end -->", listing, "README.md")
    road = MILESTONES / "README.md"
    plan = roadmap(specs_, milestones_)
    out[road] = between(
        road.read_text(), "<!-- roadmap:start -->", "<!-- roadmap:end -->", plan, "milestones/README.md"
    )
    out[LIB / "specs.json"] = json_list(specs_)
    out[LIB / "milestones.json"] = json_list(milestones_)
    out[LIB / "background.json"] = json_list(background_)
    out[LIB / "spec-pages.ts"] = pages_ts(specs_, milestones_, background_)
    return out


def main() -> None:
    """Rewrite the derived files, or with `--check` fail if one is out of date."""
    written = outputs()
    if "--check" in sys.argv:
        stale = [str(p.relative_to(ROOT)) for p, t in written.items() if not p.exists() or p.read_text() != t]
        if stale:
            fail(f"{', '.join(stale)} out of date: uv run python scripts/spec_index.py")
        return
    for path, content in written.items():
        if not path.exists() or path.read_text() != content:
            path.write_text(content)


if __name__ == "__main__":
    main()
