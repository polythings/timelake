# Contributing to Timelake

This is the code guide for everyone who changes this repository, people and AI agents alike. Formatters, linters and
type checkers enforce the mechanical rules (see [Checks](#checks)). This guide covers what they can't: how code should
read.

New here? Read [How it works](https://timelake.dev/docs/how-it-works) and the
[Codebase tour](https://timelake.dev/docs/codebase) first. The [specs](spec/README.md) hold the design, one numbered spec per feature, with its status;
milestones made of specs are the [roadmap](spec/milestones/README.md).

Found a security problem? Don't open an issue: report it privately as [SECURITY.md](SECURITY.md) describes.

## Principles

1. **Readable first.** Someone new should understand a function from its name, its signature and one pass through
   the body. Fast code and clear code are not opposites: name the trick and explain why it's there.
2. **Idiomatic.** Write each language the way its community does. Rust should read like Rust, not like Java or C++.
   Python should read like modern typed Python.
3. **Modular.** Each crate and package has one job and a small public API, and could move to its own repository.
   Shared rules live in one place (for example, `timelake-format` for the table conventions).
4. **One definition of each thing.** Before writing a helper, constant or type, look for an existing one. Copying is
   how the last cleanup started.
5. **Behaviour, docs and tests change together.** A pull request that changes what users see also updates the docs
   ([dashboard/app/(site)/docs](dashboard/app/(site)/docs)), adds or updates tests, and adds a line to
   [CHANGELOG.md](CHANGELOG.md).

## Comments and docs

- **Public items get a short doc comment**: what it does, and anything a caller must know (errors, units, invariants).
  One to three lines is usually enough.
- **Comments explain why, not what.** Don't restate the code. Do explain a non-obvious choice, a workaround, or a
  number that came from a measurement.
- **No long comments.** If an explanation needs a page, it belongs in a spec or the docs. Link to it from the code.

```rust
// Bad: restates the code.
// Loop over the files and add their sizes.
let bytes: u64 = files.iter().map(|f| f.size).sum();

// Good: says why.
// Object stores bill per request, so small files are merged once 8 of a similar size pile up.
const COMPACT_MIN_FILES: usize = 8;
```

## Credit

Timelake learns from other projects' ideas, never from copying their code. [PRIOR_ART.md](PRIOR_ART.md) lists the
ideas we took and the projects we're studying.

- **Never copy code**, whatever its license: not by pasting, and not by translating it into Rust. Understand the
  method, then write our own, better code for it.
- **Credit ideas from open-source projects**: ones under a single open-source license, best of all run by a
  foundation, such as the Apache Software Foundation's (Iceberg, Parquet, Cassandra) or DuckDB. Add a row in
  PRIOR_ART.md and a short comment where the idea is implemented, for example
  `// Size-tiered compaction, as in Apache Cassandra.` The docs may name them too.
- **Don't name open-core, dual-licensed or source-available projects, or commercial products, as a source.** Learn
  from their public docs at most, and don't present a Timelake feature as taken from them in code, docs or on the
  website. Naming them in comparisons and benchmark results is fine.
- **Third-party files used as they are** (fonts, generated UI components, benchmark query sets) keep their license
  in [NOTICE](NOTICE). Files whose license doesn't fit Apache-2.0 are downloaded when needed, not committed.
- **Studying a project** without adopting anything yet still gets notes in PRIOR_ART.md, so the next person
  doesn't start from zero.

## Rust

- **Errors:** `thiserror` enums in libraries, `anyhow` in binaries. Keep the cause: use `#[from]` or `#[source]`,
  never `map_err(|e| e.to_string())`. No `unwrap()` outside tests. Use `expect("why this can't fail")` only when it
  truly can't.
- **Types over strings:** enums, newtypes and `FromStr`/`Display` instead of strings and magic numbers. Parse input
  once at the boundary (for example, `TableName`) and pass the typed value along.
- **Small functions:** split anything over roughly 60 lines, or anything with more than 4 or 5 parameters (use a
  struct). Return an enum rather than `Option` plus an out-parameter.
- **Ownership:** borrow rather than clone; share with `Arc` for large immutable data. Don't hold a `Mutex` across an
  `.await`.
- **Async:** issue independent I/O concurrently (`try_join_all`, buffered streams) rather than awaiting in a loop.
  Never block inside async code.
- **Suppressing a lint:** use `#[expect(lint, reason = "...")]` with a real reason. Never use `#[allow]`, and never
  relax the workspace lints to make a check pass.
- **Public API:** keep fields private when there's an invariant, and expose methods. Prefer `pub(crate)` for
  anything callers don't need.

## Python

- **Types everywhere:** annotate every function. Use `X | None`, built-in generics, `TypedDict` for dict-shaped data,
  `Protocol` for duck typing, and `@dataclass(frozen=True)` for configuration. `ty` must pass.
- **Docstrings:** Google style. One line is enough when the signature says the rest.
- **No hidden state:** no module-level mutable globals, nothing read from argv or the environment at import time,
  and no `sys.path` tricks. Pass a config object instead.
- **Specific exceptions:** raise and catch specific types, never a bare `except Exception` without a reason.
  `subprocess.run(..., check=True)` whenever failure must stop.
- **Paths and time:** use `pathlib` for paths, and timezone-aware UTC for anything stored.
- **Bindings:** the native module's types live in `crates/timelake-py/python/timelake/_timelake.pyi`. Update the stub
  with every change to `src/lib.rs`; `tests/test_package.py` checks that they match.
- **Shared benchmark helpers** go in `bench/timebench`.

## TypeScript

- **Strict types:** no `any`, no non-null assertions (`!`), and code that holds up under `noUncheckedIndexedAccess`.
  Validate data from outside (requests, JSON, files) with zod at the boundary.
- **Share types and helpers:** import types across the server/client boundary with `import type` instead of
  redefining them. Put formatters and constants in `dashboard/lib`.
- **React:** keep `"use client"` to the components that need it. Don't define components inside components. Split
  components that grow past one screen.
- **Promises:** never leave one floating. Handle the error, or mark intent with `void`.
- **Style:** no semicolons and double quotes. oxfmt handles the rest.

## Tests

- **A bug fix starts with a test that fails**, then the fix.
- **Tests run offline** and on their own: no network, no AWS, no shared state between tests. Build fixtures in a temp
  dir, or commit small ones under `tests/fixtures/`. Fixtures stay byte-exact.
- **Test behaviour, not implementation**: query results, files on disk, errors. Avoid matching `EXPLAIN` strings or
  log text.
- **Benchmarks are not tests.** They run by hand (`timebench`) or in the weekly run on AWS, never in CI.

## Checks

Install once: `uv sync && bun install && uv tool install prek && prek install`. Then hooks run on commit (formatters,
fast lints) and on push (clippy, ty, tsc). Before you push, or before calling work done:

```bash
prek run --all-files --hook-stage pre-push                                  # every formatter, linter and type checker
cargo test --workspace                                                      # Rust
uv run pytest crates/timelake-py/tests && uv run pytest bench/tests         # Python
cargo build -p timelake-cli && (cd packages/timelake-client && bun test)    # TypeScript client vs the engine
```

After changing `crates/timelake-wasm` or `crates/timelake-format`, rebuild the embedded WASM with
`(cd packages/timelake-client && bun run build:wasm)`. CI runs all of the above on every push and pull request.

## Dependencies

Keep dependencies on their newest versions. [Dependabot](.github/dependabot.yml) opens a pull request each week per
ecosystem (Actions, Cargo, the Rust toolchain, uv, Bun, Go) with the minor and patch updates, and one per major
update; security updates come at once. It waits until a version is 7 days old, since most compromised releases are
pulled within days. Merge its pull requests once CI passes, fixing the code on the branch when an update breaks it.

- **Actions are pinned to a commit**, with the version in a comment (`uses: actions/checkout@<sha> # v7.0.1`), so a
  moved tag can't change what runs. Dependabot updates both.
- **`release.yml`** is generated by dist from the pins in `dist-workspace.toml`. When Dependabot updates the actions
  in it, CI fails until you run `uv run python scripts/sync_dist_pins.py` on the branch and commit the result.
- **Arrow, Parquet, object_store, Avro and PyO3** follow the newest DataFusion, which fixes the Arrow version (PyO3
  follows `arrow-pyarrow`, and `pythonize` follows PyO3). `iceberg` is
  [`polythings-iceberg`](https://github.com/polythings/iceberg-rust), iceberg 0.10.1 with only its Arrow version
  raised, until an iceberg-rust release is on DataFusion's Arrow. Dependabot only proposes their compatible updates;
  when a DataFusion major comes out, raise the fork's Arrow (a new `polythings-iceberg`, its minor version the Arrow
  major) and update them together.
- **Held back on purpose:** Geist in `brand/` (the logo lockup is outlined from 1.3.1) and the major version of
  `@types/node` (it matches the Node.js the website runs on). The reasons are in `dependabot.yml`.
- **Tool versions:** Rust in `rust-toolchain.toml` (and in `crates/timelake-wasm/rust-toolchain.toml`, which adds the
  WebAssembly target), Python in `.python-version`, Bun in `packageManager` in `dashboard/package.json`, Go in
  `go.mod`. The workflows read them from there.

## Commits

- **Subject:** a short imperative sentence with the area first, for example `Engine: …`, `CLI: …`, `Python: …`,
  `Client: …`, `Site: …`, `Docs: …`, `bench: …` or `Spec: …`. Add a body when the why isn't obvious.
- **Scope:** one logical change per commit. Formatting-only commits go in `.git-blame-ignore-revs`.
- **Secrets:** never commit credentials or `.env*` files (`.env.example` is the exception).

## Changelog

[CHANGELOG.md](CHANGELOG.md) is for users, not a list of commits. The website renders it at
[timelake.dev/docs/changelog](https://timelake.dev/docs/changelog), so links in it use full URLs.

- **What gets a line:** anything a user can notice: a feature, a behaviour or API change, a bug fix, a performance gain
  big enough to mention, a change to the files a table writes. Refactors, CI and docs-only changes don't.
- **Where:** under `## Unreleased`, in `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed` or `Security`, in the same
  commit as the change.
- **How:** one line, written for someone using Timelake. Say what changed for them and, for a breaking change, what to
  do. For example, "Commits make one round trip fewer and take about half as long", not "Engine: parallel uploads".

## Versions and releases

Everything shares one version: the Rust crates, the CLI, the Python package, the WASM reader, the TypeScript client
and the website. It follows [Semantic Versioning as Cargo applies it](https://doc.rust-lang.org/cargo/reference/semver.html):
before 1.0, a minor release (0.3 → 0.4) may break the API or behaviour, and its changelog entry says what to do; a
patch release (0.3.0 → 0.3.1) never does. Tables carry their own format version (`timelake.format-version`,
`FORMAT_VERSION` in `timelake-format`), a semantic version separate from the software's. Bump it when the files a
table writes change:

- **patch** (0.2.0 → 0.2.1) when older Timelakes still read and write the table correctly, for example a new
  optional section in the head that writers needn't keep;
- **minor** before 1.0 (0.2 → 0.3) when older ones would read it wrongly or break it by writing. From 1.0, a minor bump
  is for changes only older writers must avoid, and a major bump for changes older readers must avoid.

Teach `timelake upgrade` to bring older tables to it, and add the format to the table in the
[Upgrading](https://timelake.dev/docs/upgrading#table-format) docs.

Release when a set of changes is worth installing, not after every change: pushes to `main` publish nothing, and
changes collect under `## Unreleased` until the next release. A release takes about 40 minutes of CI and a
maintainer's approval per registry, and every version stays on the registries for good. Cut a patch release sooner
for a bug fix users are waiting on.

To release, from a clean `main`:

```bash
cargo install cargo-semver-checks --locked     # once
uv run python scripts/release.py 0.4.0         # checks, bumps, changelog section, commit, tag v0.4.0
uv run python scripts/release.py 0.4.0 --push  # the same, then pushes main, waits for CI, and pushes the tag
```

The script runs `cargo semver-checks` on the library crates against the previous tag, so a patch release can't
break the API, then sets the version in every package, turns `## Unreleased` into the release's section, rebuilds
the WASM bundle, runs the checks, commits and tags. With `--push`, it pushes `main` first and waits for CI to pass on
that commit; only then does it push the tag. If CI fails, the tag stays local: fix `main` and tag the fixed commit.
`publish.yml` checks the same before it publishes anything, so a tag on a commit without a passing CI run publishes
nothing. CI runs every commit on `main` to the end (only a pull request's older runs are cancelled), so every release
commit has a run.

Pushing the tag starts two workflows:

- [`release.yml`](.github/workflows/release.yml), generated by [dist](https://opensource.axo.dev/cargo-dist/) from
  `dist-workspace.toml`, builds the `timelake` CLI for Linux, macOS and Windows with installers
  (`curl … | sh`, PowerShell) and signed build attestations, and creates the GitHub release with this version's
  changelog section. Don't edit it by hand: change `dist-workspace.toml` and run `dist generate`.
- [`publish.yml`](.github/workflows/publish.yml) builds the Python wheels and source distribution and the npm
  package, and once all of them build, publishes `timelake-format`, `timelake` and `timelake-cli` to crates.io,
  `timelake` to PyPI and `@timelake/client` to npm. It uses trusted publishing: each registry accepts a short-lived
  token that GitHub issues to this workflow, so no registry token is stored. Each publish waits for a maintainer's
  approval (GitHub environments `crates-io`, `pypi`, `npm`). A version a registry already has is skipped, so a failed
  run can be re-run. Run it by hand (Actions → Publish → Run workflow) to build everything and dry-run each publish.

`timelake-py` goes to PyPI, not crates.io, and `timelake-wasm` ships inside the npm package. The npm package
publishes `dist/`, which `npm pack` builds (`bun run build`); in this repository the website imports the client's
TypeScript source through a path alias in `dashboard/tsconfig.json`.

## License

Contributions are licensed under the Apache License 2.0, like the rest of the project (see [LICENSE](LICENSE)).
