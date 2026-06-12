# M0 — Skeleton & Guardrails: Design

Date: 2026-06-12
Status: Approved (2026-06-12)
Master plan: [2026-06-12-trading-intelligence-design.md](2026-06-12-trading-intelligence-design.md)

## Scope

Turn the empty repository into a Cargo workspace + Python scaffold where every
quality gate is active and CI is green. No business logic. Everything M1+ builds on
lands inside guardrails instead of acquiring them later.

**Done when:** CI is green on a hello-world binary; `prek run` passes locally.

Out of scope: any dependency or code that belongs to a later milestone (no barter-rs,
sqlx, axum, leptos yet — they are added by the milestone that uses them).

## Workspace layout

```
rr/
├── Cargo.toml              # virtual workspace (no root package)
├── rust-toolchain.toml     # channel = "stable"
├── deny.toml               # cargo-deny: advisories, licenses, bans
├── .pre-commit-config.yaml # prek hooks
├── .github/workflows/ci.yml
├── crates/
│   ├── rr-cli/             # binary `rr` — clap skeleton (version/help only in M0)
│   ├── rr-engine/          # lib shell
│   ├── rr-kraken/          # lib shell
│   ├── rr-storage/         # lib shell
│   ├── rr-server/          # lib shell
│   └── rr-dashboard/       # lib shell (plain lib in M0; Leptos/WASM arrives in M3)
├── analytics/              # Python package `rr-analytics` (uv, ruff, ty, pytest)
└── docs/
```

Changes to existing files: the current root `Cargo.toml` (package `rr`) and
`src/main.rs` are replaced by the virtual workspace + `crates/rr-cli`. The `rr`
binary name is kept, produced by `rr-cli`.

A "lib shell" is a compilable `lib.rs` containing only a crate-level doc comment
stating the crate's responsibility (from `docs/architecture.md`). No placeholder
types, no `todo!` (denied by lints anyway).

## Workspace Cargo.toml

- `[workspace]` with `resolver = "3"`, members `crates/*`.
- `[workspace.package]`: edition 2024, version 0.1.0, license field per the license
  decision below; all crates inherit.
- `[workspace.lints]`: the standard clippy block (pedantic warn; deny unwrap_used,
  panic, todo, unimplemented, dbg_macro, print_stdout/stderr, exit, mem_forget,
  await_holding_lock, large_futures, allow_attributes; warn expect_used; allow
  module_name_repetitions, similar_names) plus `rust.unsafe_code = "deny"` —
  nothing in this system justifies unsafe. Every crate sets `[lints] workspace = true`.
- `[workspace.dependencies]`: only `clap` (derive) in M0, for `rr-cli`. Exact version
  resolved at implementation-plan time from crates.io (never from memory).
- Exception note: `rr-cli` is a CLI — printing is its job. It uses a single
  documented `#[expect(clippy::print_stdout)]`-style narrow allowance at the output
  boundary rather than relaxing the workspace lint.

No `rustfmt.toml`: defaults already match the 100-char standard.

## cargo-deny (deny.toml)

- `[advisories]`: deny vulnerabilities and unmaintained warnings surfaced as errors.
- `[licenses]`: allowlist MIT, Apache-2.0, BSD-2/3-Clause, ISC, Unicode-3.0.
  Anything else fails until explicitly reviewed and added.
- `[bans]`: `multiple-versions = "warn"` (informational), wildcards denied.

## prek hooks (.pre-commit-config.yaml)

Standard hygiene hooks (trailing-whitespace, end-of-file-fixer, check-yaml,
check-toml, check-merge-conflict, check-added-large-files) plus local hooks:

| Hook | Command | Runs on |
|------|---------|---------|
| fmt | `cargo fmt --all --check` | *.rs |
| clippy | `cargo clippy --all-targets --all-features -- -D warnings` | *.rs |
| ruff | `ruff check` + `ruff format --check` (in analytics/) | *.py |
| actionlint | `actionlint` | workflow files |

`prek install` documented in README; auto-update cooldown 7 days.

## CI (.github/workflows/ci.yml)

Triggers: pull_request, push to main. All actions pinned to full SHA with version
comment; `persist-credentials: false` on checkout. Workflow linted with `actionlint`
and audited with `zizmor` before commit. Jobs:

1. **rust** — stable toolchain + cache: `cargo fmt --all --check`,
   `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --workspace`,
   `cargo build --workspace`.
2. **deny** — `cargo deny check` via the official action.
3. **python** — uv setup, `ruff check`, `ruff format --check`, `ty check`,
   `pytest -q` (working-directory: analytics/).

Action SHAs and tool versions are resolved at implementation-plan time.

## Python scaffold (analytics/)

- `uv init`-based package `rr-analytics`, Python 3.13, build backend `uv_build`.
- `pyproject.toml` configures ruff (line length 100) and `[tool.ty.rules]` strict.
- One real module (`rr_analytics/__init__.py` with a version constant) and one
  trivial pytest test in `analytics/tests/`, so every Python gate has something to
  verify from day one.
- Dependencies pinned exact (`==`) when they arrive (M6); none in M0 beyond dev
  tools (ruff, ty, pytest as dev dependencies).

## .gitignore

`/target`, `analytics/.venv`, `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`,
`/data/` (future SQLite + Parquet live under `data/`, never committed), `.env`.

## rr-cli in M0

Clap-based skeleton: `rr --version` and `rr --help` work; no subcommands yet
(M1 adds `stream` and `archive-status`). One test asserting the clap command
definition is valid (`Command::debug_assert`), one integration-style test running
`rr --version` via `assert_cmd` — exercises the test harness end to end.

## Test strategy

M0's product *is* the guardrails, so verification is running them:
`prek run --all-files` clean, `cargo test --workspace` green (the two rr-cli tests),
`pytest -q` green (one test), CI green on the PR. Negative check: a deliberate
`unwrap()` added locally must fail clippy (verifies lints actually bite), then is
removed — documented in the implementation plan as a step.

## Workflow

Branch `m0-skeleton`, PR to `main`, merged on green CI after review.

## Resolved questions

1. **GitHub remote** — private repository exists: `git@github.com:mjaric/rr.git`.
   Configured as `origin`; CI and PR workflow active from M0.
2. **License** — proprietary (all rights reserved). No LICENSE files; every crate
   sets `publish = false` so nothing can be pushed to crates.io accidentally, and a
   copyright notice goes in the README. As the sole copyright holder, Milan can later
   relicense and open-source `rr-kraken` alone (it must therefore avoid depending on
   other `rr-*` crates — barter-data only).
