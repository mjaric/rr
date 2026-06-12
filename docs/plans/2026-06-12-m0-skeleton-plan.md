# M0 — Skeleton & Guardrails: Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement
> this plan task-by-task.

**Goal:** Turn the empty repo into a Cargo workspace + Python scaffold where every
quality gate (fmt, clippy, cargo-deny, prek, ruff, ty, pytest, CI) is active and green.

**Architecture:** Virtual Cargo workspace with six crates (`rr-cli` binary + five lib
shells), `analytics/` Python package, guardrails per the approved design
[2026-06-12-m0-skeleton-design.md](2026-06-12-m0-skeleton-design.md). No business
logic, no future-milestone dependencies.

**Tech Stack:** Rust stable (1.91.1), clap 4.6.1, assert_cmd 2.2.2 (dev), uv,
ruff 0.15.17, ty 0.0.49, pytest 9.0.3, prek, cargo-deny, actionlint, zizmor.

**Prerequisite:** `origin` main must exist (Milan runs `git push -u origin main`
once — hook blocks agents from pushing main).

---

### Task 1: Feature branch

**Step 1:** `git switch -c m0-skeleton`
Expected: `Switched to a new branch 'm0-skeleton'`

### Task 2: Workspace root + rr-cli stub

**Files:**
- Replace: `Cargo.toml` (root)
- Create: `rust-toolchain.toml`, `crates/rr-cli/Cargo.toml`, `crates/rr-cli/src/main.rs`
- Replace: `.gitignore`
- Delete: `src/main.rs` (old root binary)

**Step 1:** Root `Cargo.toml` (replace entirely):

```toml
[workspace]
resolver = "3"
members = ["crates/*"]

[workspace.package]
version = "0.1.0"
edition = "2024"
publish = false

[workspace.dependencies]
clap = { version = "4.6.1", features = ["derive"] }
assert_cmd = "2.2.2"

[workspace.lints.rust]
unsafe_code = "deny"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
# Panic prevention
unwrap_used = "deny"
expect_used = "warn"
panic = "deny"
panic_in_result_fn = "deny"
unimplemented = "deny"
# No cheating
allow_attributes = "deny"
# Code hygiene
dbg_macro = "deny"
todo = "deny"
print_stdout = "deny"
print_stderr = "deny"
# Safety
await_holding_lock = "deny"
large_futures = "deny"
exit = "deny"
mem_forget = "deny"
# Pedantic relaxations (too noisy)
module_name_repetitions = "allow"
similar_names = "allow"
```

**Step 2:** `rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
components = ["clippy", "rustfmt"]
```

**Step 3:** `.gitignore` (replace entirely):

```gitignore
/target
/data
.env
analytics/.venv
__pycache__/
.pytest_cache/
.ruff_cache/
```

**Step 4:** `crates/rr-cli/Cargo.toml`:

```toml
[package]
name = "rr-cli"
version.workspace = true
edition.workspace = true
publish.workspace = true

[[bin]]
name = "rr"
path = "src/main.rs"

[dependencies]
clap = { workspace = true }

[dev-dependencies]
assert_cmd = { workspace = true }

[lints]
workspace = true
```

**Step 5:** `crates/rr-cli/src/main.rs` (minimal compilable stub; CLI arrives in Task 3):

```rust
//! `rr` — trading intelligence app command-line interface.

fn main() {}
```

**Step 6:** Delete the old root binary: `rm src/main.rs && rmdir src`
(old `src/main.rs` was untracked scaffolding from `cargo init`; nothing references it)

**Step 7:** Verify: `cargo build --workspace && cargo fmt --all --check && cargo clippy --all-targets --all-features -- -D warnings`
Expected: builds clean, zero warnings.

**Step 8:** Commit:

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml .gitignore crates/
git commit -m "Convert repo to virtual workspace with rr-cli stub"
```

### Task 3: rr-cli clap skeleton (TDD)

**Files:**
- Modify: `crates/rr-cli/src/main.rs`
- Create: `crates/rr-cli/tests/cli.rs`

**Step 1: Write the failing unit test** — append to `crates/rr-cli/src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }
}
```

**Step 2:** Run: `cargo test -p rr-cli`
Expected: FAIL — `cannot find type 'Cli'`.

**Step 3: Implement** — `crates/rr-cli/src/main.rs` becomes:

```rust
//! `rr` — trading intelligence app command-line interface.

use clap::Parser;

/// Simulation-first crypto trading agent.
#[derive(Parser)]
#[command(name = "rr", version, about)]
struct Cli {}

fn main() {
    let Cli {} = Cli::parse();
}
```

(keep the test module from Step 1 below it)

**Step 4:** Run: `cargo test -p rr-cli`
Expected: PASS (1 test).

**Step 5: Write the integration test** — `crates/rr-cli/tests/cli.rs`:

```rust
//! End-to-end smoke test for the `rr` binary.

use assert_cmd::Command;

#[test]
#[expect(clippy::panic, reason = "test failure path")]
fn version_flag_succeeds() {
    let Ok(mut cmd) = Command::cargo_bin("rr") else {
        panic!("rr binary not found");
    };
    cmd.arg("--version").assert().success();
}
```

Test idiom for this repo: lints stay strict everywhere; a test that must
panic/unwrap carries a narrow `#[expect(lint, reason = ...)]` on the test fn.

**Step 6:** Run: `cargo test -p rr-cli && cargo clippy --all-targets --all-features -- -D warnings`
Expected: 2 tests PASS, clippy clean.

**Step 7:** Commit: `git add crates/rr-cli && git commit -m "Add clap CLI skeleton with version smoke tests"`

### Task 4: Lib shells

**Files:** Create `crates/<name>/Cargo.toml` + `crates/<name>/src/lib.rs` for
`rr-engine`, `rr-kraken`, `rr-storage`, `rr-server`, `rr-dashboard`.

**Step 1:** Each `Cargo.toml` (substitute the name):

```toml
[package]
name = "rr-engine"
version.workspace = true
edition.workspace = true
publish.workspace = true

[lints]
workspace = true
```

**Step 2:** Each `lib.rs` is a crate-level doc comment only. Responsibilities from
`docs/architecture.md`:

| Crate | lib.rs doc comment |
|-------|--------------------|
| rr-engine | `//! Trading engine: strategies, risk manager, mode runner (M2).` |
| rr-kraken | `//! Kraken market-data connector for barter-data (M5). Must depend only on barter-data — planned for separate open-sourcing.` |
| rr-storage | `//! Persistence: SQLite operational state + Parquet market-data archive (M1).` |
| rr-server | `//! Axum API server and WebSocket fan-out of engine events (M3).` |
| rr-dashboard | `//! Leptos (WASM) dashboard frontend (M3).` |

**Step 3:** Verify: `cargo build --workspace && cargo clippy --all-targets --all-features -- -D warnings && cargo test --workspace`
Expected: 6 crates compile, 2 tests pass, zero warnings.

**Step 4:** Commit: `git add crates/ Cargo.lock && git commit -m "Add lib shell crates per architecture"`

### Task 5: Verify lints bite (negative check — no commit)

**Step 1:** Append to `crates/rr-engine/src/lib.rs`:

```rust
pub fn bad() -> i32 {
    Option::<i32>::None.unwrap()
}
```

**Step 2:** Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: **FAIL** with `error: used unwrap() on an Option value` (and missing-docs
pedantic warnings). If it passes, STOP — the lint wiring is broken; fix before
continuing.

**Step 3:** Revert: `git checkout crates/rr-engine/src/lib.rs`, re-run clippy.
Expected: clean.

### Task 6: cargo-deny

**Files:** Create `deny.toml`

**Step 1:**

```toml
[advisories]
yanked = "deny"

[licenses]
allow = [
    "MIT",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Unicode-3.0",
]
# Our own crates are proprietary and unpublished (publish = false)
private = { ignore = true }

[bans]
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

**Step 2:** Run: `cargo deny check`
Expected: PASS (advisories, licenses, bans, sources all green). If a transitive
license (e.g. `Unicode-DFS-2016`) fails, review it and extend the allowlist with a
comment — do not disable the check.

**Step 3:** Commit: `git add deny.toml && git commit -m "Add cargo-deny supply chain checks"`

### Task 7: Python analytics scaffold

**Files:** Create `analytics/` package

**Step 1:** `uv init --package --python 3.13 analytics` then rename project to
`rr-analytics` in `analytics/pyproject.toml` (module dir: `analytics/src/rr_analytics/`).

**Step 2:** Add to `analytics/pyproject.toml`:

```toml
[dependency-groups]
dev = ["pytest==9.0.3", "ruff==0.15.17", "ty==0.0.49"]

[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4", "SIM", "RUF"]
```

(ty stays on defaults until real code arrives in M6; tighten `[tool.ty.rules]` then.)

**Step 3:** `analytics/src/rr_analytics/__init__.py`:

```python
"""Advisory analytics for the rr trading agent. Reads SQLite/Parquet, never trades."""

__version__ = "0.1.0"
```

**Step 4: Failing test** — `analytics/tests/test_package.py`:

```python
"""Smoke test: package imports and reports its version."""

from rr_analytics import __version__


def test_version() -> None:
    assert __version__ == "0.1.0"
```

Run: `cd analytics && uv sync && uv run pytest -q`
Expected: PASS (1 test). (Import error if Step 3 was skipped — that's the failing-first check.)

**Step 5:** Run all gates: `uv run ruff check && uv run ruff format --check && uv run ty check`
Expected: all clean. Fix anything reported; zero-warnings policy applies.

**Step 6:** Commit: `git add analytics && git commit -m "Add Python analytics scaffold with quality gates"`
(ensure `analytics/uv.lock` is included)

### Task 8: prek hooks

**Files:** Create `.pre-commit-config.yaml`

**Step 1:**

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v6.0.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-toml
      - id: check-merge-conflict
      - id: check-added-large-files
  - repo: local
    hooks:
      - id: cargo-fmt
        name: cargo fmt
        entry: cargo fmt --all -- --check
        language: system
        types: [rust]
        pass_filenames: false
      - id: cargo-clippy
        name: cargo clippy
        entry: cargo clippy --all-targets --all-features -- -D warnings
        language: system
        types: [rust]
        pass_filenames: false
      - id: ruff-check
        name: ruff check
        entry: bash -c 'cd analytics && uv run ruff check'
        language: system
        types: [python]
        pass_filenames: false
      - id: ruff-format
        name: ruff format
        entry: bash -c 'cd analytics && uv run ruff format --check'
        language: system
        types: [python]
        pass_filenames: false
      - id: actionlint
        name: actionlint
        entry: actionlint
        language: system
        files: ^\.github/workflows/.*\.ya?ml$
        pass_filenames: false
```

**Step 2:** Run: `prek install && prek run --all-files`
Expected: all hooks pass (end-of-file-fixer may auto-fix files — re-add and re-run
until clean).

**Step 3:** Commit: `git add .pre-commit-config.yaml && git commit -m "Add prek hooks"`

### Task 9: CI workflow

**Files:** Create `.github/workflows/ci.yml`

**Step 1:**

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10 # v6.0.3
        with:
          persist-credentials: false
      - uses: dtolnay/rust-toolchain@e97e2d8cc328f1b50210efc529dca0028893a2d9 # v1
        with:
          toolchain: stable
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@c19371144df3bb44fab255c43d04cbc2ab54d1c4 # v2.9.1
      - run: cargo fmt --all --check
      - run: cargo clippy --all-targets --all-features -- -D warnings
      - run: cargo test --workspace
      - run: cargo build --workspace

  deny:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10 # v6.0.3
        with:
          persist-credentials: false
      - uses: EmbarkStudios/cargo-deny-action@bb137d7af7e4fb67e5f82a49c4fce4fad40782fe # v2.0.20

  python:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: analytics
    steps:
      - uses: actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10 # v6.0.3
        with:
          persist-credentials: false
      - uses: astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39 # v8.2.0
      - run: uv sync --locked
      - run: uv run ruff check
      - run: uv run ruff format --check
      - run: uv run ty check
      - run: uv run pytest -q
```

**Step 2:** Lint + audit: `actionlint .github/workflows/ci.yml && zizmor .github/workflows/ci.yml`
Expected: both clean. Fix every finding (zero-warnings policy).

**Step 3:** Commit: `git add .github && git commit -m "Add CI: rust, cargo-deny, python jobs"`

### Task 10: Documentation updates

**Files:** Modify `README.md`, `CLAUDE.md`

**Step 1:** README: add under Disclaimer —
`Copyright © 2026 Milan Jaric. All rights reserved. Proprietary — no license granted.`
Set roadmap M0 status to `in review`, and add a Development section: clone, `prek install`,
`cargo build --workspace`, `cd analytics && uv sync`.

**Step 2:** CLAUDE.md repository conventions: append the test-lint idiom
(`#[expect(lint, reason = ...)]` on test fns that must panic/unwrap) and the
`rr-kraken` isolation rule (depends only on barter-data; planned open-sourcing).

**Step 3:** `prek run --all-files` then commit:
`git add README.md CLAUDE.md && git commit -m "Document development setup, copyright, and test lint idiom"`

### Task 11: PR and merge

**Step 1:** `git push -u origin m0-skeleton`

**Step 2:** `gh pr create --title "M0: skeleton & guardrails" --body` — summarize
what the PR contains (workspace, lints, deny, prek, analytics scaffold, CI) and the
done-when evidence.

**Step 3:** `gh pr checks --watch`
Expected: all three CI jobs green. If a job fails, fix on the branch — never weaken
a gate to pass.

**Step 4:** REQUIRED SUB-SKILL: superpowers:finishing-a-development-branch — review
and merge via Milan's approval; update README roadmap M0 → `done` in the merge or a
follow-up commit on the PR.

**Done-when verification (against master plan):** CI green on hello-world binary ✅
(Task 11 Step 3); `prek run` passes locally ✅ (Tasks 8/10).
