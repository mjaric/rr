---
description: Start or resume work on a roadmap milestone (design → plan → implement → verify)
argument-hint: [milestone, e.g. M1 — blank picks the next one from the roadmap]
---

You are picking up milestone work in the rr repository. The process in CLAUDE.md is
binding; this command tells you where to start.

## 1. Establish context

Read, in order: `README.md` (roadmap + status), `docs/architecture.md` (system
model), and the master plan `docs/plans/2026-06-12-trading-intelligence-design.md`
(the target milestone's scope and done-when criteria).

Target milestone: **$ARGUMENTS** — if blank, the first roadmap row whose status is
not `done`.

## 2. Detect the milestone's stage

Check `docs/plans/` for this milestone's documents and `git log`/branches for its
work, then enter the lifecycle at the right point:

| Found | Stage | Action |
|-------|-------|--------|
| No design doc | Design | Use superpowers:brainstorming to produce `docs/plans/YYYY-MM-DD-mN-<name>-design.md`; resolve open questions with Milan; get explicit approval; commit |
| Design doc not marked Approved | Design review | Resume the open questions; do not proceed without approval |
| Approved design, no implementation plan | Planning | Resolve every dependency version, tool version, and action SHA from live sources (crates.io, PyPI, GitHub API) — never from memory — then use superpowers:writing-plans |
| Plan exists, work not merged | Implementation | Branch `mN-<name>`, execute with superpowers:subagent-driven-development (implementer + spec review + quality review per batch) |
| Implementation merged | Verification | Check every done-when criterion with evidence; update README roadmap status; update `docs/architecture.md` if the milestone made it stale |

## 3. Hard rules (apply at every stage)

- Constitution in CLAUDE.md is non-negotiable: no live order execution code, AI
  advises / deterministic code decides, pessimistic simulation modeling, risk
  limits live in the RiskManager.
- Never push `main` — a hook blocks it; Milan pushes main himself. All changes go
  through a feature branch and PR with green CI.
- Zero warnings across all gates (`prek run --all-files`, clippy `-D warnings`,
  ruff, ty, pytest, cargo-deny, actionlint, zizmor).
- Design docs are immutable after approval; `docs/architecture.md` is the living
  model.
- Performance claims require a `run_id` or backtest artifact.

Report which milestone and stage you detected, then proceed.
