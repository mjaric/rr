# CLAUDE.md — working rules for the rr repository

Trading intelligence app: simulation-first crypto trading agent. Read
`README.md` for the overview and `docs/architecture.md` for the system model before
making changes.

## Process: milestone lifecycle

Work proceeds milestone by milestone (M0–M7, see README roadmap). **No implementation
without an approved milestone design and an implementation plan.** For each milestone:

1. **Design** — `docs/plans/YYYY-MM-DD-mN-<name>-design.md`. Detailed design for this
   milestone only: components, interfaces, data schemas, error handling, test
   strategy, open questions. Approved by Milan before proceeding.
2. **Plan** — implementation plan (superpowers:writing-plans) broken into bite-sized
   TDD tasks, saved next to the design doc.
3. **Implement** — on a feature branch (`mN-<name>`), TDD, frequent commits, PR to
   `main`. Never push directly to main.
4. **Verify** — every "done when" criterion from the master plan checked with
   evidence before the milestone is called complete; update README roadmap status.

Design documents are point-in-time decision records and are never edited after
approval; `docs/architecture.md` is the living model and MUST be updated whenever an
implemented change makes it stale.

## The constitution (non-negotiable engine rules)

- **No live order execution code** exists in this repo until the dedicated live-trading
  milestone is designed and approved. Do not add exchange API keys, signing code for
  private endpoints, or order-placement paths "to prepare".
- **Deterministic code decides, AI advises.** Analytics/LLM output reaches the engine
  only as data through the database, never as code paths that place orders.
- **Pessimistic modeling.** Simulation must charge fees, slippage, and latency; fill
  logic errs against us (limit orders fill only on trade-through).
- **Risk limits are enforced in the RiskManager, not in strategies:** ≤1% capital
  risk per trade, ≤6% total open risk, −2% daily stop, half size at −10% drawdown,
  full stop at −20%. Strategies request; the RiskManager disposes.

## Repository conventions

- Cargo workspace under `crates/`: `rr-engine`, `rr-kraken`, `rr-storage`,
  `rr-server`, `rr-dashboard`. Python analytics under `analytics/` (uv/ruff/ty).
- Storage: SQLite via sqlx (WAL) for operational state; Parquet for market-data
  history (partitioned by exchange/pair/day). Keep SQL portable — Postgres is the
  upgrade path.
- Every simulation/backtest run carries a `run_id`; all orders, fills, positions,
  and equity snapshots are persisted under it.
- Web stack is Rust-only (Axum + Leptos/WASM). No Node toolchain. The single JS
  exception is the CDN-loaded `lightweight-charts` library.
- Rust style, lints, and quality gates follow the global standards (clippy pedantic
  + deny unwrap/panic/todo, zero warnings, `prek run` before commit).
- Lints stay strict everywhere, tests included. A test that must panic/unwrap/expect
  carries a narrow `#[expect(lint, reason = "...")]` on the test fn — never `#[allow]`.
- `rr-kraken` depends only on barter-data, never on other `rr-*` crates — it is
  planned for separate open-sourcing.

## Domain guardrails for agents

- Market-data code must assume disconnects, gaps, and out-of-order events; surface
  them with `tracing` — never silently skip.
- Money and quantities: use `rust_decimal::Decimal` (or barter's types), never `f64`,
  for prices, quantities, and PnL arithmetic that affects records.
- Timestamps are UTC everywhere (`chrono::DateTime<Utc>`), milliseconds precision min.
- When a claim about strategy performance is made (docs, reports, commit messages),
  it must cite a `run_id` or backtest artifact — no unverifiable performance claims.
