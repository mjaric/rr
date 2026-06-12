# rr — Trading Intelligence App

A crypto trading agent that trades conservatively and by plan — proven in simulation
on real market data before any real money is involved.

**Status: M0 (skeleton & guardrails) in review.** Milestones are designed, planned,
and implemented one at a time — see the roadmap below.

## What it does (when built)

- Streams live market data from Kraken, Binance, and Coinbase
- Runs simple, well-understood strategies (momentum, mean reversion) behind a market
  regime filter, under hard risk limits the engine enforces
- **Simulation mode (default):** trades a virtual portfolio against live real prices,
  with realistic fees, slippage, and fill rules — watchable in a local web dashboard
- **Backtest mode:** replays archived data through the same engine
- Analyzes its own performance with quant metrics, classical ML (regime detection,
  anomaly flags), and Claude-written critique reports — all advisory; deterministic
  code makes every trade decision
- Later: on-chain "smart money" wallet scoring as a signal source, and an arbitrage
  spread monitor to measure (not assume) whether any edge exists

## Principles

1. **Simulation-first.** Live trading is a distant, separately designed milestone.
2. **Deterministic code decides; AI advises.**
3. **Pessimistic assumptions.** The simulator charges costs worse than reality.
4. **Hard risk limits:** ≤1% risk per trade, ≤6% open risk, −2% daily stop,
   drawdown brake (half size at −10%, full stop at −20%).

## Roadmap

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 | Skeleton & guardrails (workspace, lints, CI, hooks) | in review |
| M1 | Market data ingestion & archiving (Binance, Coinbase → Parquet/SQLite) | pending |
| M2 | Simulation engine + first strategy + risk limits | pending |
| M3 | Web dashboard v1 | pending |
| M4 | Backtesting + second strategy + regime filter | pending |
| M5 | Kraken connector | pending |
| M6 | Analytics & AI reports (Python + Claude) | pending |
| M7 | On-chain signals + arbitrage monitor | pending |
| — | Live trading, leaderboards, social signals, L2 fills, auto-tuning | deferred |

## Documentation

- [System architecture](docs/architecture.md) — the current system model (kept up to date)
- [Design & master plan](docs/plans/2026-06-12-trading-intelligence-design.md) — approved foundation design
- `docs/plans/` — per-milestone design and implementation plan documents
- [CLAUDE.md](CLAUDE.md) — working rules and process for this repository

## Tech stack

Rust (Cargo workspace: engine on [barter-rs](https://github.com/barter-rs/barter-rs),
Axum server, Leptos/WASM dashboard) · Python via `uv` for analytics/ML ·
SQLite (sqlx) + Parquet for storage · Claude API for analysis reports.

## Development

```sh
git clone https://github.com/mjaric/rr.git && cd rr
prek install                  # git hooks (fmt, clippy, ruff, actionlint)
prek auto-update --cooldown-days 7   # keep hook versions fresh, 7-day cooldown
cargo build --workspace       # Rust workspace
cd analytics && uv sync       # Python analytics environment
```

## Disclaimer

This is a personal research and learning project. Nothing here is financial advice.
Crypto trading carries a real risk of loss; that is exactly why this project trades
simulated money until proven otherwise.

Copyright © 2026 Milan Jaric. All rights reserved. Proprietary — no license granted.
