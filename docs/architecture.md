# System Architecture

The living model of the system. Update this document whenever an implemented change
makes it stale. Decision history lives in `docs/plans/`; this file describes the
current (intended) state.

## Components

```
rr/
├── crates/
│   ├── rr-engine      # barter-rs integration: strategies, risk manager, mode runner
│   ├── rr-kraken      # Kraken connector implementing barter-data's Connector trait
│   ├── rr-storage     # SQLite (operational state) + Parquet (market-data history)
│   ├── rr-server      # Axum: REST API + WebSocket push + serves the dashboard
│   └── rr-dashboard   # Leptos (WASM): charts, positions, P&L, risk panel, reports
├── analytics/         # Python (uv): metrics, ML models, Claude critique reports
└── docs/
```

| Component | Responsibility | Key dependency |
|-----------|----------------|----------------|
| rr-engine | Event loop, strategies, risk enforcement, execution routing | barter-rs |
| rr-kraken | Kraken WebSocket market data, normalized to barter-data model | barter-data |
| rr-storage | Persistence: SQLite schema, Parquet archiver, run tracking | sqlx, parquet |
| rr-server | API for dashboard, fan-out of engine audit events | axum |
| rr-dashboard | Browser UI; candlestick chart via CDN lightweight-charts | leptos |
| analytics | Advisory analysis; reads SQLite/Parquet, writes reports to SQLite | uv, claude API |

## Data flow

```
Binance ─┐
Coinbase ─┼─ barter-data WebSocket streams ──> Engine (event loop)
Kraken  ─┘   (normalized trades/candles)          │
                    │                             ├─> Strategies ──> order requests
                    └──> Parquet archive          ├─> RiskManager (approve/veto/resize)
                                                  └─> ExecutionClient (per mode)
                                                          │
            SQLite <── orders, fills, positions, equity ──┘
               │
               ├──> rr-server ──WebSocket──> rr-dashboard (live view)
               └──> analytics (read) ──reports──> SQLite ──> dashboard
```

## Modes

One engine, one strategy/risk codebase, three modes; only the `ExecutionClient`
differs. This guarantees backtest, simulation, and (eventual) live behavior match.

| Mode | Market data | Execution | Status |
|------|-------------|-----------|--------|
| Simulation (default) | live, real-time | simulated fills (model below) | planned M2 |
| Backtest | recorded Parquet, accelerated | simulated fills | planned M4 |
| Live | live, real-time | real exchange — **does not exist yet** | deferred, own design |

## Fill and slippage model (simulated execution)

Principle: model pessimistically; the simulation underestimates performance.

- Market orders: fill from current best bid/ask + slippage penalty (scaled by
  volatility and order size) + simulated latency (~100–300 ms, configurable).
  Fill size capped to a fraction of observed market volume; excess → partial fill.
- Limit orders: fill only on trade-*through* (touching the price doesn't count).
- Fees: real per-exchange maker/taker rates, in config.
- Planned upgrade: L2 order-book streaming and book-walking fills.

## Risk constitution (enforced in RiskManager)

≤1% capital risk per trade · ≤6% total open risk · −2% daily loss stop ·
half position size at −10% drawdown · full stop at −20%. Strategies request;
the RiskManager disposes.

## Storage

- **SQLite (sqlx, WAL):** runs, orders, fills, positions, equity snapshots, AI
  reports. One writer (engine), multiple readers (server, analytics). Portable SQL;
  Postgres is the upgrade path if processes split across machines.
- **Parquet:** candle/trade history partitioned by `exchange/pair/day`. Written by
  the archiver from day one (builds the backtest dataset); read by backtests (Rust)
  and ML (Python) natively.

## AI analysis stack (advisory only)

1. Quant metrics (deterministic): Sharpe, Sortino, drawdown, win rate, profit
   factor, fee drag, per-strategy attribution.
2. Classical ML (Python): market regime detection, trader/wallet scoring (M7),
   anomaly detection on the agent's own trades.
3. LLM (Claude): consumes layers 1–2 + trade log; writes plain-language critique
   reports. Humans approve any strategy change.

The boundary is the database: analytics reads SQLite/Parquet, writes reports back to
SQLite. No code path from analytics to order placement.

## Technology choices

| Concern | Choice | Why |
|---------|--------|-----|
| Engine framework | barter-rs (MIT) | event-driven engine, paper/live parity, normalized exchange streams |
| Web server | axum | de-facto standard, tokio-native |
| Frontend | Leptos (WASM) | Rust-only toolchain, no Node |
| Charts | lightweight-charts (CDN) | best-in-class financial charts; the one JS exception |
| DB | SQLite via sqlx | zero-ops, fits one-writer workload, Postgres path open |
| Bulk data | Parquet | fast columnar reads from both Rust and Python |
| Python tooling | uv, ruff, ty | per global standards |
| Decimals | rust_decimal / barter types | never f64 for money |
