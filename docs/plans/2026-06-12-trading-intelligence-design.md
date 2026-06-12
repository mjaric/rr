# Trading Intelligence App — Design

Date: 2026-06-12
Status: Approved

## Goal

A trading intelligence app that trades crypto conservatively and by plan — proven in
simulation on real market data for months before any real money is involved.

## Decisions made

| Decision | Choice |
|----------|--------|
| End goal | Simulation-first; live trading much later, with small capital, behind explicit safeguards |
| Copy-trade sources | On-chain wallets first; exchange leaderboards and social posts deferred |
| AI role | Full advisory stack: quant metrics + classical ML + LLM critique. AI advises, deterministic code decides |
| Language split | Rust core (engine, connectors, server, dashboard) + Python analytics (uv/ruff/ty) |
| Dashboard | Rust-native web stack (Axum + Leptos/WASM), no Node toolchain; charts via CDN-loaded lightweight-charts |
| Engine foundation | Build on barter-rs (approach A) rather than from scratch |
| Database | SQLite (WAL) now via sqlx; Postgres is a known upgrade path if services split |
| Bulk time-series | Parquet files (read natively by both Rust and Python) |

## Principles (the constitution)

- **Simulation-first.** No live order execution exists until a dedicated, separately
  designed milestone, after months of good simulated performance.
- **Deterministic code decides, AI advises.** Analytics and LLM output never place
  orders; they communicate only through the database.
- **Pessimistic assumptions everywhere.** Fees, slippage, latency, and fills are
  modeled to underestimate performance, never flatter it.
- **Hard risk limits the engine enforces regardless of strategy:**
  - ≤1% of capital at risk per trade (fixed-fractional; quarter-Kelly arrives at ~the same number)
  - ≤6% total open risk (portfolio heat)
  - −2% daily loss → stop trading for the day
  - Drawdown brake: half position size at −10% drawdown, full stop at −20%

## Research findings

- **Strategies are regime-dependent.** Momentum wins in trends, mean reversion in
  ranges; each bleeds in the other regime. Best documented results come from blending
  both behind a regime filter (blended Sharpe ~1.7 vs. each alone).
  ([source](https://medium.com/@briplotnik/systematic-crypto-trading-strategies-momentum-mean-reversion-volatility-filtering-8d7da06d60ed))
- **Vanilla cross-exchange arbitrage is effectively dead for retail.** Gaps above the
  ~0.2% fee floor are taken in milliseconds by colocated HFT; Bitsgap discontinued its
  arbitrage product. High-return claims come from vendors selling arbitrage software.
  We build a spread *monitor* to measure reality, not a profit pillar.
  ([source](https://bitsgap.com/blog/crypto-arbitrage-explained-tutorial))
- **On-chain trader scoring is real.** Every DEX trade is public; wallets can be
  scored objectively on realized PnL, win rate, and consistency (Nansen's business
  model; their API exposes this). Blind copying fails (you fill later and worse) —
  the sound use is signal generation feeding our own risk-managed decisions.
  ([source](https://docs.nansen.ai/guides/templates/complex-use-cases/use-case-4-copytrading-top-performing-wallets))
- **barter-rs** (MIT) provides the event-driven engine, pluggable `Strategy` /
  `RiskManager` / `ExecutionClient` traits, normalized multi-exchange WebSocket data
  (Binance, Coinbase, Bybit, OKX — Kraken missing, we write it), and paper/backtest/live
  parity on one engine. ([source](https://github.com/barter-rs/barter-rs))

## Architecture

```
rr/
├── crates/
│   ├── rr-engine      # barter-rs integration: strategies, risk manager, sim/live runner
│   ├── rr-kraken      # Kraken connector implementing barter-data's Connector trait
│   ├── rr-storage     # SQLite (operational state) + Parquet (market-data history)
│   ├── rr-server      # Axum: REST API + WebSocket push + serves the dashboard
│   └── rr-dashboard   # Leptos (WASM): charts, positions, P&L, risk panel, reports
├── analytics/         # Python (uv): metrics, ML models, Claude critique reports
└── docs/plans/
```

The Python side reads the same SQLite/Parquet data, writes analysis results and
reports back to SQLite, and is rendered by the dashboard. It never places orders.

## Data flow and modes

```
Binance ─┐
Coinbase ─┼─ barter-data WebSocket streams ──> Engine (event loop)
Kraken  ─┘   (normalized trades/candles)          │
                    │                             ├─> Strategies ──> Orders
                    └──> Parquet archive          ├─> RiskManager (approve/veto/resize)
                                                  └─> Execution client (per mode)
```

One engine, three modes; only the execution client differs:

1. **Simulation (default):** live real-time market data, simulated fills against real
   prices with the fill model below. Virtual portfolio, real market.
2. **Backtest:** the same engine fed recorded Parquet data at accelerated speed.
3. **Live (far future):** the same engine, real execution, small capital, explicit
   flag + confirmation. Designed in its own milestone, not now.

Every candle is archived to Parquet from day one (builds the backtest dataset for
free). Every order, fill, position change, and equity snapshot is persisted to SQLite
with a `run_id`, making each simulation run a comparable experiment. The engine's
audit stream fans out over WebSocket to the dashboard.

## Fill and slippage model

Principle: when in doubt, model pessimistically — a strategy profitable in our
simulation has survived costs worse than reality.

- **Market orders:** fill from current best bid/ask (not last trade) + slippage
  penalty scaled by volatility and order size + simulated decision-to-fill latency
  (~100–300 ms, configurable). Fill size capped to a fraction of observed market
  volume in the window; excess becomes a partial fill.
- **Limit orders:** fill only when the market trades *through* the price (touching
  doesn't count — queue position is unknowable). Partial fills tracked.
- **Fees:** real per-exchange maker/taker rates in config (e.g. Kraken 0.25/0.40%,
  Binance 0.10/0.10%).
- **Later upgrade:** stream L2 order-book depth and walk the book for exact fill
  prices; archive book snapshots so backtests gain the same fidelity.
- barter's mock execution client must be evaluated against this spec during M2; if
  too simplistic, we implement our own `ExecutionClient`. The spec is the contract
  and gets its own test suite (e.g. a limit at 100 must NOT fill when the low touches
  exactly 100).

## AI analysis stack (advisory only)

1. **Quant metrics (deterministic):** Sharpe, Sortino, max drawdown, win rate, profit
   factor, fee drag, per-strategy attribution.
2. **Classical ML (Python):** market regime detection (trending/ranging/volatile,
   correlated with strategy performance); trader/wallet scoring for the on-chain
   feature; anomaly detection on the agent's own trades.
3. **LLM (Claude):** consumes layers 1–2 plus the trade log and writes the
   plain-language critique: what worked, what lost money, rule violations, suggested
   experiments. Humans approve any strategy change.

## Master plan — milestones

Each milestone is independently useful and gets its own detailed implementation plan
only when work on it begins.

### M0 — Skeleton & guardrails
Workspace with empty crate shells; clippy lint block (deny unwrap/panic/todo…),
rustfmt, cargo-deny; prek hooks; GitHub Actions CI; analytics/ scaffold (uv/ruff/ty).
**Done when:** CI green on a hello-world binary; `prek run` passes.

### M1 — Market data in & archived
barter-data streams from Binance + Coinbase (BTC, ETH pairs; trades + candles);
reconnection handling, gap detection, tracing logs; archiver to Parquet (per
exchange/pair/day); SQLite schema v1; CLI (`rr stream`, `rr archive-status`).
**Done when:** runs overnight unattended; morning Parquet has no gaps.

### M2 — Simulation engine + first strategy
Engine wired end to end; fill/slippage model per spec (verify barter's mock, replace
if needed); risk constitution v1 as a RiskManager; one simple momentum strategy
(MA crossover + volatility filter) as testbed; everything persisted with run_id.
**Done when:** a sim run yields a queryable trade log and equity curve; the risk
manager provably vetoes oversized trades (tested).

### M3 — Dashboard v1
Axum API + WebSocket push; Leptos WASM frontend; candlestick chart
(lightweight-charts via CDN) with buy/sell markers; positions, equity curve, risk
panel; run picker for past runs.
**Done when:** you watch the simulation trade live in the browser.

### M4 — Backtesting + second strategy
Replay archived Parquet through the same engine at speed; mean-reversion strategy +
regime filter selecting the active strategy; side-by-side run comparison (return,
Sharpe, drawdown, fees paid).
**Done when:** "which strategy was better over the last 3 months and why" is
answerable from the dashboard.

### M5 — Kraken connector
Implement barter-data's Connector trait for Kraken's WebSocket API; candidate for
upstreaming.
**Done when:** Kraken streams, archives, and trades in simulation identically to the
other exchanges.

### M6 — Analytics & AI reports
Python metrics job; regime-detection model; anomaly flags; Claude-written critique
report per run/week stored in SQLite and rendered in the dashboard.
**Done when:** each run produces a readable report with correct attribution and a
proposed next experiment.

### M7 — On-chain signals + arbitrage monitor
Starts with a scope checkpoint, not code. Evaluate Nansen API vs. raw chain data;
pick chains. Wallet scoring (win rate, realized PnL, consistency) → classify traders;
signals are engine *inputs*, never auto-trades. Arbitrage monitor logs cross-exchange
spreads net of fees to prove or disprove the edge with data.
**Done when:** checkpoint review decides what graduates into the engine.

### Explicitly deferred
Exchange leaderboards and social-post signals · live trading (own milestone with its
own safety design) · L2 order-book fill simulation · AI auto-tuning of parameters.
