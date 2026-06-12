# System Architecture

The living model of the system. Update this document whenever an implemented change
makes it stale. Decision history lives in `docs/plans/`; this file describes the
current (intended) state.

## Components

```
rr/
├── crates/
│   ├── rr-cli         # the `rr` binary: CLI entry point for all engine commands
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
| rr-cli | The `rr` binary: command-line entry point (`stream`, `archive-status`, …) | clap |
| rr-engine | Market-data ingest (`market_data`, M1); later: event loop, strategies, risk, execution | barter-data |
| rr-kraken | Kraken WebSocket market data, normalized to barter-data model | barter-data |
| rr-storage | Persistence: record types, Parquet archiver, SQLite schema, coverage report | sqlx, parquet, arrow |
| rr-server | API for dashboard, fan-out of engine audit events | axum |
| rr-dashboard | Browser UI; candlestick chart via CDN lightweight-charts | leptos |
| analytics | Advisory analysis; reads SQLite/Parquet, writes reports to SQLite | uv, claude API |

`rr-engine` depends on `rr-storage` (which owns the persisted record types `TradeRecord`/`CandleRecord`); the dependency never runs the other way.

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

## Market data ingestion (implemented, M1)

The ingest pipeline that feeds the archive and (from M2) the engine. `rr stream`
runs it in the foreground until Ctrl-C (SIGINT); overnight runs use tmux/nohup.

```
barter-data Streams<PublicTrades>  (Binance spot BTC/ETH-USDT, Coinbase BTC/ETH-USD)
  │  reconnecting stream: Event::{Reconnecting, Item}
  ▼
rr-engine::market_data supervisor          (async select loop)
  ├─ convert: f64 → Decimal at the boundary; reject non-finite, non-positive,
  │           or >30 s-future timestamps (recorded as Error events, never archived)
  ├─ gap detector: per-(exchange,pair) trade-id sequence → gap_detected events
  ├─ 1-minute candle aggregator: tumbling UTC windows on exchange time, 5 s grace
  ├─ trades  ──mpsc(4096)──┐
  └─ candles ──mpsc(1024)──┤
                           ▼
        rr-storage::archive_task   (one blocking OS thread per dataset)
          PartitionedWriter<TradesDataset|CandlesDataset>
          ├─ Parquet:  data/parquet/<dataset>/exchange=…/pair=…/date=YYYY-MM-DD/part-NNNN.parquet
          └─ on each finalized file → SQLite archive_files row
                           │
   stream_sessions / stream_events / archive_files  ◄── supervisor records
   (SQLite, WAL)                                         session + connect/disconnect/
                           │                             gap/late/error events
                           ▼
        rr archive-status --date D  →  per (exchange,pair) candle-minute coverage
        (present / quiet / gap), reading candle Parquet + the SQLite event log
```

Key properties:

- **Candles are derived locally** from the trade stream (1-minute OHLCV, `Decimal`),
  one code path for every exchange — barter-data exposes no Coinbase kline stream.
- **Pessimistic, lossless.** Channel sends apply backpressure (a slow writer slows
  ingest; records are never dropped). A trade that fails conversion is recorded as an
  `Error` event and skipped, never archived. Any anomaly (disconnect, gap, late
  trade) is both a `tracing` line and a `stream_events` row — never silently skipped.
- **Crash-bounded durability.** Parquet files roll every 15 minutes and at UTC day
  rollover, are `fsync`-closed before their `archive_files` row is written, and a
  footer-less file left by a crash is quarantined to `*.corrupt` on restart. A crash
  loses at most one unclosed file per open partition.
- **Fatal failures stop the process.** A writer or database error signals shutdown
  so both archive threads drain and the CLI exits non-zero rather than running blind.

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

- **SQLite (sqlx, WAL):** one writer (the engine process), multiple readers (the
  `archive-status` command now; server and analytics later). Portable SQL; Postgres
  is the upgrade path if processes split across machines. Schema v1 (M1) holds the
  ingest's operational metadata only — no market data:
  - `stream_sessions` — one row per `rr stream` run (started/ended, config JSON).
  - `stream_events` — `connected` / `disconnected` / `gap_detected` / `late_trade` /
    `error`, timestamped, under a session; the provenance and coverage log.
  - `archive_files` — one row per finalized Parquet file (dataset, exchange, pair,
    date, path, rows, ts range); what `archive-status` reads.

  Later milestones add `runs`, orders, fills, positions, equity snapshots, and AI
  reports. The single deliberate SQLite-ism is `INTEGER PRIMARY KEY AUTOINCREMENT`
  (the Postgres path uses identity/serial columns; migrations are per-database).
- **Parquet:** candle/trade history partitioned `exchange=…/pair=…/date=YYYY-MM-DD`.
  Decimals stored as `Decimal128(38,18)` (encode errors rather than rounds beyond
  representable precision); timestamps as UTC millisecond `Timestamp`. Written by the
  archiver from day one (builds the backtest dataset); read by backtests (Rust) and
  ML (Python) natively.

### Known limitations / future work (M1)

- `rr stream` handles SIGINT only; under a service manager (SIGTERM) the final
  Parquet part may truncate. A SIGTERM arm is deferred until it runs supervised.
- An aborted run leaves `stream_sessions.ended_at` NULL with no clean-vs-crash
  marker; coverage treats post-last-event time as disconnected (pessimistic).
- A crash between `fsync`-closing a Parquet file and writing its `archive_files`
  row leaves a valid but unregistered file; a startup rescan to reconcile is future
  work. Parent-directory `fsync` of new part files is likewise deferred.

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
