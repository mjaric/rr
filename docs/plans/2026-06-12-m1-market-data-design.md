# M1 — Market Data Ingestion & Archiving: Design

Date: 2026-06-12
Status: Approved (2026-06-12)
Master plan: [2026-06-12-trading-intelligence-design.md](2026-06-12-trading-intelligence-design.md)

## Scope

Stream public trades from Binance and Coinbase via barter-data, derive 1-minute
candles locally, archive both to Parquet, record operational metadata in SQLite
schema v1, and expose it all through `rr stream` and `rr archive-status`.

**Done when:** `rr stream` runs overnight unattended; the morning Parquet has no
gaps (every missing candle minute is classified as quiet-market or a recorded gap).

Out of scope: order execution of any kind, Kraken (M5), order books, candle
intervals other than 1m, config files, service managers.

## Decisions

| Decision | Choice |
|----------|--------|
| Candle source | Derived locally from the trade stream for all exchanges — one code path, identical semantics, gap detection from our own stream continuity. barter-data offers no Coinbase candle subscription, so native klines would mean two code paths. |
| Pairs | Binance BTC/USDT + ETH/USDT, Coinbase BTC/USD + ETH/USD (spot) — most liquid market per exchange. Quote currency is part of pair identity; no normalization. Compile-time constants in M1. |
| Candle intervals | 1-minute only. 5m/1h/1d are deterministic roll-ups computed at read time when M2/M4 need them. |
| Code placement | Ingest (subscriptions, gap detection, candle aggregator) in `rr-engine::market_data`; Parquet writer, SQLite schema, status queries in `rr-storage`; `rr-cli` parses args and wires. Matches the architecture responsibility table; M2's engine reuses the same stream plumbing. |

Alternatives considered and rejected: native Binance klines + derived Coinbase
candles (two code paths, custom subscription work in M1); trades-only archiving
with read-time candles (pushes candle logic into M2, makes overnight verification
harder); a separate `rr-ingest` crate (premature); materialized roll-ups (redundant
data, more gap surface).

## Components and data flow

```
barter-data Streams<PublicTrades> (ReconnectingStream, Binance + Coinbase)
  → rr-engine::market_data
      ingest supervisor: tracing, f64→Decimal at the boundary, gap detection
        ├─ bounded mpsc → rr-storage Parquet trade writer
        ├─ 1m candle aggregator → bounded mpsc → rr-storage Parquet candle writer
        └─ stream events (connect/disconnect/gap/late) → SQLite
```

### Ingest supervisor (`rr-engine::market_data`)

Owns the barter-data subscriptions. Reconnection comes from barter's
`ReconnectingStream`; every disconnect/reconnect is logged with `tracing` and
recorded in SQLite. Trade prices and quantities convert from barter's `f64` to
`rust_decimal::Decimal` exactly once, at this boundary.

### Gap detection

Per (exchange, pair): trade-ID sequence checking where IDs are sequential (Binance
aggTrade IDs, Coinbase match `trade_id`), falling back to disconnect-event tracking
if an ID stream proves non-sequential after barter-data normalization (verified
during implementation). Gaps are recorded as events, never silently skipped.

### Candle aggregator (pure, deterministic)

Tumbling UTC minutes `[t, t+60s)` keyed on **exchange timestamps**. Produces OHLCV
(`Decimal`) plus trade count. A candle is emitted when the first trade of a later
window arrives or a 5-second grace timeout passes; trades arriving after emission
are logged and recorded as `late_trade` events. Trade-less minutes produce no row —
`archive-status` classifies missing minutes as quiet-market vs gap using the
stream-events log.

### Parquet archiver (`rr-storage`)

One open file per active partition. Files roll (footer closed, `part-NNNN`
incremented) every 15 minutes and at UTC day rollover, so a crash loses at most the
unclosed file. On startup the writer continues part numbering from the existing
directory and quarantines any footer-less file as `.corrupt` with a warning.

### CLI (`rr-cli`)

- `rr stream [--data-dir ./data]` — foreground until Ctrl-C; graceful shutdown
  drains channels, finalizes open files, closes the session row. Overnight runs use
  tmux/nohup; no service manager in M1.
- `rr archive-status [--date YYYY-MM-DD] [--data-dir ./data]` — per exchange/pair:
  file count, rows, candle coverage (minutes present / minutes expected), and
  missing-minute classification.

## Data schemas

### Parquet

Layout: `data/parquet/{trades|candles}/exchange=…/pair=…/date=YYYY-MM-DD/part-NNNN.parquet`

| Dataset | Columns |
|---------|---------|
| trades | `ts_exchange`, `ts_received` (UTC, ms), `price`, `amount` (decimal128), `side`, `trade_id` |
| candles | `ts_open` (window start, UTC), `open`, `high`, `low`, `close`, `volume` (decimal128), `trade_count` |

`ts_received` is kept alongside `ts_exchange` for latency observability.

### SQLite schema v1 (sqlx migrations)

Operational metadata only — no market data rows.

| Table | Columns (essentials) |
|-------|----------------------|
| `stream_sessions` | id, started_at, ended_at, config (exchanges/pairs JSON) |
| `stream_events` | session_id, ts, exchange, pair, kind (`connected` \| `disconnected` \| `gap_detected` \| `late_trade` \| `error`), details JSON |
| `archive_files` | session_id, dataset, exchange, pair, date, path, rows, ts_min, ts_max, closed_at |

`archive_files` is written on each file finalize and is what `archive-status`
reads. No `runs` table yet — `run_id` arrives in M2 when there are orders to
attribute; `stream_sessions` is M1's provenance unit.

## Error handling

- Bounded channels apply **backpressure** — ingest awaits and warns when storage is
  slow; data is never dropped to keep up.
- SQLite or Parquet write failure (e.g. disk full) is fatal: attempt a clean
  finalize, exit nonzero.
- Ctrl-C: drain channels, finalize all open files, close the session row.
- Every abnormal path goes through `tracing`; anything affecting data completeness
  is also a SQLite event.

## Test strategy

- **Candle aggregator:** unit + property tests (proptest) — OHLCV correctness,
  window boundaries, out-of-order within grace, late trades, empty minutes.
- **Gap detector:** synthetic ID sequences (contiguous, gapped, reset).
- **Parquet writer:** tempdir round-trips (write → read → equal), day rollover,
  part-number continuation on restart, corrupt-file quarantine.
- **SQLite / archive-status:** migration + query tests against a temp DB with
  seeded sessions and events.
- **CLI:** `assert_cmd` smoke tests; one `#[ignore]`d 30-second live-WebSocket
  smoke test, run manually.
- **Done-when evidence:** an overnight `rr stream` session, then `rr archive-status`
  output showing full candle coverage (or only classified quiet/gap minutes),
  attached to the PR.

## Open questions (non-blocking)

1. Whether barter-data preserves sequential trade IDs for both exchanges after
   normalization — verified during implementation; the gap detector degrades to
   disconnect-tracking only if not.

## Workflow

Branch `m1-market-data`, PR to `main`, merged on green CI after review.
Dependency versions (barter-data, tokio, sqlx, parquet/arrow, rust_decimal,
tracing, …) are resolved at implementation-plan time from live sources, never from
memory.
