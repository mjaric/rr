# M1 — Market Data Ingestion & Archiving: Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement
> this plan task-by-task.

**Goal:** Stream Binance + Coinbase public trades via barter-data, derive 1-minute
candles, archive both to partitioned Parquet, track sessions/events/files in SQLite,
expose `rr stream` and `rr archive-status`.

**Architecture:** Ingest (barter-data subscriptions, gap detection, candle
aggregation) lives in `rr-engine::market_data`; persistence (Parquet writer thread,
SQLite via sqlx) lives in `rr-storage`; `rr-cli` wires them. Bounded tokio mpsc
channels carry records from async ingest to a blocking writer thread
(`Receiver::blocking_recv`), giving natural backpressure. Records (`TradeRecord`,
`CandleRecord`) are defined in `rr-storage` (it owns the persisted schemas);
`rr-engine` depends on `rr-storage`, never the reverse.

**Tech Stack (versions resolved 2026-06-12 from crates.io):**
barter-data 0.11.0 · tokio 1.52.3 · futures 0.3.32 · sqlx 0.9.0 (sqlite,
runtime-tokio) · parquet 59.0.0 · arrow 59.0.0 · rust_decimal 1.42.1 ·
chrono 0.4.45 · tracing 0.1.44 · tracing-subscriber 0.3.23 · serde 1.0.228 ·
serde_json 1.0.150 · anyhow 1.0.102 · thiserror 2.0.18 · proptest 1.11.0 (dev) ·
tempfile 3.27.0 (dev). barter-instrument/barter-integration resolve transitively —
do not pin them.

**Design doc:** [2026-06-12-m1-market-data-design.md](2026-06-12-m1-market-data-design.md)
— the contract. Constitution applies: no execution code, Decimal for money, UTC
timestamps, never silently skip data problems.

**Branch:** `m1-market-data` (already exists, design doc committed).

---

## Task 1: Workspace dependencies and crate wiring

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `crates/rr-storage/Cargo.toml`
- Modify: `crates/rr-engine/Cargo.toml`
- Modify: `crates/rr-cli/Cargo.toml`

**Step 1: Add to `[workspace.dependencies]` in root `Cargo.toml`** (keep `clap`,
`assert_cmd` lines as-is):

```toml
anyhow = "1.0.102"
arrow = "59.0.0"
barter-data = "0.11.0"
chrono = { version = "0.4.45", features = ["serde"] }
futures = "0.3.32"
parquet = "59.0.0"
proptest = "1.11.0"
rust_decimal = "1.42.1"
serde = { version = "1.0.228", features = ["derive"] }
serde_json = "1.0.150"
sqlx = { version = "0.9.0", default-features = false, features = ["sqlite", "runtime-tokio", "chrono", "migrate", "macros"] }
tempfile = "3.27.0"
thiserror = "2.0.18"
tokio = { version = "1.52.3", features = ["rt-multi-thread", "macros", "signal", "sync", "time"] }
tracing = "0.1.44"
tracing-subscriber = { version = "0.3.23", features = ["env-filter"] }
```

**Step 2: Crate manifests.** `rr-storage` gets arrow, parquet, sqlx, chrono,
rust_decimal, serde, serde_json, thiserror, tracing, tokio (sync only is not a
feature split worth making — use workspace tokio); dev: tempfile, proptest.
`rr-engine` gets barter-data, futures, tokio, chrono, rust_decimal, thiserror,
tracing, rr-storage (path dep); dev: proptest. `rr-cli` gets clap, anyhow, tokio,
tracing, tracing-subscriber, rr-engine + rr-storage (path deps); dev: assert_cmd,
tempfile. All keep `[lints] workspace = true`.

**Step 3: Verify** — `cargo build --workspace` and `cargo deny check` both green.
cargo-deny failures here mean a transitive license needs review — stop and surface
it, do not silently allowlist.

**Step 4: Commit** — `git commit -m "Add M1 dependencies to workspace"`

## Task 2: barter-data API spike (throwaway, verifies assumptions)

The design's open question: verify `MarketEvent`/`PublicTrade` shape and trade-ID
sequentiality. Check docs.rs/barter-data/0.11.0 for: `MarketEvent { time_exchange,
time_received, exchange, instrument, kind }`, `PublicTrade { id, price: f64,
amount: f64, side }`, `MarketStreamResult`, reconnect `Event::{Item, Reconnecting}`.

**Step 1:** Write `crates/rr-engine/examples/spike_stream.rs` subscribing to the
four design pairs with `Streams::<PublicTrades>::builder()` (see design doc data
flow), printing events via `tracing` for 30 seconds.

**Step 2:** `cargo run -p rr-engine --example spike_stream` (needs network). Record
in the task notes: exact field names/types, whether Binance + Coinbase trade IDs
parse as sequential u64s.

**Step 3:** Delete the example (`git clean` it) — knowledge goes into the next
tasks, not the repo. If field shapes differ from the design's assumptions, update
the affected code in Tasks 5–7 accordingly and note the deviation in the PR.

## Task 3: `rr-storage` records module

**Files:**
- Create: `crates/rr-storage/src/records.rs`
- Modify: `crates/rr-storage/src/lib.rs`

**Step 1: Write the failing test** (bottom of `records.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;

    #[test]
    fn trade_record_partition_date_is_utc_exchange_time() -> Result<(), chrono::ParseError> {
        let trade = TradeRecord {
            exchange: "binance_spot".into(),
            pair: "BTC-USDT".into(),
            ts_exchange: Utc.with_ymd_and_hms(2026, 6, 12, 23, 59, 59).single().ok_or_else(|| unreachable!())?,
            ts_received: Utc.with_ymd_and_hms(2026, 6, 13, 0, 0, 1).single().ok_or_else(|| unreachable!())?,
            price: Decimal::new(67_000_12, 2),
            amount: Decimal::new(5, 3),
            side: Side::Buy,
            trade_id: "12345".into(),
        };
        assert_eq!(trade.partition_date().to_string(), "2026-06-12");
        Ok(())
    }
}
```

(Use a plain `#[expect(clippy::unwrap_used, reason = "test fixture")]` +
`.unwrap()` if the Result-returning fixture above fights the lints — pick whichever
compiles warning-free; the repo convention is narrow `#[expect]`, never `#[allow]`.)

**Step 2:** `cargo test -p rr-storage` — FAIL (types don't exist).

**Step 3: Implement:**

```rust
//! Canonical persisted record types. `rr-storage` owns what gets written;
//! `rr-engine` produces these at its ingest boundary.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRecord {
    pub exchange: String,
    pub pair: String,
    pub ts_exchange: DateTime<Utc>,
    pub ts_received: DateTime<Utc>,
    pub price: Decimal,
    pub amount: Decimal,
    pub side: Side,
    pub trade_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandleRecord {
    pub exchange: String,
    pub pair: String,
    /// Window start, UTC, aligned to the minute.
    pub ts_open: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trade_count: u64,
}

impl TradeRecord {
    pub fn partition_date(&self) -> NaiveDate {
        self.ts_exchange.date_naive()
    }
}

impl CandleRecord {
    pub fn partition_date(&self) -> NaiveDate {
        self.ts_open.date_naive()
    }
}
```

`lib.rs`: replace doc-comment-only body with `pub mod records;` (keep crate doc).

**Step 4:** `cargo test -p rr-storage` — PASS. `cargo clippy --all-targets -- -D warnings` clean.

**Step 5:** `git commit -m "Add trade and candle record types to rr-storage"`

## Task 4: Candle aggregator (pure, in `rr-engine`)

**Files:**
- Create: `crates/rr-engine/src/market_data/mod.rs`, `crates/rr-engine/src/market_data/candle.rs`
- Modify: `crates/rr-engine/src/lib.rs` (`pub mod market_data;`)

Semantics from the design: tumbling UTC minutes `[t, t+60s)` on `ts_exchange`;
emit a window when a trade for a **later** window arrives on the same
(exchange, pair) key, or when `flush_older_than(watermark)` is called (the
supervisor calls it on a 1s tick with `now − 5s` grace); a trade older than an
already-emitted window is **late**: it is NOT folded into any candle (the candle
already shipped) but the caller still archives the raw trade and records a
`late_trade` event. `flush_all()` drains everything at shutdown.

**Step 1: Write failing tests** covering, at minimum:
- one trade → `flush_all` yields one candle with o=h=l=c=price, volume=amount, count=1
- trades across two minutes → first candle emitted by `ingest` of the second-minute trade
- OHLCV correctness with several trades in one minute (high/low/close ordering)
- out-of-order trades **within** the open window are folded in correctly
- trade for an already-emitted window → `IngestOutcome::Late`, no candle mutation
- `flush_older_than` emits only windows strictly older than the watermark
- empty minutes yield nothing
- independent (exchange, pair) keys don't interfere

Test shape (one example; write the rest in the same style):

```rust
#[test]
fn emits_completed_window_when_next_window_trade_arrives() {
    let mut agg = CandleAggregator::default();
    let (emitted, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:30Z", "100", "1"));
    assert!(emitted.is_empty());
    assert_eq!(outcome, IngestOutcome::Ok);
    let (emitted, _) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:01:02Z", "101", "2"));
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");
    assert_eq!(emitted[0].close, dec("100"));
}
```

with `fn trade(pair, rfc3339, price, amount) -> TradeRecord` and
`fn dec(s) -> Decimal` test helpers (parse, `#[expect]` narrow on the helper if
needed).

**Step 2:** `cargo test -p rr-engine` — FAIL.

**Step 3: Implement** `candle.rs`:

```rust
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use rust_decimal::Decimal;
use rr_storage::records::{CandleRecord, TradeRecord};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Ok,
    /// Trade belongs to a window that was already emitted; archived raw, not candled.
    Late,
}

#[derive(Debug, Default)]
pub struct CandleAggregator {
    /// Open windows keyed by (exchange, pair) → (window_start → building candle).
    open: BTreeMap<(String, String), BTreeMap<DateTime<Utc>, CandleRecord>>,
    /// Highest emitted window start per key; anything ≤ this is late.
    emitted_through: BTreeMap<(String, String), DateTime<Utc>>,
}
```

Window start: `trade.ts_exchange.duration_trunc(TimeDelta::minutes(1))` —
`duration_trunc` returns `Result`; a failure is unreachable for minute truncation
but must be handled (map to `Late`? No — return a dedicated error). Simplest
lint-clean approach: compute via timestamp math, which is infallible:

```rust
fn window_start(ts: DateTime<Utc>) -> DateTime<Utc> {
    let secs = ts.timestamp() - ts.timestamp().rem_euclid(60);
    DateTime::from_timestamp(secs, 0).unwrap_or(ts) // unreachable; ts came from a valid DateTime
}
```

(`unwrap_or(ts)` keeps it panic-free without `#[expect]`.)

`ingest`: compute window; if `window <= emitted_through[key]` → `Late`. Otherwise
fold into `open[key][window]` (create with o=h=l=c=price on first trade; else
`high = max`, `low = min`, `close = price`, `volume += amount`, `count += 1`).
Then emit: every open window for this key strictly older than the new trade's
window is finalized (removed, pushed to the return Vec, `emitted_through`
advanced). `flush_older_than(watermark)`: across all keys, emit windows whose
**end** (`start + 60s`) ≤ watermark. `flush_all`: emit everything. Always return
candles sorted by `ts_open`.

**Step 4:** `cargo test -p rr-engine` — PASS; clippy clean.

**Step 5:** `git commit -m "Add 1m candle aggregator"`

**Step 6 (property test):** Add a proptest: for any vec of trades within one
minute on one key, `flush_all` returns one candle where `high == max(prices)`,
`low == min(prices)`, `volume == sum(amounts)`, `trade_count == len`, `open` is
the first ingested trade's price (document: open/close follow **ingest order**
within the window, which is stream order — exchange order is the stream's
promise). Run, PASS, `git commit -m "Add candle aggregator property test"`.

## Task 5: Gap detector (pure, in `rr-engine`)

**Files:**
- Create: `crates/rr-engine/src/market_data/gap.rs`

**Step 1: Failing tests:** first observation → `Ok`; consecutive IDs → `Ok`;
jump (5 then 8) → `Gap { expected: 6, got: 8 }`; non-numeric ID → `NonSequential`
once, then all further observations on that key → `Untracked` (detector stops
checking that key, never errors again); independent keys independent; ID lower
than last (exchange reset/snapshot) → `Gap` is wrong — emit `Regression { last,
got }` so it's distinguishable in events.

**Step 2:** Run — FAIL.

**Step 3: Implement:**

```rust
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapCheck {
    Ok,
    Gap { expected: u64, got: u64 },
    Regression { last: u64, got: u64 },
    /// ID didn't parse as u64; this key degrades to disconnect-tracking only.
    NonSequential,
    /// Key previously degraded; no checking performed.
    Untracked,
}

#[derive(Debug, Default)]
pub struct GapDetector {
    last: BTreeMap<(String, String), u64>,
    degraded: BTreeMap<(String, String), ()>,
}

impl GapDetector {
    pub fn observe(&mut self, exchange: &str, pair: &str, trade_id: &str) -> GapCheck { /* per spec above */ }
}
```

**Step 4:** PASS; clippy clean. **Step 5:** `git commit -m "Add trade-id gap detector"`

## Task 6: Partitioned Parquet writer (generic, in `rr-storage`)

**Files:**
- Create: `crates/rr-storage/src/parquet/mod.rs`, `partition.rs`, `encode.rs`
- Modify: `crates/rr-storage/src/lib.rs`

One generic writer serves both datasets (DRY). A `Dataset` trait provides the
arrow schema and row encoding; `PartitionedWriter<D: Dataset>` owns open files
keyed by (exchange, pair, date), rolls them, and reports finalized files.

Key decisions (from design): decimals as arrow `Decimal128(38, 18)` (helper
`decimal_to_i128_scale18(d: Decimal) -> Result<i128, StorageError>`, erroring —
never rounding — if `d` has >18 decimal places); timestamps as
`Timestamp(Millisecond, Some("UTC".into()))`; layout
`<data_dir>/parquet/<dataset>/exchange=<e>/pair=<p>/date=<YYYY-MM-DD>/part-NNNN.parquet`;
roll when open file age > `roll_interval` (default 15 min, constructor param) or
record's date ≠ file's date; on open, scan dir for max `part-NNNN` and continue;
a file present but lacking a Parquet footer is renamed `*.corrupt` with
`tracing::warn!`. Finalize returns:

```rust
pub struct FinalizedFile {
    pub dataset: &'static str,
    pub exchange: String,
    pub pair: String,
    pub date: NaiveDate,
    pub path: PathBuf,
    pub rows: u64,
    pub ts_min: DateTime<Utc>,
    pub ts_max: DateTime<Utc>,
}
```

**TDD steps (each: test → fail → implement → pass → commit):**

1. `partition.rs`: pure path construction + `next_part_number(dir)` scanning —
   tests with tempfile dirs (empty dir → 0; existing part-0003 → 4; non-matching
   files ignored). Commit: `"Add parquet partition path logic"`.
2. `encode.rs`: `decimal_to_i128_scale18` (exact values round-trip; >18 dp →
   error; negative OK) + `TradesDataset`/`CandlesDataset` schema + RecordBatch
   encoding — test: encode a Vec<TradeRecord>, assert batch columns/values via
   arrow readers. Commit: `"Add arrow encoding for trades and candles"`.
3. `mod.rs`: `PartitionedWriter::append(&mut self, record)` buffering +
   `roll_due(now)` + `finalize_all()` — round-trip test: append trades across two
   pairs and a date boundary, finalize, read every file back with
   `parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder`, assert
   record-for-record equality and correct `FinalizedFile` metadata. Separate
   tests: part numbering continues across writer restart; pre-existing
   footer-less file is quarantined to `.corrupt` on startup. Buffered rows flush
   to the open file as a row group every `append` batch of 1000 or on roll —
   simplest correct policy; tune later only if measured slow. Commit:
   `"Add partitioned parquet writer with roll and quarantine"`.

## Task 7: SQLite schema v1 and repositories (`rr-storage`)

**Files:**
- Create: `crates/rr-storage/migrations/0001_schema_v1.sql`, `crates/rr-storage/src/db.rs`

**Migration** (portable SQL — no SQLite-isms beyond what sqlx maps):

```sql
CREATE TABLE stream_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    config TEXT NOT NULL
);
CREATE TABLE stream_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES stream_sessions(id),
    ts TEXT NOT NULL,
    exchange TEXT,
    pair TEXT,
    kind TEXT NOT NULL,
    details TEXT
);
CREATE TABLE archive_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES stream_sessions(id),
    dataset TEXT NOT NULL,
    exchange TEXT NOT NULL,
    pair TEXT NOT NULL,
    date TEXT NOT NULL,
    path TEXT NOT NULL,
    rows INTEGER NOT NULL,
    ts_min TEXT NOT NULL,
    ts_max TEXT NOT NULL,
    closed_at TEXT NOT NULL
);
CREATE INDEX idx_stream_events_session ON stream_events(session_id, ts);
CREATE INDEX idx_archive_files_date ON archive_files(date, exchange, pair);
```

**`db.rs` API** (async, sqlx `SqlitePool`, WAL on connect via
`SqliteConnectOptions::journal_mode`):

```rust
pub struct Db { pool: SqlitePool }
impl Db {
    pub async fn open(path: &Path) -> Result<Self, StorageError>; // creates file, runs sqlx::migrate!()
    pub async fn start_session(&self, config_json: &str) -> Result<i64, StorageError>;
    pub async fn end_session(&self, session_id: i64) -> Result<(), StorageError>;
    pub async fn record_event(&self, session_id: i64, ev: &StreamEvent) -> Result<(), StorageError>;
    pub async fn record_file(&self, session_id: i64, f: &FinalizedFile) -> Result<(), StorageError>;
    pub async fn events_for_date(&self, date: NaiveDate) -> Result<Vec<StreamEvent>, StorageError>;
    pub async fn files_for_date(&self, date: NaiveDate) -> Result<Vec<ArchiveFileRow>, StorageError>;
}
```

`StreamEvent { ts, exchange: Option<String>, pair: Option<String>, kind: EventKind,
details: Option<String> }`; `EventKind` enum
(`Connected | Disconnected | GapDetected | LateTrade | Error`) with `as_str()`/
`from_str` round-trip test.

**TDD:** tests open a `Db` in a tempdir; assert migration runs twice idempotently;
session lifecycle; event/file insert + query-by-date round-trips. Run with
`cargo test -p rr-storage`. Commit: `"Add SQLite schema v1 and Db repositories"`.

## Task 8: Coverage report (`rr-storage::status`)

**Files:**
- Create: `crates/rr-storage/src/status.rs`

Pure function + thin IO wrapper, so classification is fully unit-tested:

```rust
pub struct PairCoverage {
    pub exchange: String,
    pub pair: String,
    pub files: u64,
    pub trade_rows: u64,
    pub minutes_expected: u64,   // 1440, or minutes elapsed for today
    pub minutes_present: u64,
    pub minutes_quiet: u64,      // missing, but connected the whole minute
    pub minutes_gap: u64,        // missing, overlapping a disconnected interval
    pub gap_events: u64,
    pub late_events: u64,
}

/// Pure: classify from candle window starts + connection intervals.
pub fn classify(
    date: NaiveDate,
    now: DateTime<Utc>,
    present: &BTreeSet<DateTime<Utc>>,
    disconnected: &[(DateTime<Utc>, DateTime<Utc>)],
) -> (u64, u64, u64, u64); // (expected, present, quiet, gap)

/// IO: read candle parquet for the date (ts_open column only) + Db events/files,
/// build disconnected intervals (disconnected→next connected; session end with
/// no `ended_at` counts as disconnected-to-now), call `classify`.
pub async fn archive_status(data_dir: &Path, db: &Db, date: NaiveDate) -> Result<Vec<PairCoverage>, StorageError>;
```

**TDD:** unit tests on `classify` (full coverage; missing minute inside a
disconnect window → gap; missing minute outside → quiet; today truncates
expected; boundary minutes). Integration test: seed tempdir parquet (reuse Task 6
writer) + Db events, assert the report. Commit: `"Add archive coverage report"`.

## Task 9: Ingest supervisor (`rr-engine::market_data::supervisor`)

**Files:**
- Create: `crates/rr-engine/src/market_data/supervisor.rs`, `convert.rs`, `spec.rs`

**`spec.rs`:** the four M1 subscriptions as a const-ish fn (compile-time scope per
design):

```rust
pub struct PairSpec { pub exchange: &'static str, pub base: &'static str, pub quote: &'static str }
pub const PAIRS: [PairSpec; 4] = [
    PairSpec { exchange: "binance_spot", base: "btc", quote: "usdt" },
    PairSpec { exchange: "binance_spot", base: "eth", quote: "usdt" },
    PairSpec { exchange: "coinbase", base: "btc", quote: "usd" },
    PairSpec { exchange: "coinbase", base: "eth", quote: "usd" },
];
```

`pair` string convention everywhere: uppercase `BASE-QUOTE` (`BTC-USDT`).

**`convert.rs` (pure, TDD):** `fn to_trade_record(event: &MarketEvent<MarketDataInstrument, PublicTrade>) -> Result<TradeRecord, ConvertError>` —
maps `time_exchange`/`time_received`, formats the pair, converts price/amount via
`Decimal::from_f64(...).ok_or(ConvertError::NonFiniteNumber { .. })`, rejects
non-positive amounts with an error (never silently). Unit-test with hand-built
`MarketEvent` values (adjust field construction to what the Task 2 spike found).

**`supervisor.rs`:** the only IO-heavy async glue; keep each fn ≤100 lines by
splitting handlers.

```rust
pub struct SupervisorHandles {
    pub trade_tx: tokio::sync::mpsc::Sender<TradeRecord>,   // capacity 4096
    pub candle_tx: tokio::sync::mpsc::Sender<CandleRecord>, // capacity 1024
}

pub async fn run(db: Db, session_id: i64, handles: SupervisorHandles, shutdown: CancellationToken /* or watch channel */) -> Result<(), EngineError>
```

Loop body (`tokio::select!`):
- next stream event: `Event::Reconnecting(exchange)` → `tracing::warn!` +
  `record_event(Disconnected)`; `Event::Item(Err(e))` → `record_event(Error)`;
  `Event::Item(Ok(market_event))` → convert (error → `record_event(Error)`,
  continue), `gap.observe(...)` (Gap/Regression → `record_event(GapDetected)`
  with details JSON), `trade_tx.send(record).await` (backpressure point),
  `agg.ingest(...)` → emitted candles to `candle_tx`, `Late` →
  `record_event(LateTrade)`.
- 1s `tokio::time::interval` tick: `agg.flush_older_than(Utc::now() - 5s)` →
  candles to `candle_tx`.
- shutdown signal: break; then `flush_all()`, send remaining candles, drop
  senders (lets writer threads drain and finish), return.

Also record a `Connected` event per exchange after successful `init()`.

Unit tests cover `convert.rs` and the pure pieces; the loop itself is exercised by
the Task 11 smoke test (live) — do not mock the WebSocket layer (mock boundaries,
and barter-data IS the boundary; a fake stream injected via generics is acceptable
if cheap, otherwise skip). Commit: `"Add market data ingest supervisor"`.

## Task 10: Writer threads (`rr-storage::archive_task`)

**Files:**
- Create: `crates/rr-storage/src/archive_task.rs`

Bridges async→blocking: spawn one `std::thread` per dataset, owning a
`PartitionedWriter`, consuming `tokio::sync::mpsc::Receiver::blocking_recv()`;
on every finalized file, send `FinalizedFile` back over a small tokio mpsc to an
async task that calls `db.record_file`. Thread exits when the channel closes
(all senders dropped) after `finalize_all()`. Roll checks piggyback on a
`recv_timeout`-style pattern: use `blocking_recv` plus a check of
`roll_due(Utc::now())` after each message and a 1s `std::sync::mpsc::RecvTimeout`…
— **simplest correct version:** loop on
`receiver.blocking_recv_timeout(Duration::from_secs(1))` (tokio 1.52 provides
`blocking_recv_timeout`; verify on docs.rs, else wrap with a tokio runtime handle
and `recv()` + `tokio::time::timeout` from a `block_on`), appending on `Ok`,
checking rolls on timeout.

**TDD:** test: spawn the archive task with a tempdir; send 10 trades; drop sender;
join thread; assert parquet file exists, `FinalizedFile` arrived on the feedback
channel, rows == 10. Commit: `"Add blocking archive writer task"`.

## Task 11: CLI — `rr stream`

**Files:**
- Modify: `crates/rr-cli/src/main.rs`

clap derive: subcommands `Stream { #[arg(long, default_value = "./data")] data_dir: PathBuf }`
and `ArchiveStatus { #[arg(long)] date: Option<NaiveDate>, #[arg(long, default_value = "./data")] data_dir: PathBuf }`.

`stream` flow: init `tracing_subscriber` (env-filter, default `info`); create
`<data_dir>/rr.sqlite` via `Db::open`; `start_session` with the PAIRS config JSON;
spawn archive tasks (trades + candles); build barter streams; run supervisor with
ctrl-c wired to the shutdown token (`tokio::signal::ctrl_c`); on exit
`end_session`, log summary. All errors bubble as `anyhow::Result` from `main` —
exit nonzero, no panics. Stdout printing only in `archive-status` (Task 12) behind
the repo's established narrow `#[expect(clippy::print_stdout, ...)]` boundary fn.

**Tests:** `assert_cmd`: `rr stream --help` exits 0 and mentions `--data-dir`.
(Running `stream` itself needs network — covered by Task 13.)
Commit: `"Add rr stream command"`.

## Task 12: CLI — `rr archive-status`

Render `archive_status(...)` as an aligned text table (write a tiny formatter fn —
no table crate; YAGNI) showing the `PairCoverage` columns, one row per
exchange/pair, plus a one-line verdict per row: `OK` (no gap minutes) or
`GAPS: n`. Default date is **today UTC**; the overnight check is explicitly
`rr archive-status --date <yesterday>`.

**Tests:** `assert_cmd` against a tempdir seeded by the Task 8 integration
fixtures: exit 0, output contains the pair rows; `--date` with no data → exits 0
with "no data" message (not an error — an empty archive is a fact, not a fault).
Commit: `"Add rr archive-status command"`.

## Task 13: Live smoke test (`#[ignore]`)

**Files:**
- Create: `crates/rr-cli/tests/live_smoke.rs`

`#[ignore = "network: hits real exchange websockets"]` test: spawn the `rr` binary
via `std::process::Command` with `stream --data-dir <tempdir>`, sleep ~30s, send
SIGINT (`Command::new("kill").arg("-INT").arg(pid)` — `Child::kill` is SIGKILL and
would skip graceful shutdown), wait, assert exit 0, assert ≥1 trades parquet file
exists and `rr archive-status` reports presence.
Run manually: `cargo test -p rr-cli --test live_smoke -- --ignored`.
Commit: `"Add ignored live smoke test"`.

## Task 14: Gates, docs, PR

1. `prek run --all-files` · `cargo clippy --all-targets --all-features -- -D warnings`
   · `cargo test --workspace` · `cargo deny check` — all clean, fix everything.
2. Update `docs/architecture.md`: record-type ownership in `rr-storage`
   (engine→storage dependency direction), candle derivation (1m, from trades,
   5s grace), Parquet roll policy, SQLite schema v1 tables, CLI commands. Update
   README status line (`M1 in progress` → leave roadmap row update for
   verification).
3. PR `m1-market-data` → `main`: plain factual description of what exists now;
   note the Task 2 spike findings (trade-ID sequentiality per exchange).
4. **Verification (after merge, before calling M1 done):** overnight
   `rr stream` run; morning `rr archive-status --date <date>` output attached as
   evidence; README roadmap row → `done`.

---

## Execution notes for the implementer

- Read `CLAUDE.md` first; the lint wall is real (`unwrap_used` denied, tests
  included — use Result-returning tests or narrow `#[expect(..., reason)]`).
- barter-data 0.11.0 API: verify against docs.rs/barter-data/0.11.0 whenever a
  signature in this plan doesn't compile; the plan was written against the README
  and may drift in detail (subscription tuple shape, `MarketDataInstrumentKind`
  location). The Task 2 spike exists precisely to de-risk this.
- Never `f64` for money past `convert.rs`. Never silently skip an anomaly — every
  weird thing becomes a `tracing` line AND a `stream_events` row if it affects
  data completeness.
- Commit after every green step; keep each commit compiling and lint-clean.
