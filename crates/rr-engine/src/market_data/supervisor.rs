//! Ingest supervisor: drives the merged market stream, converts trades,
//! detects gaps, aggregates candles, and forwards records to the archive
//! channels while recording operational events under the stream session.
//!
//! Anomalies are handled per the design: a bad trade is dropped (with an
//! `Error` event) but never crashes ingest; a dead archive channel or a
//! failed database write is fatal — the process restarts rather than run
//! blind. Backpressure is real: channel sends `await` (never `try_send`),
//! so a slow writer slows ingest instead of dropping records.

use std::collections::BTreeSet;
use std::time::Duration;

use barter_data::error::DataError;
use barter_data::event::MarketEvent;
use barter_data::exchange::binance::spot::BinanceSpot;
use barter_data::exchange::coinbase::Coinbase;
use barter_data::streams::Streams;
use barter_data::streams::consumer::MarketStreamResult;
use barter_data::streams::reconnect::Event;
use barter_data::subscription::trade::{PublicTrade, PublicTrades};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt};
use rr_storage::db::{Db, EventKind, StreamEvent};
use rr_storage::records::{CandleRecord, TradeRecord};
use tokio::sync::{mpsc, watch};

use crate::error::EngineError;
use crate::market_data::candle::{CandleAggregator, IngestOutcome};
use crate::market_data::convert::{ConvertError, to_trade_record};
use crate::market_data::gap::{GapCheck, GapDetector};
use crate::market_data::spec::{PAIRS, exchanges, pair_string};

/// Senders feeding the blocking archive writer tasks. Capacities are the
/// caller's choice (the CLI uses 4096 trades / 1024 candles).
pub struct ArchiveSenders {
    /// Raw trade records, one per converted stream trade.
    pub trades: mpsc::Sender<TradeRecord>,
    /// Finalized 1-minute candles.
    pub candles: mpsc::Sender<CandleRecord>,
}

/// How far behind `Utc::now()` the tick flush watermark sits, absorbing
/// exchange timestamp skew before a quiet window is finalized.
const FLUSH_LAG: TimeDelta = TimeDelta::seconds(5);

/// Builds the real four-pair public-trade stream ([`PAIRS`]) and merges all
/// exchanges into one stream. Network-bound: covered by the live smoke test,
/// not unit tests.
///
/// # Errors
///
/// Returns [`EngineError::StreamInit`] if subscribing to any exchange fails.
pub async fn build_stream() -> Result<
    impl Stream<Item = MarketStreamResult<MarketDataInstrument, PublicTrade>> + Unpin,
    EngineError,
> {
    let binance = subscriptions(BinanceSpot::default(), "binance_spot");
    let coinbase = subscriptions(Coinbase, "coinbase");
    // Guards against an exchange id in PAIRS that no `subscriptions` call
    // matches (a silent drift would yield an empty subscription set).
    debug_assert_eq!(
        binance.len() + coinbase.len(),
        PAIRS.len(),
        "subscription exchange ids must match PAIRS exchange strings"
    );
    let streams = Streams::<PublicTrades>::builder()
        .subscribe(binance)
        .subscribe(coinbase)
        .init()
        .await?;
    Ok(streams.select_all())
}

/// The [`PAIRS`] entries for one exchange as barter subscription tuples.
fn subscriptions<Exchange: Copy>(
    exchange: Exchange,
    id: &str,
) -> Vec<(
    Exchange,
    &'static str,
    &'static str,
    MarketDataInstrumentKind,
    PublicTrades,
)> {
    let mut subs = Vec::new();
    for pair in &PAIRS {
        if pair.exchange == id {
            subs.push((
                exchange,
                pair.base,
                pair.quote,
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            ));
        }
    }
    subs
}

/// Runs the ingest loop over `stream` until `shutdown` signals, recording
/// stream events under `session_id`.
///
/// Records `Connected` per exchange at startup and `Disconnected` per
/// exchange on shutdown (so coverage reports never count post-shutdown time
/// as connected), then flushes all open candle windows before returning.
///
/// # Errors
///
/// Returns [`EngineError::StreamEnded`] if the stream yields `None`,
/// [`EngineError::ArchiveChannelClosed`] if a writer task is gone, or
/// [`EngineError::Storage`] if any event write fails.
pub async fn run<S>(
    mut stream: S,
    db: Db,
    session_id: i64,
    senders: ArchiveSenders,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EngineError>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, PublicTrade>> + Unpin,
{
    let mut state = Supervisor::new(db, session_id, senders);
    for exchange in exchanges() {
        state
            .record(EventKind::Connected, Some(exchange.to_owned()), None, None)
            .await?;
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event = stream.next() => match event {
                None => state.on_stream_ended().await?,
                Some(Event::Reconnecting(exchange)) => {
                    state.on_reconnecting(exchange.as_str()).await?;
                }
                Some(Event::Item(Err(error))) => state.on_stream_error(&error).await?,
                Some(Event::Item(Ok(market_event))) => {
                    state.on_market_event(&market_event).await?;
                }
            },
            _ = interval.tick() => state.flush(Utc::now() - FLUSH_LAG).await?,
            // Any signal (or a dropped sender) means shut down cleanly.
            _ = shutdown.changed() => break,
        }
    }
    state.finish().await
}

/// Mutable ingest state threaded through the loop handlers.
struct Supervisor {
    db: Db,
    session_id: i64,
    senders: ArchiveSenders,
    agg: CandleAggregator,
    gap: GapDetector,
    /// Exchanges that emitted `Reconnecting` and have not yet delivered a
    /// successful item; the next item closes the disconnected interval.
    reconnecting: BTreeSet<String>,
}

impl Supervisor {
    fn new(db: Db, session_id: i64, senders: ArchiveSenders) -> Self {
        Self {
            db,
            session_id,
            senders,
            agg: CandleAggregator::default(),
            gap: GapDetector::default(),
            reconnecting: BTreeSet::new(),
        }
    }

    /// Persists one stream event now; any failure is fatal.
    async fn record(
        &self,
        kind: EventKind,
        exchange: Option<String>,
        pair: Option<String>,
        details: Option<String>,
    ) -> Result<(), EngineError> {
        let event = StreamEvent {
            ts: Utc::now(),
            exchange,
            pair,
            kind,
            details,
        };
        self.db.record_event(self.session_id, &event).await?;
        Ok(())
    }

    /// The merged stream ended: barter streams reconnect internally, so this
    /// is unexpected and fatal.
    async fn on_stream_ended(&self) -> Result<(), EngineError> {
        tracing::error!("market data stream ended unexpectedly");
        self.record(
            EventKind::Error,
            None,
            None,
            Some("stream ended".to_owned()),
        )
        .await?;
        Err(EngineError::StreamEnded)
    }

    /// A connection dropped; barter emits no explicit "reconnected" event,
    /// so mark the exchange and close the interval on its next item.
    ///
    /// Repeated `Reconnecting` for the same exchange before any item records
    /// `Disconnected` more than once; coverage reporting
    /// (`rr_storage::status::disconnected_intervals`) folds consecutive
    /// down-transitions, so the duplicates are harmless.
    async fn on_reconnecting(&mut self, exchange: &str) -> Result<(), EngineError> {
        tracing::warn!(exchange, "market stream disconnected; reconnecting");
        self.reconnecting.insert(exchange.to_owned());
        self.record(
            EventKind::Disconnected,
            Some(exchange.to_owned()),
            None,
            None,
        )
        .await
    }

    /// A per-message stream error: recorded and skipped, never fatal.
    async fn on_stream_error(&self, error: &DataError) -> Result<(), EngineError> {
        tracing::warn!(%error, "market stream item error");
        self.record(EventKind::Error, None, None, Some(error.to_string()))
            .await
    }

    /// One successful market event: close any pending reconnect interval,
    /// convert, gap-check, archive, and aggregate.
    async fn on_market_event(
        &mut self,
        event: &MarketEvent<MarketDataInstrument, PublicTrade>,
    ) -> Result<(), EngineError> {
        let exchange = event.exchange.as_str();
        if self.reconnecting.remove(exchange) {
            self.record(EventKind::Connected, Some(exchange.to_owned()), None, None)
                .await?;
        }
        let record = match to_trade_record(event, Utc::now()) {
            Ok(record) => record,
            Err(error) => return self.on_bad_trade(event, &error).await,
        };
        self.check_gap(&record).await?;
        if self.senders.trades.send(record.clone()).await.is_err() {
            return Err(EngineError::ArchiveChannelClosed { dataset: "trades" });
        }
        let (candles, outcome) = self.agg.ingest(&record);
        self.send_candles(candles).await?;
        if outcome == IngestOutcome::Late {
            self.on_late_trade(&record).await?;
        }
        Ok(())
    }

    /// A trade that cannot be converted is dropped with an `Error` event —
    /// never archived, never fatal.
    async fn on_bad_trade(
        &self,
        event: &MarketEvent<MarketDataInstrument, PublicTrade>,
        error: &ConvertError,
    ) -> Result<(), EngineError> {
        let exchange = event.exchange.as_str();
        let pair = pair_string(
            event.instrument.base.name().as_str(),
            event.instrument.quote.name().as_str(),
        );
        tracing::warn!(
            exchange,
            pair,
            trade_id = %event.kind.id,
            %error,
            "dropping unconvertible trade"
        );
        let details = serde_json::json!({
            "trade_id": event.kind.id,
            "error": error.to_string(),
        })
        .to_string();
        self.record(
            EventKind::Error,
            Some(exchange.to_owned()),
            Some(pair),
            Some(details),
        )
        .await
    }

    /// Feeds the gap detector; every anomaly becomes a `GapDetected` event.
    async fn check_gap(&mut self, record: &TradeRecord) -> Result<(), EngineError> {
        let details = match self
            .gap
            .observe(&record.exchange, &record.pair, &record.trade_id)
        {
            GapCheck::Ok | GapCheck::Untracked => return Ok(()),
            GapCheck::Gap { expected, got } => {
                serde_json::json!({ "kind": "gap", "expected": expected, "got": got })
            }
            GapCheck::Regression { last, got } => {
                serde_json::json!({ "kind": "regression", "last": last, "got": got })
            }
            GapCheck::NonSequential => {
                serde_json::json!({ "kind": "non_sequential", "trade_id": record.trade_id })
            }
        };
        tracing::warn!(
            exchange = %record.exchange,
            pair = %record.pair,
            %details,
            "trade id sequence anomaly"
        );
        self.record(
            EventKind::GapDetected,
            Some(record.exchange.clone()),
            Some(record.pair.clone()),
            Some(details.to_string()),
        )
        .await
    }

    /// A trade for an already-emitted window: archived raw upstream, but
    /// recorded so coverage reports know the candle is incomplete.
    async fn on_late_trade(&self, record: &TradeRecord) -> Result<(), EngineError> {
        tracing::warn!(
            exchange = %record.exchange,
            pair = %record.pair,
            trade_id = %record.trade_id,
            ts_exchange = %record.ts_exchange,
            "late trade for an already-emitted candle window"
        );
        let details = serde_json::json!({
            "trade_id": record.trade_id,
            "ts": record.ts_exchange.to_rfc3339(),
        })
        .to_string();
        self.record(
            EventKind::LateTrade,
            Some(record.exchange.clone()),
            Some(record.pair.clone()),
            Some(details),
        )
        .await
    }

    /// Tick flush: finalize every window that ended at or before `watermark`.
    async fn flush(&mut self, watermark: DateTime<Utc>) -> Result<(), EngineError> {
        let candles = self.agg.flush_older_than(watermark);
        self.send_candles(candles).await
    }

    async fn send_candles(&self, candles: Vec<CandleRecord>) -> Result<(), EngineError> {
        for candle in candles {
            if self.senders.candles.send(candle).await.is_err() {
                return Err(EngineError::ArchiveChannelClosed { dataset: "candles" });
            }
        }
        Ok(())
    }

    /// Clean shutdown: close every exchange's connected interval, flush all
    /// open windows, and drop the senders so writer tasks drain and finish.
    async fn finish(mut self) -> Result<(), EngineError> {
        for exchange in exchanges() {
            self.record(
                EventKind::Disconnected,
                Some(exchange.to_owned()),
                None,
                None,
            )
            .await?;
        }
        let candles = self.agg.flush_all();
        self.send_candles(candles).await
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use barter_data::event::MarketEvent;
    use barter_data::subscription::trade::PublicTrade;
    use barter_instrument::Side as BarterSide;
    use barter_instrument::exchange::ExchangeId;
    use barter_instrument::instrument::market_data::MarketDataInstrument;
    use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
    use chrono::{DateTime, Utc};
    use futures::StreamExt as _;
    use futures::stream::{self, Stream};
    use rr_storage::db::{Db, EventKind, StreamEvent};
    use rr_storage::records::{CandleRecord, TradeRecord};
    use rust_decimal::Decimal;
    use tokio::sync::{mpsc, watch};
    use tokio::task::JoinHandle;

    use crate::error::EngineError;
    use crate::market_data::supervisor::{ArchiveSenders, run};

    type TestResult = Result<(), Box<dyn Error>>;
    type FakeEvent =
        barter_data::streams::consumer::MarketStreamResult<MarketDataInstrument, PublicTrade>;

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn item(
        exchange: ExchangeId,
        pair: (&str, &str),
        kind: PublicTrade,
        ts_str: &str,
    ) -> FakeEvent {
        barter_data::streams::reconnect::Event::Item(Ok(MarketEvent {
            time_exchange: ts(ts_str),
            time_received: ts(ts_str),
            exchange,
            instrument: MarketDataInstrument::from((
                pair.0,
                pair.1,
                MarketDataInstrumentKind::Spot,
            )),
            kind,
        }))
    }

    fn public_trade(id: &str, price: f64, amount: f64) -> PublicTrade {
        PublicTrade {
            id: id.to_owned(),
            price,
            amount,
            side: BarterSide::Buy,
        }
    }

    /// Binance BTC-USDT trade at price 100.0, amount 1.0.
    fn trade(id: &str, ts_str: &str) -> FakeEvent {
        item(
            ExchangeId::BinanceSpot,
            ("btc", "usdt"),
            public_trade(id, 100.0, 1.0),
            ts_str,
        )
    }

    struct Harness {
        db: Db,
        trades_rx: mpsc::Receiver<TradeRecord>,
        candles_rx: mpsc::Receiver<CandleRecord>,
        shutdown_tx: watch::Sender<bool>,
        handle: JoinHandle<Result<(), EngineError>>,
        _tmp: tempfile::TempDir,
    }

    async fn spawn_with<S>(stream: S) -> Result<Harness, Box<dyn Error>>
    where
        S: Stream<Item = FakeEvent> + Unpin + Send + 'static,
    {
        let tmp = tempfile::tempdir()?;
        let db = Db::open(&tmp.path().join("rr.sqlite")).await?;
        let session_id = db.start_session("{}").await?;
        let (trades_tx, trades_rx) = mpsc::channel(4);
        let (candles_tx, candles_rx) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let senders = ArchiveSenders {
            trades: trades_tx,
            candles: candles_tx,
        };
        let handle = tokio::spawn(run(stream, db.clone(), session_id, senders, shutdown_rx));
        Ok(Harness {
            db,
            trades_rx,
            candles_rx,
            shutdown_tx,
            handle,
            _tmp: tmp,
        })
    }

    /// Spawns `run` over `events` followed by a never-ending tail, so the
    /// supervisor only stops via the shutdown signal.
    async fn spawn(events: Vec<FakeEvent>) -> Result<Harness, Box<dyn Error>> {
        spawn_with(stream::iter(events).chain(stream::pending())).await
    }

    async fn recv<T>(rx: &mut mpsc::Receiver<T>) -> Result<T, Box<dyn Error>> {
        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await?;
        Ok(received.ok_or("channel closed")?)
    }

    /// All recorded events in `(ts, id)` order, robust to tests straddling
    /// a UTC midnight.
    async fn recorded_events(db: &Db) -> Result<Vec<StreamEvent>, Box<dyn Error>> {
        let today = Utc::now().date_naive();
        let mut events = Vec::new();
        let yesterday = today.pred_opt().ok_or("date underflow")?;
        let tomorrow = today.succ_opt().ok_or("date overflow")?;
        for date in [yesterday, today, tomorrow] {
            events.extend(db.events_for_date(date).await?);
        }
        Ok(events)
    }

    fn connection_log(events: &[StreamEvent]) -> Vec<(EventKind, Option<&str>)> {
        let mut log = Vec::new();
        for event in events {
            if event.kind == EventKind::Connected || event.kind == EventKind::Disconnected {
                log.push((event.kind, event.exchange.as_deref()));
            }
        }
        log
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn startup_records_connected_and_shutdown_records_disconnected() -> TestResult {
        let mut h = spawn(Vec::new()).await?;
        h.shutdown_tx.send(true)?;
        h.handle.await??;

        let events = recorded_events(&h.db).await?;
        assert_eq!(
            connection_log(&events),
            vec![
                (EventKind::Connected, Some("binance_spot")),
                (EventKind::Connected, Some("coinbase")),
                (EventKind::Disconnected, Some("binance_spot")),
                (EventKind::Disconnected, Some("coinbase")),
            ]
        );
        // No trades, no candles; channels close when the supervisor returns.
        assert!(h.trades_rx.recv().await.is_none());
        assert!(h.candles_rx.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn trades_flow_to_channel_and_window_advance_emits_candle() -> TestResult {
        let mut h = spawn(vec![
            trade("1", "2026-06-12T10:00:30Z"),
            trade("2", "2026-06-12T10:01:05Z"),
        ])
        .await?;

        let first = recv(&mut h.trades_rx).await?;
        assert_eq!(first.exchange, "binance_spot");
        assert_eq!(first.pair, "BTC-USDT");
        assert_eq!(first.price, dec("100"));
        assert_eq!(first.amount, dec("1"));
        assert_eq!(first.trade_id, "1");
        let second = recv(&mut h.trades_rx).await?;
        assert_eq!(second.trade_id, "2");

        // The 10:00 window is finalized (by ingest advance or tick flush).
        let candle = recv(&mut h.candles_rx).await?;
        assert_eq!(candle.ts_open, ts("2026-06-12T10:00:00Z"));
        assert_eq!(candle.close, dec("100"));
        assert_eq!(candle.trade_count, 1);

        h.shutdown_tx.send(true)?;
        h.handle.await??;
        // Shutdown flushes the still-open 10:01 window, then closes.
        let flushed = recv(&mut h.candles_rx).await?;
        assert_eq!(flushed.ts_open, ts("2026-06-12T10:01:00Z"));
        assert!(h.candles_rx.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn reconnecting_records_disconnect_and_next_item_records_connect() -> TestResult {
        let mut h = spawn(vec![
            trade("1", "2026-06-12T10:00:01Z"),
            barter_data::streams::reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
            // Another exchange's item must NOT close binance's interval.
            item(
                ExchangeId::Coinbase,
                ("btc", "usd"),
                public_trade("900", 99.0, 1.0),
                "2026-06-12T10:00:02Z",
            ),
            trade("2", "2026-06-12T10:00:03Z"),
        ])
        .await?;

        for _ in 0..3 {
            recv(&mut h.trades_rx).await?;
        }
        h.shutdown_tx.send(true)?;
        h.handle.await??;

        let events = recorded_events(&h.db).await?;
        assert_eq!(
            connection_log(&events),
            vec![
                (EventKind::Connected, Some("binance_spot")),
                (EventKind::Connected, Some("coinbase")),
                (EventKind::Disconnected, Some("binance_spot")),
                // Closed by binance trade "2", not by the coinbase item.
                (EventKind::Connected, Some("binance_spot")),
                (EventKind::Disconnected, Some("binance_spot")),
                (EventKind::Disconnected, Some("coinbase")),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn bad_trade_records_error_and_is_never_archived() -> TestResult {
        let mut h = spawn(vec![
            item(
                ExchangeId::BinanceSpot,
                ("btc", "usdt"),
                public_trade("13", 100.0, 0.0),
                "2026-06-12T10:00:01Z",
            ),
            trade("14", "2026-06-12T10:00:02Z"),
        ])
        .await?;

        // Only the good trade reaches the archive channel.
        let archived = recv(&mut h.trades_rx).await?;
        assert_eq!(archived.trade_id, "14");
        h.shutdown_tx.send(true)?;
        h.handle.await??;
        assert!(h.trades_rx.recv().await.is_none());

        let events = recorded_events(&h.db).await?;
        let Some(error) = events.iter().find(|e| e.kind == EventKind::Error) else {
            panic!("expected an Error event for the bad trade, got {events:?}");
        };
        assert_eq!(error.exchange.as_deref(), Some("binance_spot"));
        assert_eq!(error.pair.as_deref(), Some("BTC-USDT"));
        let details: serde_json::Value =
            serde_json::from_str(error.details.as_deref().ok_or("missing details")?)?;
        assert_eq!(details["trade_id"], "13");
        assert!(
            details["error"]
                .as_str()
                .ok_or("error not a string")?
                .contains("amount"),
            "details should name the offending field: {details}"
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn trade_id_jump_records_gap_detected_with_details() -> TestResult {
        let mut h = spawn(vec![
            trade("1", "2026-06-12T10:00:01Z"),
            trade("5", "2026-06-12T10:00:02Z"),
        ])
        .await?;

        // Both trades are archived; a gap never blocks the raw archive.
        recv(&mut h.trades_rx).await?;
        recv(&mut h.trades_rx).await?;
        h.shutdown_tx.send(true)?;
        h.handle.await??;

        let events = recorded_events(&h.db).await?;
        let Some(gap) = events.iter().find(|e| e.kind == EventKind::GapDetected) else {
            panic!("expected a GapDetected event, got {events:?}");
        };
        assert_eq!(gap.exchange.as_deref(), Some("binance_spot"));
        assert_eq!(gap.pair.as_deref(), Some("BTC-USDT"));
        let details: serde_json::Value =
            serde_json::from_str(gap.details.as_deref().ok_or("missing details")?)?;
        assert_eq!(
            details,
            serde_json::json!({ "kind": "gap", "expected": 2, "got": 5 })
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn late_trade_is_archived_raw_and_records_late_trade_event() -> TestResult {
        let mut h = spawn(vec![
            trade("1", "2026-06-12T10:00:30Z"),
            // Advances the window: emits the 10:00 candle.
            trade("2", "2026-06-12T10:01:05Z"),
            // Back into the already-emitted 10:00 window: late.
            trade("3", "2026-06-12T10:00:50Z"),
        ])
        .await?;

        let mut archived = Vec::new();
        for _ in 0..3 {
            archived.push(recv(&mut h.trades_rx).await?.trade_id);
        }
        assert_eq!(archived, vec!["1", "2", "3"]);
        h.shutdown_tx.send(true)?;
        h.handle.await??;

        let events = recorded_events(&h.db).await?;
        let Some(late) = events.iter().find(|e| e.kind == EventKind::LateTrade) else {
            panic!("expected a LateTrade event, got {events:?}");
        };
        assert_eq!(late.pair.as_deref(), Some("BTC-USDT"));
        let details: serde_json::Value =
            serde_json::from_str(late.details.as_deref().ok_or("missing details")?)?;
        assert_eq!(details["trade_id"], "3");
        assert_eq!(details["ts"], "2026-06-12T10:00:50+00:00");

        // The late trade's window was already emitted exactly once.
        let candle = recv(&mut h.candles_rx).await?;
        assert_eq!(candle.ts_open, ts("2026-06-12T10:00:00Z"));
        assert_eq!(candle.trade_count, 1);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn tick_flush_emits_quiet_window_without_new_trades() -> TestResult {
        // A trade well in the past: its window ended long before the
        // watermark, so the 1s interval tick alone must finalize it —
        // no shutdown, no further trades (real-time wait of ~1s).
        let old = (Utc::now() - chrono::TimeDelta::minutes(10))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut h = spawn(vec![trade("1", &old)]).await?;

        recv(&mut h.trades_rx).await?;
        let candle = recv(&mut h.candles_rx).await?;
        assert_eq!(candle.trade_count, 1);
        assert_eq!(candle.close, dec("100"));

        h.shutdown_tx.send(true)?;
        h.handle.await??;
        // Nothing left to flush at shutdown.
        assert!(h.candles_rx.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn stream_end_records_error_and_returns_stream_ended() -> TestResult {
        let mut h = spawn_with(stream::iter(vec![trade("1", "2026-06-12T10:00:01Z")])).await?;

        match h.handle.await? {
            Err(EngineError::StreamEnded) => {}
            other => panic!("expected StreamEnded, got {other:?}"),
        }
        // The trade before the end was still archived.
        assert_eq!(recv(&mut h.trades_rx).await?.trade_id, "1");

        let events = recorded_events(&h.db).await?;
        let Some(error) = events.iter().find(|e| e.kind == EventKind::Error) else {
            panic!("expected an Error event, got {events:?}");
        };
        assert_eq!(error.details.as_deref(), Some("stream ended"));
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn dead_trade_channel_is_fatal() -> TestResult {
        let h = spawn(vec![trade("1", "2026-06-12T10:00:01Z")]).await?;
        // Drop the trade receiver so the supervisor's send fails.
        drop(h.trades_rx);
        match h.handle.await? {
            Err(EngineError::ArchiveChannelClosed { dataset: "trades" }) => {}
            other => panic!("expected ArchiveChannelClosed trades, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn per_message_stream_error_is_recorded_and_ingest_continues() -> TestResult {
        let mut h = spawn(vec![
            barter_data::streams::reconnect::Event::Item(Err(
                barter_data::error::DataError::Socket("boom".to_owned()),
            )),
            trade("1", "2026-06-12T10:00:01Z"),
        ])
        .await?;

        // The trade after the error still flows.
        assert_eq!(recv(&mut h.trades_rx).await?.trade_id, "1");
        h.shutdown_tx.send(true)?;
        h.handle.await??;

        let events = recorded_events(&h.db).await?;
        let Some(error) = events.iter().find(|e| e.kind == EventKind::Error) else {
            panic!("expected an Error event, got {events:?}");
        };
        assert!(
            error
                .details
                .as_deref()
                .ok_or("missing details")?
                .contains("boom"),
            "details should carry the stream error: {error:?}"
        );
        Ok(())
    }
}
