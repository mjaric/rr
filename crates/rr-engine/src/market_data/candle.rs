//! Pure 1-minute candle aggregation over tumbling UTC windows `[t, t+60s)`
//! keyed by `(exchange, pair)` and aligned on `ts_exchange`.
//!
//! The aggregator holds no clock and performs no IO: the caller drives it with
//! `ingest` per trade, `flush_older_than` on a watermark tick, and `flush_all`
//! at shutdown. `open`/`close` follow ingest (stream) order within a window —
//! stream order is the exchange's promise; trades are not re-sorted by
//! timestamp inside a window.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use rr_storage::records::{CandleRecord, TradeRecord};

type Key = (String, String);

/// Result of feeding one trade to the aggregator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    /// Trade was folded into an open (or newly opened) window.
    Ok,
    /// Trade belongs to a window that was already emitted; archived raw by the
    /// caller, never folded into a candle.
    Late,
}

/// Per-key stream-order candle builder over tumbling 1-minute UTC windows.
#[derive(Debug, Default)]
pub struct CandleAggregator {
    /// Open windows keyed by (exchange, pair) → (window start → building candle).
    open: BTreeMap<Key, BTreeMap<DateTime<Utc>, CandleRecord>>,
    /// Highest emitted window start per key; any window at or before it is late.
    emitted_through: BTreeMap<Key, DateTime<Utc>>,
}

impl CandleAggregator {
    /// Folds one trade into its window and emits every window for the same
    /// key that the trade proves complete (strictly older than its own).
    ///
    /// Returns the emitted candles sorted by `ts_open`, and whether the trade
    /// was folded or arrived late (its window already emitted).
    pub fn ingest(&mut self, trade: &TradeRecord) -> (Vec<CandleRecord>, IngestOutcome) {
        let window = window_start(trade.ts_exchange);
        let key: Key = (trade.exchange.clone(), trade.pair.clone());
        if self.emitted_through.get(&key).is_some_and(|t| window <= *t) {
            return (Vec::new(), IngestOutcome::Late);
        }

        let windows = self.open.entry(key.clone()).or_default();
        fold(windows, window, trade);

        // The trade proves every strictly older open window of this key complete.
        let still_open = windows.split_off(&window);
        let completed = std::mem::replace(windows, still_open);
        let mut emitted = Vec::with_capacity(completed.len());
        for (start, candle) in completed {
            advance(&mut self.emitted_through, &key, start);
            emitted.push(candle);
        }
        (emitted, IngestOutcome::Ok)
    }

    /// Emits every open window whose end (`ts_open + 60s`) is at or before
    /// `watermark`, across all keys, sorted by `ts_open`.
    pub fn flush_older_than(&mut self, watermark: DateTime<Utc>) -> Vec<CandleRecord> {
        self.flush_if(|start| start + TimeDelta::seconds(60) <= watermark)
    }

    /// Emits every open window across all keys, sorted by `ts_open`.
    pub fn flush_all(&mut self) -> Vec<CandleRecord> {
        self.flush_if(|_| true)
    }

    fn flush_if(&mut self, due: impl Fn(DateTime<Utc>) -> bool) -> Vec<CandleRecord> {
        let mut emitted = Vec::new();
        for (key, windows) in &mut self.open {
            let starts: Vec<DateTime<Utc>> = windows
                .keys()
                .take_while(|start| due(**start))
                .copied()
                .collect();
            for start in starts {
                let Some(candle) = windows.remove(&start) else {
                    continue;
                };
                advance(&mut self.emitted_through, key, start);
                emitted.push(candle);
            }
        }
        self.open.retain(|_, windows| !windows.is_empty());
        emitted.sort_by_key(|candle| candle.ts_open);
        emitted
    }
}

/// Folds the trade into the window's building candle, opening it on first
/// trade. `open`/`close` follow ingest order, not `ts_exchange` order.
fn fold(
    windows: &mut BTreeMap<DateTime<Utc>, CandleRecord>,
    window: DateTime<Utc>,
    trade: &TradeRecord,
) {
    if let Some(candle) = windows.get_mut(&window) {
        candle.high = candle.high.max(trade.price);
        candle.low = candle.low.min(trade.price);
        candle.close = trade.price;
        candle.volume += trade.amount;
        candle.trade_count += 1;
    } else {
        windows.insert(
            window,
            CandleRecord {
                exchange: trade.exchange.clone(),
                pair: trade.pair.clone(),
                ts_open: window,
                open: trade.price,
                high: trade.price,
                low: trade.price,
                close: trade.price,
                volume: trade.amount,
                trade_count: 1,
            },
        );
    }
}

/// Records that the key's windows are emitted up to and including `start`.
fn advance(emitted_through: &mut BTreeMap<Key, DateTime<Utc>>, key: &Key, start: DateTime<Utc>) {
    let through = emitted_through.entry(key.clone()).or_insert(start);
    if *through < start {
        *through = start;
    }
}

/// Floor of `ts` to its tumbling UTC minute `[t, t+60s)`.
fn window_start(ts: DateTime<Utc>) -> DateTime<Utc> {
    let secs = ts.timestamp() - ts.timestamp().rem_euclid(60);
    // `unwrap_or` is unreachable: `secs` came from a valid `DateTime`.
    DateTime::from_timestamp(secs, 0).unwrap_or(ts)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use rr_storage::records::{Side, TradeRecord};
    use rust_decimal::Decimal;

    use crate::market_data::candle::{CandleAggregator, IngestOutcome};

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn trade_on(exchange: &str, pair: &str, ts: &str, price: &str, amount: &str) -> TradeRecord {
        let ts_exchange: DateTime<Utc> = ts.parse().unwrap();
        TradeRecord {
            exchange: exchange.to_owned(),
            pair: pair.to_owned(),
            ts_exchange,
            ts_received: ts_exchange,
            price: dec(price),
            amount: dec(amount),
            side: Side::Buy,
            trade_id: "t".to_owned(),
        }
    }

    fn trade(pair: &str, ts: &str, price: &str, amount: &str) -> TradeRecord {
        trade_on("binance_spot", pair, ts, price, amount)
    }

    #[test]
    fn one_trade_flush_all_yields_one_candle() {
        let mut agg = CandleAggregator::default();
        let (emitted, outcome) =
            agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:30Z", "100", "1.5"));
        assert!(emitted.is_empty());
        assert_eq!(outcome, IngestOutcome::Ok);

        let candles = agg.flush_all();
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        assert_eq!(c.exchange, "binance_spot");
        assert_eq!(c.pair, "BTC-USDT");
        assert_eq!(c.ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");
        assert_eq!(c.open, dec("100"));
        assert_eq!(c.high, dec("100"));
        assert_eq!(c.low, dec("100"));
        assert_eq!(c.close, dec("100"));
        assert_eq!(c.volume, dec("1.5"));
        assert_eq!(c.trade_count, 1);
    }

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

    #[test]
    fn ohlcv_folds_multiple_trades_in_one_minute() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:01Z", "100", "1"));
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:20Z", "105", "2"));
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:40Z", "95", "3"));
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:59Z", "102", "4"));

        let candles = agg.flush_all();
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        assert_eq!(c.open, dec("100"));
        assert_eq!(c.high, dec("105"));
        assert_eq!(c.low, dec("95"));
        assert_eq!(c.close, dec("102"));
        assert_eq!(c.volume, dec("10"));
        assert_eq!(c.trade_count, 4);
    }

    #[test]
    fn out_of_order_trade_within_open_window_is_folded_in_stream_order() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:30Z", "100", "1"));
        let (emitted, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:10Z", "110", "2"));
        assert!(emitted.is_empty());
        assert_eq!(outcome, IngestOutcome::Ok);

        let candles = agg.flush_all();
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        // Stream order, not timestamp order: open is the first ingested trade,
        // close is the last ingested trade.
        assert_eq!(c.open, dec("100"));
        assert_eq!(c.close, dec("110"));
        assert_eq!(c.high, dec("110"));
        assert_eq!(c.low, dec("100"));
        assert_eq!(c.volume, dec("3"));
        assert_eq!(c.trade_count, 2);
    }

    #[test]
    fn trade_for_already_emitted_window_is_late_and_mutates_nothing() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:30Z", "100", "1"));
        let (emitted, _) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:01:10Z", "101", "2"));
        assert_eq!(emitted.len(), 1);

        let (emitted, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:55Z", "999", "9"));
        assert!(emitted.is_empty());
        assert_eq!(outcome, IngestOutcome::Late);

        // The still-open 10:01 window must be untouched by the late trade.
        let candles = agg.flush_all();
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        assert_eq!(c.ts_open.to_rfc3339(), "2026-06-12T10:01:00+00:00");
        assert_eq!(c.close, dec("101"));
        assert_eq!(c.volume, dec("2"));
        assert_eq!(c.trade_count, 1);
    }

    #[test]
    fn older_window_trade_before_any_emission_is_not_late() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:05:30Z", "100", "1"));
        // Earlier window, but nothing was emitted for this key yet: it opens a
        // second concurrent window instead of being dropped as late.
        let (emitted, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:10Z", "90", "2"));
        assert!(emitted.is_empty());
        assert_eq!(outcome, IngestOutcome::Ok);

        let candles = agg.flush_all();
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");
        assert_eq!(candles[0].close, dec("90"));
        assert_eq!(candles[1].ts_open.to_rfc3339(), "2026-06-12T10:05:00+00:00");
        assert_eq!(candles[1].close, dec("100"));
    }

    #[test]
    fn ingest_emission_finalizes_all_older_open_windows_for_the_key() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:05:30Z", "100", "1"));
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:10Z", "90", "2"));
        let (emitted, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:06:01Z", "105", "3"));
        assert_eq!(outcome, IngestOutcome::Ok);
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");
        assert_eq!(emitted[1].ts_open.to_rfc3339(), "2026-06-12T10:05:00+00:00");

        // Both emitted windows are now late for this key.
        let (_, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:50Z", "1", "1"));
        assert_eq!(outcome, IngestOutcome::Late);
        let (_, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:05:50Z", "1", "1"));
        assert_eq!(outcome, IngestOutcome::Late);
    }

    #[test]
    fn flush_older_than_emits_only_windows_ending_at_or_before_watermark() {
        let mut agg = CandleAggregator::default();
        // Ingest newest-first so both windows stay open (nothing emitted yet).
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:01:30Z", "101", "1"));
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:30Z", "100", "1"));

        // Watermark inside the first window: nothing has ended yet.
        assert!(
            agg.flush_older_than(ts("2026-06-12T10:00:59.999Z"))
                .is_empty()
        );

        // Watermark exactly at the first window's end: that window is emitted.
        let emitted = agg.flush_older_than(ts("2026-06-12T10:01:00Z"));
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");

        // A trade for the flushed window is now late.
        let (_, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:10Z", "1", "1"));
        assert_eq!(outcome, IngestOutcome::Late);

        // The second window is still open until its own end passes.
        let emitted = agg.flush_older_than(ts("2026-06-12T10:02:00Z"));
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].ts_open.to_rfc3339(), "2026-06-12T10:01:00+00:00");
        assert!(agg.flush_all().is_empty());
    }

    #[test]
    fn empty_minutes_yield_nothing() {
        let mut agg = CandleAggregator::default();
        assert!(agg.flush_all().is_empty());
        assert!(agg.flush_older_than(ts("2026-06-12T10:10:00Z")).is_empty());

        let (e1, _) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:30Z", "100", "1"));
        let (e2, _) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:02:30Z", "102", "1"));
        let mut candles = e1;
        candles.extend(e2);
        candles.extend(agg.flush_all());
        let opens: Vec<String> = candles.iter().map(|c| c.ts_open.to_rfc3339()).collect();
        // No synthetic row for the trade-less 10:01 minute, anywhere.
        assert_eq!(
            opens,
            vec!["2026-06-12T10:00:00+00:00", "2026-06-12T10:02:00+00:00"]
        );
    }

    #[test]
    fn trade_exactly_on_minute_boundary_opens_the_next_window() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:59.999Z", "100", "1"));
        let (emitted, outcome) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:01:00Z", "101", "1"));
        assert_eq!(outcome, IngestOutcome::Ok);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");

        let candles = agg.flush_all();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].ts_open.to_rfc3339(), "2026-06-12T10:01:00+00:00");
        assert_eq!(candles[0].open, dec("101"));
    }

    #[test]
    fn independent_exchange_pair_keys_do_not_interfere() {
        let mut agg = CandleAggregator::default();
        agg.ingest(&trade("BTC-USDT", "2026-06-12T10:00:10Z", "100", "1"));
        agg.ingest(&trade("ETH-USDT", "2026-06-12T10:00:20Z", "10", "5"));
        agg.ingest(&trade_on(
            "coinbase",
            "BTC-USDT",
            "2026-06-12T10:00:30Z",
            "99",
            "2",
        ));

        // Advancing binance BTC-USDT emits only that key's window.
        let (emitted, _) = agg.ingest(&trade("BTC-USDT", "2026-06-12T10:01:05Z", "101", "1"));
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].exchange, "binance_spot");
        assert_eq!(emitted[0].pair, "BTC-USDT");

        // Lateness is per key: the same minute is still open for the others.
        let (_, outcome) = agg.ingest(&trade("ETH-USDT", "2026-06-12T10:00:40Z", "11", "1"));
        assert_eq!(outcome, IngestOutcome::Ok);
        let (_, outcome) = agg.ingest(&trade_on(
            "coinbase",
            "BTC-USDT",
            "2026-06-12T10:00:50Z",
            "98",
            "1",
        ));
        assert_eq!(outcome, IngestOutcome::Ok);

        let candles = agg.flush_all();
        assert_eq!(candles.len(), 3);
        // Sorted by ts_open: the two still-open 10:00 windows, then 10:01.
        assert_eq!(candles[0].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");
        assert_eq!(candles[1].ts_open.to_rfc3339(), "2026-06-12T10:00:00+00:00");
        assert_eq!(candles[2].ts_open.to_rfc3339(), "2026-06-12T10:01:00+00:00");
    }

    mod props {
        use chrono::TimeDelta;
        use proptest::prelude::{prop, prop_assert, prop_assert_eq, proptest};
        use rust_decimal::Decimal;

        use crate::market_data::candle::tests::ts;
        use crate::market_data::candle::{CandleAggregator, IngestOutcome};
        use rr_storage::records::{Side, TradeRecord};

        proptest! {
            #[test]
            fn single_minute_candle_matches_its_trades(
                raw in prop::collection::vec(
                    (1i64..1_000_000_000, 1i64..1_000_000_000, 0i64..60_000),
                    1..50,
                )
            ) {
                let base = ts("2026-06-12T10:00:00Z");
                let trades: Vec<TradeRecord> = raw
                    .iter()
                    .map(|&(price, amount, offset_ms)| TradeRecord {
                        exchange: "binance_spot".to_owned(),
                        pair: "BTC-USDT".to_owned(),
                        ts_exchange: base + TimeDelta::milliseconds(offset_ms),
                        ts_received: base + TimeDelta::milliseconds(offset_ms),
                        price: Decimal::new(price, 4),
                        amount: Decimal::new(amount, 6),
                        side: Side::Buy,
                        trade_id: "t".to_owned(),
                    })
                    .collect();

                let mut agg = CandleAggregator::default();
                for t in &trades {
                    let (emitted, outcome) = agg.ingest(t);
                    prop_assert!(emitted.is_empty());
                    prop_assert_eq!(outcome, IngestOutcome::Ok);
                }

                let candles = agg.flush_all();
                prop_assert_eq!(candles.len(), 1);
                let c = &candles[0];
                prop_assert_eq!(c.ts_open, base);
                prop_assert_eq!(Some(c.open), trades.first().map(|t| t.price));
                prop_assert_eq!(Some(c.close), trades.last().map(|t| t.price));
                prop_assert_eq!(Some(c.high), trades.iter().map(|t| t.price).max());
                prop_assert_eq!(Some(c.low), trades.iter().map(|t| t.price).min());
                prop_assert_eq!(c.volume, trades.iter().map(|t| t.amount).sum::<Decimal>());
                prop_assert_eq!(usize::try_from(c.trade_count).ok(), Some(trades.len()));
            }
        }
    }
}
