//! Canonical persisted record types. `rr-storage` owns what gets written;
//! `rr-engine` produces these at its ingest boundary.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

/// Taker side of a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// Taker bought (aggressive buy).
    Buy,
    /// Taker sold (aggressive sell).
    Sell,
}

/// A single public trade as persisted to the market-data archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRecord {
    /// Exchange identifier, `snake_case` (e.g. `binance_spot`).
    pub exchange: String,
    /// Trading pair, uppercase dash-separated (e.g. `BTC-USDT`).
    pub pair: String,
    /// Trade timestamp as reported by the exchange, UTC.
    pub ts_exchange: DateTime<Utc>,
    /// Timestamp when we received the event, UTC.
    pub ts_received: DateTime<Utc>,
    /// Trade price in quote currency.
    pub price: Decimal,
    /// Trade quantity in base currency.
    pub amount: Decimal,
    /// Taker side.
    pub side: Side,
    /// Exchange-assigned trade identifier.
    pub trade_id: String,
}

/// A one-minute OHLCV candle as persisted to the market-data archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandleRecord {
    /// Exchange identifier, `snake_case` (e.g. `binance_spot`).
    pub exchange: String,
    /// Trading pair, uppercase dash-separated (e.g. `BTC-USDT`).
    pub pair: String,
    /// Window start, UTC, aligned to the minute.
    pub ts_open: DateTime<Utc>,
    /// First trade price in the window.
    pub open: Decimal,
    /// Highest trade price in the window.
    pub high: Decimal,
    /// Lowest trade price in the window.
    pub low: Decimal,
    /// Last trade price in the window.
    pub close: Decimal,
    /// Total base-currency volume in the window.
    pub volume: Decimal,
    /// Number of trades aggregated into the window.
    pub trade_count: u64,
}

impl TradeRecord {
    /// Partition date for the `date=YYYY-MM-DD` archive layout,
    /// derived from exchange time (authoritative for partitioning).
    #[must_use]
    pub fn partition_date(&self) -> NaiveDate {
        self.ts_exchange.date_naive()
    }
}

impl CandleRecord {
    /// Partition date for the `date=YYYY-MM-DD` archive layout,
    /// derived from the candle's window start.
    #[must_use]
    pub fn partition_date(&self) -> NaiveDate {
        self.ts_open.date_naive()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;

    use crate::records::{CandleRecord, Side, TradeRecord};

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?` parsing"
    )]
    fn trade_record_partition_date_is_utc_exchange_time() -> Result<(), chrono::ParseError> {
        let trade = TradeRecord {
            exchange: "binance_spot".to_owned(),
            pair: "BTC-USDT".to_owned(),
            ts_exchange: "2026-06-12T23:59:59Z".parse::<DateTime<Utc>>()?,
            ts_received: "2026-06-13T00:00:01Z".parse::<DateTime<Utc>>()?,
            price: Decimal::new(10_450_025, 2),
            amount: Decimal::new(15, 3),
            side: Side::Buy,
            trade_id: "12345".to_owned(),
        };
        assert_eq!(trade.partition_date().to_string(), "2026-06-12");
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?` parsing"
    )]
    fn candle_record_partition_date_is_utc_open_time() -> Result<(), chrono::ParseError> {
        let candle = CandleRecord {
            exchange: "coinbase".to_owned(),
            pair: "BTC-USDT".to_owned(),
            ts_open: "2026-06-12T23:59:00Z".parse::<DateTime<Utc>>()?,
            open: Decimal::new(104_500, 0),
            high: Decimal::new(104_600, 0),
            low: Decimal::new(104_400, 0),
            close: Decimal::new(104_550, 0),
            volume: Decimal::new(125, 1),
            trade_count: 42,
        };
        assert_eq!(candle.partition_date().to_string(), "2026-06-12");
        Ok(())
    }
}
