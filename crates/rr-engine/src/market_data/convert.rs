//! Pure conversion from barter market events to archive [`TradeRecord`]s.
//!
//! This is the only place `f64` market data crosses into the engine: prices
//! and amounts become [`Decimal`] here or the trade is rejected — non-finite
//! and non-positive values are errors, never silently archived.

use barter_data::event::MarketEvent;
use barter_data::subscription::trade::PublicTrade;
use barter_instrument::Side as BarterSide;
use barter_instrument::instrument::market_data::MarketDataInstrument;
use rr_storage::records::{Side, TradeRecord};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

use crate::market_data::spec::pair_string;

/// Why a barter trade event could not become a [`TradeRecord`].
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// The field is NaN or infinite and has no decimal representation.
    #[error("trade field `{field}` is not a finite number: {value}")]
    NonFinite {
        /// Offending trade field (`price` / `amount`).
        field: &'static str,
        /// The raw value as received.
        value: f64,
    },
    /// The field converted but is zero or negative — meaningless for a trade.
    #[error("trade field `{field}` must be positive, got {value}")]
    NonPositive {
        /// Offending trade field (`price` / `amount`).
        field: &'static str,
        /// The converted value.
        value: Decimal,
    },
}

/// Converts one barter public-trade event into an archive [`TradeRecord`].
///
/// # Errors
///
/// Returns [`ConvertError::NonFinite`] if price or amount is NaN/infinite,
/// or [`ConvertError::NonPositive`] if either is zero or negative.
pub fn to_trade_record(
    event: &MarketEvent<MarketDataInstrument, PublicTrade>,
) -> Result<TradeRecord, ConvertError> {
    let price = positive_decimal("price", event.kind.price)?;
    let amount = positive_decimal("amount", event.kind.amount)?;
    Ok(TradeRecord {
        exchange: event.exchange.as_str().to_owned(),
        pair: pair_string(
            event.instrument.base.name().as_str(),
            event.instrument.quote.name().as_str(),
        ),
        ts_exchange: event.time_exchange,
        ts_received: event.time_received,
        price,
        amount,
        side: match event.kind.side {
            BarterSide::Buy => Side::Buy,
            BarterSide::Sell => Side::Sell,
        },
        trade_id: event.kind.id.clone(),
    })
}

/// `f64` → positive [`Decimal`], rejecting non-finite and non-positive values.
fn positive_decimal(field: &'static str, value: f64) -> Result<Decimal, ConvertError> {
    let decimal = Decimal::from_f64(value).ok_or(ConvertError::NonFinite { field, value })?;
    if decimal <= Decimal::ZERO {
        return Err(ConvertError::NonPositive {
            field,
            value: decimal,
        });
    }
    Ok(decimal)
}

#[cfg(test)]
mod tests {
    use barter_data::event::MarketEvent;
    use barter_data::subscription::trade::PublicTrade;
    use barter_instrument::Side as BarterSide;
    use barter_instrument::exchange::ExchangeId;
    use barter_instrument::instrument::market_data::MarketDataInstrument;
    use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
    use chrono::{DateTime, Utc};
    use rr_storage::records::Side;
    use rust_decimal::Decimal;

    use crate::market_data::convert::{ConvertError, to_trade_record};

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn event(price: f64, amount: f64) -> MarketEvent<MarketDataInstrument, PublicTrade> {
        MarketEvent {
            time_exchange: ts("2026-06-12T10:00:00.123Z"),
            time_received: ts("2026-06-12T10:00:00.456Z"),
            exchange: ExchangeId::BinanceSpot,
            instrument: MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            kind: PublicTrade {
                id: "987654".to_owned(),
                price,
                amount,
                side: BarterSide::Sell,
            },
        }
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn normal_trade_converts_every_field() -> Result<(), ConvertError> {
        let record = to_trade_record(&event(104_500.25, 0.015))?;
        assert_eq!(record.exchange, "binance_spot");
        assert_eq!(record.pair, "BTC-USDT");
        assert_eq!(record.ts_exchange, ts("2026-06-12T10:00:00.123Z"));
        assert_eq!(record.ts_received, ts("2026-06-12T10:00:00.456Z"));
        assert_eq!(record.price, dec("104500.25"));
        assert_eq!(record.amount, dec("0.015"));
        assert_eq!(record.side, Side::Sell);
        assert_eq!(record.trade_id, "987654");
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn coinbase_exchange_id_converts_to_snake_case_str() -> Result<(), ConvertError> {
        let mut ev = event(100.0, 1.0);
        ev.exchange = ExchangeId::Coinbase;
        ev.instrument = MarketDataInstrument::from(("eth", "usd", MarketDataInstrumentKind::Spot));
        let record = to_trade_record(&ev)?;
        assert_eq!(record.exchange, "coinbase");
        assert_eq!(record.pair, "ETH-USD");
        assert_eq!(record.side, Side::Sell);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn coinbase_minimum_amount_converts_exactly() -> Result<(), ConvertError> {
        // 1e-8 is the live-verified Coinbase minimum trade size; this pins
        // Decimal::from_f64 producing the exact decimal, not a float smear.
        let record = to_trade_record(&event(100.0, 1e-8))?;
        assert_eq!(record.amount, dec("0.00000001"));
        Ok(())
    }

    #[test]
    #[expect(clippy::panic, reason = "test assertion on unexpected variant")]
    fn nan_and_infinite_price_are_non_finite_errors() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            match to_trade_record(&event(bad, 1.0)) {
                Err(ConvertError::NonFinite { field, value }) => {
                    assert_eq!(field, "price");
                    assert!(value.is_nan() || value.is_infinite());
                }
                other => panic!("expected NonFinite for {bad}, got {other:?}"),
            }
        }
    }

    #[test]
    #[expect(clippy::panic, reason = "test assertion on unexpected variant")]
    fn zero_amount_is_a_non_positive_error() {
        match to_trade_record(&event(100.0, 0.0)) {
            Err(ConvertError::NonPositive { field, value }) => {
                assert_eq!(field, "amount");
                assert_eq!(value, Decimal::ZERO);
            }
            other => panic!("expected NonPositive, got {other:?}"),
        }
    }

    #[test]
    #[expect(clippy::panic, reason = "test assertion on unexpected variant")]
    fn negative_price_is_a_non_positive_error() {
        match to_trade_record(&event(-0.5, 1.0)) {
            Err(ConvertError::NonPositive { field, value }) => {
                assert_eq!(field, "price");
                assert!(value < Decimal::ZERO);
            }
            other => panic!("expected NonPositive, got {other:?}"),
        }
    }
}
