//! Arrow schemas and `RecordBatch` encoding for the archive datasets.
//!
//! Decimals are stored as `Decimal128(38, 18)`; conversion errors — never
//! rounds — on values with more than 18 fractional digits. Timestamps are
//! stored as UTC milliseconds: sub-millisecond precision from exchanges
//! (Coinbase sends microseconds) is truncated on write by design, so
//! read-back equality holds at millisecond precision.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

use crate::error::StorageError;
use crate::records::{CandleRecord, TradeRecord};

/// Precision of every decimal column in the archive.
pub const DECIMAL_PRECISION: u8 = 38;
/// Scale of every decimal column in the archive.
pub const DECIMAL_SCALE: i8 = 18;

/// Largest magnitude representable in `Decimal128(38, 18)`: 38 nines.
const MAX_ABS_DECIMAL128: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// Converts a decimal to the `Decimal128(38, 18)` wire integer.
///
/// # Errors
///
/// - [`StorageError::DecimalPrecision`] if `value` has more than 18
///   fractional digits after normalization — the archive never rounds.
/// - [`StorageError::DecimalOverflow`] if the scaled magnitude exceeds
///   38 digits.
pub fn decimal_to_i128_scale18(value: Decimal) -> Result<i128, StorageError> {
    const SCALE: u32 = 18;
    let normalized = value.normalize();
    let scale = normalized.scale();
    if scale > SCALE {
        return Err(StorageError::DecimalPrecision { value, scale });
    }
    let scaled = normalized
        .mantissa()
        .checked_mul(10_i128.pow(SCALE - scale))
        .ok_or(StorageError::DecimalOverflow { value })?;
    if scaled.abs() > MAX_ABS_DECIMAL128 {
        return Err(StorageError::DecimalOverflow { value });
    }
    Ok(scaled)
}

/// One archive dataset: its name, arrow schema, row encoding, and the
/// partition/timestamp accessors the partitioned writer needs.
pub trait Dataset {
    /// Record type this dataset persists.
    type Record;
    /// Dataset directory name in the archive layout (`trades` / `candles`).
    const NAME: &'static str;

    /// Arrow schema of one part file.
    fn schema() -> SchemaRef;
    /// Encodes records into one `RecordBatch` (one row per record).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if a decimal cannot be represented in
    /// `Decimal128(38, 18)` or batch construction fails.
    fn encode(records: &[Self::Record]) -> Result<RecordBatch, StorageError>;
    /// Exchange partition component.
    fn exchange(record: &Self::Record) -> &str;
    /// Pair partition component.
    fn pair(record: &Self::Record) -> &str;
    /// Date partition component (exchange-time based).
    fn partition_date(record: &Self::Record) -> NaiveDate;
    /// Timestamp used for `ts_min`/`ts_max` file metadata.
    fn timestamp(record: &Self::Record) -> DateTime<Utc>;
}

/// Public-trade archive dataset.
pub struct TradesDataset;

/// One-minute-candle archive dataset.
pub struct CandlesDataset;

impl Dataset for TradesDataset {
    type Record = TradeRecord;
    const NAME: &'static str = "trades";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            timestamp_field("ts_exchange"),
            timestamp_field("ts_received"),
            decimal_field("price"),
            decimal_field("amount"),
            Field::new("side", DataType::Utf8, false),
            Field::new("trade_id", DataType::Utf8, false),
        ]))
    }

    fn encode(records: &[TradeRecord]) -> Result<RecordBatch, StorageError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp_array(records.iter().map(|r| r.ts_exchange))),
            Arc::new(timestamp_array(records.iter().map(|r| r.ts_received))),
            Arc::new(decimal_array(records.iter().map(|r| r.price))?),
            Arc::new(decimal_array(records.iter().map(|r| r.amount))?),
            Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.side.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                records.iter().map(|r| r.trade_id.as_str()),
            )),
        ];
        Ok(RecordBatch::try_new(Self::schema(), columns)?)
    }

    fn exchange(record: &TradeRecord) -> &str {
        &record.exchange
    }

    fn pair(record: &TradeRecord) -> &str {
        &record.pair
    }

    fn partition_date(record: &TradeRecord) -> NaiveDate {
        record.partition_date()
    }

    fn timestamp(record: &TradeRecord) -> DateTime<Utc> {
        record.ts_exchange
    }
}

impl Dataset for CandlesDataset {
    type Record = CandleRecord;
    const NAME: &'static str = "candles";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            timestamp_field("ts_open"),
            decimal_field("open"),
            decimal_field("high"),
            decimal_field("low"),
            decimal_field("close"),
            decimal_field("volume"),
            Field::new("trade_count", DataType::UInt64, false),
        ]))
    }

    fn encode(records: &[CandleRecord]) -> Result<RecordBatch, StorageError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp_array(records.iter().map(|r| r.ts_open))),
            Arc::new(decimal_array(records.iter().map(|r| r.open))?),
            Arc::new(decimal_array(records.iter().map(|r| r.high))?),
            Arc::new(decimal_array(records.iter().map(|r| r.low))?),
            Arc::new(decimal_array(records.iter().map(|r| r.close))?),
            Arc::new(decimal_array(records.iter().map(|r| r.volume))?),
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|r| r.trade_count),
            )),
        ];
        Ok(RecordBatch::try_new(Self::schema(), columns)?)
    }

    fn exchange(record: &CandleRecord) -> &str {
        &record.exchange
    }

    fn pair(record: &CandleRecord) -> &str {
        &record.pair
    }

    fn partition_date(record: &CandleRecord) -> NaiveDate {
        record.partition_date()
    }

    fn timestamp(record: &CandleRecord) -> DateTime<Utc> {
        record.ts_open
    }
}

fn timestamp_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )
}

fn decimal_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        false,
    )
}

fn timestamp_array(values: impl Iterator<Item = DateTime<Utc>>) -> TimestampMillisecondArray {
    TimestampMillisecondArray::from_iter_values(values.map(|ts| ts.timestamp_millis()))
        .with_timezone("UTC")
}

fn decimal_array(values: impl Iterator<Item = Decimal>) -> Result<Decimal128Array, StorageError> {
    let mut scaled = Vec::with_capacity(values.size_hint().0);
    for value in values {
        scaled.push(decimal_to_i128_scale18(value)?);
    }
    Ok(Decimal128Array::from_iter_values(scaled)
        .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use arrow::array::{Decimal128Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;

    use crate::error::StorageError;
    use crate::parquet::encode::{
        CandlesDataset, DECIMAL_PRECISION, DECIMAL_SCALE, Dataset, TradesDataset,
        decimal_to_i128_scale18,
    };
    use crate::records::{CandleRecord, Side, TradeRecord};

    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_exact_values_convert_to_scale_18() -> TestResult {
        assert_eq!(
            decimal_to_i128_scale18("104500.25".parse::<Decimal>()?)?,
            104_500_250_000_000_000_000_000
        );
        assert_eq!(
            decimal_to_i128_scale18(Decimal::ONE)?,
            1_000_000_000_000_000_000
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_coinbase_minimum_converts_exactly() -> TestResult {
        // 0.00000001 BTC — the minimum size observed live on Coinbase.
        assert_eq!(
            decimal_to_i128_scale18("0.00000001".parse::<Decimal>()?)?,
            10_000_000_000
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_negative_converts() -> TestResult {
        assert_eq!(
            decimal_to_i128_scale18("-1.5".parse::<Decimal>()?)?,
            -1_500_000_000_000_000_000
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_eighteen_places_is_exact_and_trailing_zeros_normalize() -> TestResult {
        assert_eq!(
            decimal_to_i128_scale18("0.000000000000000001".parse::<Decimal>()?)?,
            1
        );
        // 19 fractional digits, but the last is a zero — normalizes to scale ≤ 18.
        assert_eq!(
            decimal_to_i128_scale18("1.0000000000000000000".parse::<Decimal>()?)?,
            1_000_000_000_000_000_000
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_more_than_eighteen_places_errors_never_rounds() -> TestResult {
        let value = "0.0000000000000000001".parse::<Decimal>()?;
        match decimal_to_i128_scale18(value) {
            Err(StorageError::DecimalPrecision {
                value: errored,
                scale,
            }) => {
                assert_eq!(errored, value);
                assert_eq!(scale, 19);
            }
            other => panic!("expected DecimalPrecision, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_too_large_for_decimal128_errors() -> TestResult {
        // 2e20 × 10^18 overflows i128; 1.6e20 × 10^18 fits i128 but exceeds
        // the 38-digit precision of Decimal128(38, 18). Both must error.
        for raw in ["200000000000000000000", "160000000000000000000"] {
            let value = raw.parse::<Decimal>()?;
            match decimal_to_i128_scale18(value) {
                Err(StorageError::DecimalOverflow { value: errored }) => {
                    assert_eq!(errored, value);
                }
                other => panic!("expected DecimalOverflow for {raw}, got {other:?}"),
            }
        }
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn decimal_round_trips_through_i128_scale18() -> TestResult {
        for raw in [
            "104500.25",
            "0.00000001",
            "-1.5",
            "0",
            "73.000000000000000009",
        ] {
            let value = raw.parse::<Decimal>()?;
            let read_back = Decimal::try_from_i128_with_scale(decimal_to_i128_scale18(value)?, 18)?;
            assert_eq!(read_back, value, "round trip of {raw}");
        }
        Ok(())
    }

    #[test]
    fn trades_schema_matches_design() {
        let schema = TradesDataset::schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "ts_exchange",
                "ts_received",
                "price",
                "amount",
                "side",
                "trade_id"
            ]
        );
        let ts_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        assert_eq!(schema.field(0).data_type(), &ts_type);
        assert_eq!(schema.field(1).data_type(), &ts_type);
        let decimal_type = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);
        assert_eq!(schema.field(2).data_type(), &decimal_type);
        assert_eq!(schema.field(3).data_type(), &decimal_type);
        assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
        assert!(schema.fields().iter().all(|f| !f.is_nullable()));
        assert_eq!(TradesDataset::NAME, "trades");
    }

    #[test]
    fn candles_schema_matches_design() {
        let schema = CandlesDataset::schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "ts_open",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trade_count"
            ]
        );
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        let decimal_type = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);
        for index in 1..=5 {
            assert_eq!(schema.field(index).data_type(), &decimal_type);
        }
        assert_eq!(schema.field(6).data_type(), &DataType::UInt64);
        assert!(schema.fields().iter().all(|f| !f.is_nullable()));
        assert_eq!(CandlesDataset::NAME, "candles");
    }

    fn trade(
        ts_exchange: &str,
        price: &str,
        amount: &str,
        side: Side,
        id: &str,
    ) -> Result<TradeRecord, Box<dyn Error>> {
        Ok(TradeRecord {
            exchange: "binance_spot".to_owned(),
            pair: "BTC-USDT".to_owned(),
            ts_exchange: ts_exchange.parse::<DateTime<Utc>>()?,
            ts_received: "2026-06-12T10:00:01Z".parse::<DateTime<Utc>>()?,
            price: price.parse::<Decimal>()?,
            amount: amount.parse::<Decimal>()?,
            side,
            trade_id: id.to_owned(),
        })
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn trades_encode_to_record_batch_columns() -> TestResult {
        let trades = vec![
            // Sub-millisecond input: stored at millisecond precision by design.
            trade(
                "2026-06-12T10:00:00.123456Z",
                "104500.25",
                "0.015",
                Side::Buy,
                "1001",
            )?,
            trade(
                "2026-06-12T10:00:02Z",
                "104499.00",
                "0.00000001",
                Side::Sell,
                "1002",
            )?,
        ];
        let batch = TradesDataset::encode(&trades)?;
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);

        let ts_exchange = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or("ts_exchange column type")?;
        let expected_ms = "2026-06-12T10:00:00.123Z".parse::<DateTime<Utc>>()?;
        assert_eq!(ts_exchange.value(0), expected_ms.timestamp_millis());

        let price = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or("price column type")?;
        assert_eq!(price.value(0), 104_500_250_000_000_000_000_000);

        let amount = batch
            .column(3)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or("amount column type")?;
        assert_eq!(amount.value(1), 10_000_000_000);

        let side = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("side column type")?;
        assert_eq!(side.value(0), "buy");
        assert_eq!(side.value(1), "sell");

        let trade_id = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("trade_id column type")?;
        assert_eq!(trade_id.value(0), "1001");
        assert_eq!(trade_id.value(1), "1002");
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn candles_encode_to_record_batch_columns() -> TestResult {
        let candle = CandleRecord {
            exchange: "coinbase".to_owned(),
            pair: "ETH-USD".to_owned(),
            ts_open: "2026-06-12T10:00:00Z".parse::<DateTime<Utc>>()?,
            open: "2500.5".parse::<Decimal>()?,
            high: "2501".parse::<Decimal>()?,
            low: "2499.75".parse::<Decimal>()?,
            close: "2500".parse::<Decimal>()?,
            volume: "12.345".parse::<Decimal>()?,
            trade_count: 42,
        };
        let batch = CandlesDataset::encode(std::slice::from_ref(&candle))?;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);

        let ts_open = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or("ts_open column type")?;
        assert_eq!(ts_open.value(0), candle.ts_open.timestamp_millis());

        let close = batch
            .column(4)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or("close column type")?;
        assert_eq!(close.value(0), 2_500_000_000_000_000_000_000);

        let trade_count = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("trade_count column type")?;
        assert_eq!(trade_count.value(0), 42);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn dataset_partition_accessors_delegate_to_records() -> TestResult {
        let record = trade("2026-06-12T23:59:59.500Z", "1", "1", Side::Buy, "1")?;
        assert_eq!(TradesDataset::exchange(&record), "binance_spot");
        assert_eq!(TradesDataset::pair(&record), "BTC-USDT");
        assert_eq!(
            TradesDataset::partition_date(&record),
            record.partition_date()
        );
        assert_eq!(TradesDataset::timestamp(&record), record.ts_exchange);
        Ok(())
    }
}
