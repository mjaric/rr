//! Partitioned Parquet market-data archive.
//!
//! [`PartitionedWriter`] keeps one open part file per (exchange, pair, date)
//! partition under
//! `<data_dir>/parquet/<dataset>/exchange=<e>/pair=<p>/date=<YYYY-MM-DD>/part-NNNN.parquet`.
//! Files roll — footer closed, part number incremented — when an open file's
//! age exceeds the roll interval or a record arrives for the same
//! (exchange, pair) on a different date, so a crash loses at most one
//! unclosed file. On (re)opening a partition directory, footer-less leftovers
//! are quarantined as `*.corrupt` and part numbering continues past every
//! existing part, quarantined or not.
//!
//! The writer is synchronous (`std::fs`; it runs on a dedicated thread) and
//! holds no clock: callers supply `now` to [`PartitionedWriter::append`] and
//! [`PartitionedWriter::roll_due`]. Rows are buffered and flushed to the open
//! file as a row group every [`FLUSH_THRESHOLD`] appends and on roll/finalize.

pub mod encode;
pub mod partition;

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ::parquet::arrow::ArrowWriter;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};

use crate::error::StorageError;
use crate::parquet::encode::Dataset;

/// Buffered rows flushed to the open file as one row group.
pub const FLUSH_THRESHOLD: usize = 1000;

/// Parquet footer magic bytes.
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Metadata of one closed part file, recorded in `archive_files` by the
/// caller. `ts_min`/`ts_max` are over the dataset timestamp (`ts_exchange`
/// for trades, `ts_open` for candles) at millisecond precision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedFile {
    /// Dataset directory name (`trades` / `candles`).
    pub dataset: &'static str,
    /// Exchange partition component.
    pub exchange: String,
    /// Pair partition component.
    pub pair: String,
    /// Date partition component.
    pub date: NaiveDate,
    /// Absolute path of the closed part file.
    pub path: PathBuf,
    /// Number of rows written.
    pub rows: u64,
    /// Smallest dataset timestamp in the file.
    pub ts_min: DateTime<Utc>,
    /// Largest dataset timestamp in the file.
    pub ts_max: DateTime<Utc>,
}

type PartitionKey = (String, String, NaiveDate);

/// Partition-aware Parquet writer for one dataset; see the module docs.
pub struct PartitionedWriter<D: Dataset> {
    data_dir: PathBuf,
    roll_interval: TimeDelta,
    open: BTreeMap<PartitionKey, OpenFile<D>>,
}

impl<D: Dataset> PartitionedWriter<D> {
    /// Creates a writer rooted at `data_dir`; open files roll once older
    /// than `roll_interval` (15 minutes in production).
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>, roll_interval: TimeDelta) -> Self {
        Self {
            data_dir: data_dir.into(),
            roll_interval,
            open: BTreeMap::new(),
        }
    }

    /// Appends one record, first rolling every file that is due at `now`
    /// (aged out, or same (exchange, pair) but a different date than the
    /// record). Returns the files finalized by those rolls.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] on filesystem failures, encoding failures,
    /// or decimals that do not fit `Decimal128(38, 18)`.
    pub fn append(
        &mut self,
        record: D::Record,
        now: DateTime<Utc>,
    ) -> Result<Vec<FinalizedFile>, StorageError> {
        let mut finalized = self.roll_due(now)?;
        let key: PartitionKey = (
            D::exchange(&record).to_owned(),
            D::pair(&record).to_owned(),
            D::partition_date(&record),
        );
        let other_dates: Vec<PartitionKey> = self
            .open
            .keys()
            .filter(|(exchange, pair, date)| *exchange == key.0 && *pair == key.1 && *date != key.2)
            .cloned()
            .collect();
        finalized.extend(self.finalize_keys(other_dates)?);

        let open = match self.open.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let file = OpenFile::create(&self.data_dir, entry.key(), now)?;
                entry.insert(file)
            }
        };
        open.push(record)?;
        Ok(finalized)
    }

    /// Finalizes every open file older than the roll interval at `now` and
    /// returns them. Call this on a timer tick so quiet partitions still
    /// roll without waiting for their next record.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if flushing or closing a file fails.
    pub fn roll_due(&mut self, now: DateTime<Utc>) -> Result<Vec<FinalizedFile>, StorageError> {
        let due: Vec<PartitionKey> = self
            .open
            .iter()
            .filter(|(_, file)| now - file.opened_at > self.roll_interval)
            .map(|(key, _)| key.clone())
            .collect();
        self.finalize_keys(due)
    }

    /// Finalizes every open file (graceful shutdown). With nothing appended
    /// since the last roll this writes nothing and returns an empty list.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if flushing or closing a file fails.
    pub fn finalize_all(&mut self) -> Result<Vec<FinalizedFile>, StorageError> {
        let open = std::mem::take(&mut self.open);
        let mut finalized = Vec::with_capacity(open.len());
        for (key, file) in open {
            finalized.push(file.finalize(&key)?);
        }
        Ok(finalized)
    }

    fn finalize_keys(
        &mut self,
        keys: Vec<PartitionKey>,
    ) -> Result<Vec<FinalizedFile>, StorageError> {
        let mut finalized = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(file) = self.open.remove(&key) else {
                continue;
            };
            finalized.push(file.finalize(&key)?);
        }
        Ok(finalized)
    }
}

/// One open part file: parquet writer plus the row buffer and metadata
/// accumulated for its eventual [`FinalizedFile`].
struct OpenFile<D: Dataset> {
    path: PathBuf,
    writer: ArrowWriter<File>,
    opened_at: DateTime<Utc>,
    buffer: Vec<D::Record>,
    rows: u64,
    ts_min: DateTime<Utc>,
    ts_max: DateTime<Utc>,
}

impl<D: Dataset> OpenFile<D> {
    /// Opens the next part file of `key`'s partition directory, quarantining
    /// footer-less leftovers first. `ts_min`/`ts_max` start as MAX/MIN
    /// sentinels and are set by the first `push` — a file is only ever
    /// created when a record is about to be appended, so they never leak
    /// into a [`FinalizedFile`].
    fn create(
        data_dir: &Path,
        key: &PartitionKey,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        let (exchange, pair, date) = key;
        let dir = partition::partition_dir(data_dir, D::NAME, exchange, pair, *date);
        std::fs::create_dir_all(&dir).map_err(|source| io_error(&dir, source))?;
        quarantine_footerless(&dir)?;
        let part = partition::next_part_number(&dir)?;
        let path = dir.join(partition::part_file_name(part));
        let file = File::create(&path).map_err(|source| io_error(&path, source))?;
        let writer = ArrowWriter::try_new(file, D::schema(), None)?;
        Ok(OpenFile {
            path,
            writer,
            opened_at: now,
            buffer: Vec::new(),
            rows: 0,
            ts_min: DateTime::<Utc>::MAX_UTC,
            ts_max: DateTime::<Utc>::MIN_UTC,
        })
    }

    fn push(&mut self, record: D::Record) -> Result<(), StorageError> {
        // ts_min/ts_max must describe the file's contents, which hold
        // millisecond precision — not the in-memory record.
        let ts = truncate_to_ms(D::timestamp(&record));
        self.ts_min = self.ts_min.min(ts);
        self.ts_max = self.ts_max.max(ts);
        self.rows += 1;
        self.buffer.push(record);
        if self.buffer.len() >= FLUSH_THRESHOLD {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = D::encode(&self.buffer)?;
        self.writer.write(&batch)?;
        self.writer.flush()?;
        self.buffer.clear();
        Ok(())
    }

    fn finalize(mut self, key: &PartitionKey) -> Result<FinalizedFile, StorageError> {
        self.flush()?;
        self.writer.close()?;
        Ok(FinalizedFile {
            dataset: D::NAME,
            exchange: key.0.clone(),
            pair: key.1.clone(),
            date: key.2,
            path: self.path,
            rows: self.rows,
            ts_min: self.ts_min,
            ts_max: self.ts_max,
        })
    }
}

/// Renames every `*.parquet` file in `dir` that lacks the Parquet footer
/// magic to `*.parquet.corrupt` (crash leftovers), warning via `tracing`.
fn quarantine_footerless(dir: &Path) -> Result<(), StorageError> {
    let entries = std::fs::read_dir(dir).map_err(|source| io_error(dir, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| io_error(dir, source))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.ends_with(".parquet") {
            continue;
        }
        let path = entry.path();
        if has_parquet_footer(&path)? {
            continue;
        }
        let mut quarantined = path.clone().into_os_string();
        quarantined.push(".corrupt");
        let quarantined = PathBuf::from(quarantined);
        tracing::warn!(
            file = %path.display(),
            quarantined = %quarantined.display(),
            "parquet file has no footer (crash leftover); quarantining"
        );
        std::fs::rename(&path, &quarantined).map_err(|source| io_error(&path, source))?;
    }
    Ok(())
}

/// A structurally complete parquet file ends with the 4-byte footer magic.
fn has_parquet_footer(path: &Path) -> Result<bool, StorageError> {
    let mut file = File::open(path).map_err(|source| io_error(path, source))?;
    let len = file
        .metadata()
        .map_err(|source| io_error(path, source))?
        .len();
    if len < 8 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-4))
        .map_err(|source| io_error(path, source))?;
    let mut magic = [0_u8; 4];
    file.read_exact(&mut magic)
        .map_err(|source| io_error(path, source))?;
    Ok(&magic == PARQUET_MAGIC)
}

/// Truncates to the archive's millisecond precision. A `DateTime` rebuilt
/// from its own `timestamp_millis` is always in range, so the identity
/// fallback never fires.
fn truncate_to_ms(ts: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ts.timestamp_millis()).unwrap_or(ts)
}

fn io_error(path: &Path, source: std::io::Error) -> StorageError {
    StorageError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs::File;

    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow::array::{Decimal128Array, StringArray, TimestampMillisecondArray};
    use chrono::{DateTime, TimeDelta, Utc};
    use rust_decimal::Decimal;

    use crate::parquet::encode::TradesDataset;
    use crate::parquet::{FinalizedFile, PartitionedWriter, partition};
    use crate::records::{Side, TradeRecord};

    type TestResult = Result<(), Box<dyn Error>>;

    const ROLL: TimeDelta = TimeDelta::minutes(15);

    fn trade(pair: &str, ts_exchange: &str, id: &str) -> Result<TradeRecord, Box<dyn Error>> {
        Ok(TradeRecord {
            exchange: "binance_spot".to_owned(),
            pair: pair.to_owned(),
            ts_exchange: ts_exchange.parse::<DateTime<Utc>>()?,
            ts_received: "2026-06-11T10:00:01Z".parse::<DateTime<Utc>>()?,
            price: "104500.25".parse::<Decimal>()?,
            amount: "0.00000001".parse::<Decimal>()?,
            side: Side::Buy,
            trade_id: id.to_owned(),
        })
    }

    /// The trade as it must read back: timestamps truncated to milliseconds
    /// (the archive stores millisecond precision by design).
    fn at_ms_precision(trade: &TradeRecord) -> Result<TradeRecord, Box<dyn Error>> {
        let truncate = |ts: DateTime<Utc>| {
            DateTime::from_timestamp_millis(ts.timestamp_millis())
                .ok_or("timestamp out of millisecond range")
        };
        Ok(TradeRecord {
            ts_exchange: truncate(trade.ts_exchange)?,
            ts_received: truncate(trade.ts_received)?,
            ..trade.clone()
        })
    }

    fn read_trades(file: &FinalizedFile) -> Result<Vec<TradeRecord>, Box<dyn Error>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path)?)?.build()?;
        let mut trades = Vec::new();
        for batch in reader {
            let batch = batch?;
            let ts_exchange = column::<TimestampMillisecondArray>(&batch, 0, "ts_exchange")?;
            let ts_received = column::<TimestampMillisecondArray>(&batch, 1, "ts_received")?;
            let price = column::<Decimal128Array>(&batch, 2, "price")?;
            let amount = column::<Decimal128Array>(&batch, 3, "amount")?;
            let side = column::<StringArray>(&batch, 4, "side")?;
            let trade_id = column::<StringArray>(&batch, 5, "trade_id")?;
            for row in 0..batch.num_rows() {
                trades.push(TradeRecord {
                    exchange: file.exchange.clone(),
                    pair: file.pair.clone(),
                    ts_exchange: DateTime::from_timestamp_millis(ts_exchange.value(row))
                        .ok_or("ts_exchange out of range")?,
                    ts_received: DateTime::from_timestamp_millis(ts_received.value(row))
                        .ok_or("ts_received out of range")?,
                    price: Decimal::try_from_i128_with_scale(price.value(row), 18)?,
                    amount: Decimal::try_from_i128_with_scale(amount.value(row), 18)?,
                    side: Side::parse(side.value(row)).ok_or("unknown side encoding")?,
                    trade_id: trade_id.value(row).to_owned(),
                });
            }
        }
        Ok(trades)
    }

    fn column<'a, A: 'static>(
        batch: &'a arrow::array::RecordBatch,
        index: usize,
        name: &str,
    ) -> Result<&'a A, Box<dyn Error>> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| format!("column {name} has unexpected type").into())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn round_trip_across_pairs_and_date_boundary() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let now = "2026-06-11T10:00:00Z".parse::<DateTime<Utc>>()?;
        let mut writer = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);

        // Sub-millisecond timestamp: must read back truncated to milliseconds.
        let btc_first = trade("BTC-USDT", "2026-06-11T09:59:58.123456Z", "1")?;
        let btc_second = trade("BTC-USDT", "2026-06-11T09:59:59Z", "2")?;
        let eth = trade("ETH-USDT", "2026-06-11T09:59:59.500Z", "10")?;
        let btc_next_day = trade("BTC-USDT", "2026-06-12T00:00:00.250Z", "3")?;

        assert!(writer.append(btc_first.clone(), now)?.is_empty());
        assert!(writer.append(btc_second.clone(), now)?.is_empty());
        assert!(writer.append(eth.clone(), now)?.is_empty());

        // Crossing the BTC date boundary finalizes the 06-11 BTC file.
        let rolled = writer.append(btc_next_day.clone(), now)?;
        assert_eq!(rolled.len(), 1);
        assert_eq!(rolled[0].date.to_string(), "2026-06-11");
        assert_eq!(rolled[0].pair, "BTC-USDT");

        let mut finalized = rolled;
        finalized.extend(writer.finalize_all()?);
        finalized.sort_by(|a, b| (&a.pair, a.date).cmp(&(&b.pair, b.date)));
        assert_eq!(finalized.len(), 3);

        let expected: [(&str, &str, &[&TradeRecord]); 3] = [
            ("BTC-USDT", "2026-06-11", &[&btc_first, &btc_second]),
            ("BTC-USDT", "2026-06-12", &[&btc_next_day]),
            ("ETH-USDT", "2026-06-11", &[&eth]),
        ];
        for (file, (pair, date, records)) in finalized.iter().zip(expected) {
            assert_eq!(file.dataset, "trades");
            assert_eq!(file.exchange, "binance_spot");
            assert_eq!(file.pair, pair);
            assert_eq!(file.date.to_string(), date);
            assert_eq!(file.rows, records.len() as u64);

            let expected_records = records
                .iter()
                .map(|r| at_ms_precision(r))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(read_trades(file)?, expected_records, "{pair} {date}");

            let ts_values: Vec<DateTime<Utc>> =
                expected_records.iter().map(|r| r.ts_exchange).collect();
            assert_eq!(Some(file.ts_min), ts_values.iter().min().copied());
            assert_eq!(Some(file.ts_max), ts_values.iter().max().copied());

            let dir =
                partition::partition_dir(tmp.path(), "trades", "binance_spot", pair, date.parse()?);
            assert_eq!(file.path, dir.join("part-0000.parquet"));
        }
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn part_numbering_continues_across_writer_restart() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let now = "2026-06-11T10:00:00Z".parse::<DateTime<Utc>>()?;

        let mut first = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);
        first.append(trade("BTC-USDT", "2026-06-11T09:59:58Z", "1")?, now)?;
        let finalized = first.finalize_all()?;
        assert_eq!(finalized.len(), 1);
        assert!(finalized[0].path.ends_with("part-0000.parquet"));
        drop(first);

        let mut second = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);
        second.append(trade("BTC-USDT", "2026-06-11T09:59:59Z", "2")?, now)?;
        let finalized = second.finalize_all()?;
        assert_eq!(finalized.len(), 1);
        assert!(finalized[0].path.ends_with("part-0001.parquet"));
        assert_eq!(read_trades(&finalized[0])?.len(), 1);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn footerless_file_is_quarantined_on_open() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let now = "2026-06-11T10:00:00Z".parse::<DateTime<Utc>>()?;
        let record = trade("BTC-USDT", "2026-06-11T09:59:58Z", "1")?;

        let dir = partition::partition_dir(
            tmp.path(),
            "trades",
            "binance_spot",
            "BTC-USDT",
            record.partition_date(),
        );
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("part-0000.parquet"), b"not a parquet file")?;

        let mut writer = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);
        writer.append(record, now)?;
        let finalized = writer.finalize_all()?;

        assert!(!dir.join("part-0000.parquet").exists());
        assert_eq!(
            std::fs::read(dir.join("part-0000.parquet.corrupt"))?,
            b"not a parquet file"
        );
        // Quarantined part numbers stay reserved: the new file is part-0001.
        assert_eq!(finalized[0].path, dir.join("part-0001.parquet"));
        assert_eq!(read_trades(&finalized[0])?.len(), 1);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn aged_file_rolls_on_next_append() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let opened = "2026-06-11T10:00:00Z".parse::<DateTime<Utc>>()?;
        let mut writer = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);

        writer.append(trade("BTC-USDT", "2026-06-11T09:59:58Z", "1")?, opened)?;
        // 14 minutes: not due yet (roll is strictly greater than the interval).
        assert!(writer.roll_due(opened + TimeDelta::minutes(14))?.is_empty());

        let later = opened + TimeDelta::minutes(16);
        let rolled = writer.append(trade("BTC-USDT", "2026-06-11T10:15:30Z", "2")?, later)?;
        assert_eq!(rolled.len(), 1);
        assert!(rolled[0].path.ends_with("part-0000.parquet"));
        assert_eq!(rolled[0].rows, 1);

        let finalized = writer.finalize_all()?;
        assert_eq!(finalized.len(), 1);
        assert!(finalized[0].path.ends_with("part-0001.parquet"));
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn finalize_without_appends_writes_nothing() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let mut writer = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);
        assert!(writer.finalize_all()?.is_empty());
        assert!(!tmp.path().join("parquet").exists());
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn buffered_rows_flush_as_row_groups_every_thousand_appends() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let now = "2026-06-11T10:00:00Z".parse::<DateTime<Utc>>()?;
        let mut writer = PartitionedWriter::<TradesDataset>::new(tmp.path(), ROLL);
        for id in 0..1001 {
            writer.append(
                trade("BTC-USDT", "2026-06-11T09:59:58Z", &id.to_string())?,
                now,
            )?;
        }
        let finalized = writer.finalize_all()?;
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].rows, 1001);

        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&finalized[0].path)?)?;
        // 1000 rows flushed mid-stream + 1 row flushed on finalize.
        assert_eq!(builder.metadata().num_row_groups(), 2);
        assert_eq!(read_trades(&finalized[0])?.len(), 1001);
        Ok(())
    }
}
