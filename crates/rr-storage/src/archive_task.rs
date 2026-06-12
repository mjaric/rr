//! Blocking archive writer tasks: the bridge from the async ingest world to
//! the synchronous [`PartitionedWriter`].
//!
//! The ingest supervisor sends [`TradeRecord`]s and [`CandleRecord`]s over
//! bounded tokio mpsc channels. Each dataset is drained on its own dedicated
//! OS thread so the synchronous `std::fs` Parquet writer never blocks the
//! async runtime. Both datasets share one generic worker ([`run_archiver`]).
//!
//! Each worker drives a ~1s timeout `recv` against the supplied runtime
//! [`Handle`]:
//! - a record is appended (rolling due files first);
//! - a closed channel (all senders dropped at shutdown) triggers
//!   `finalize_all` and a clean return;
//! - a timeout tick calls `roll_due` so idle open partitions still roll.
//!
//! Every finalized file is registered in `archive_files` via
//! [`Db::record_file`], blocking on the runtime handle. Ordering is
//! deliberate: `finalize` fsyncs and closes the Parquet file *before* its
//! `archive_files` row is written, so a crash between the two leaves a valid
//! but unregistered file on disk — reconciled by a future startup rescan
//! (documented as future work in the `parquet` module).
//!
//! Any writer or database error is FATAL: the worker returns `Err`, and the
//! CLI joins the thread and treats `Err` (or a thread panic) as
//! process-fatal. Before returning that `Err`, the dying worker signals the
//! shared shutdown watch so the supervisor drops BOTH archive senders. This
//! closes a quiet-market hang: the supervisor only learns a receiver died on
//! its next `send`, which may never come overnight, so without the signal the
//! surviving archiver would block on `recv` forever and [`ArchiveHandles::join`]
//! would never return.
//!
//! Lifetime contract: callers MUST [`ArchiveHandles::join`] the threads before
//! the tokio runtime is dropped. The workers call `handle.block_on`, and a
//! runtime dropped mid-`block_on` panics.

use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use tokio::runtime::Handle;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;

use crate::db::Db;
use crate::error::StorageError;
use crate::parquet::encode::{CandlesDataset, Dataset, TradesDataset};
use crate::parquet::{FinalizedFile, PartitionedWriter};
use crate::records::{CandleRecord, TradeRecord};

/// How long a worker waits for a record before falling back to a `roll_due`
/// tick, so idle partitions still roll without their next record.
const RECV_TIMEOUT: Duration = Duration::from_secs(1);

/// Shared configuration for the archive writer threads. Bundled so [`spawn`]
/// and [`run_archiver`] stay within the positional-parameter limit; the
/// per-dataset receivers are the only non-shared inputs.
#[derive(Clone)]
pub struct ArchiveConfig {
    /// Archive root; each [`PartitionedWriter`] is created under it.
    pub data_dir: PathBuf,
    /// How long an open part file may live before it rolls.
    pub roll_interval: TimeDelta,
    /// Operational database; finalized files are registered here.
    pub db: Db,
    /// Session every registered file is attributed to.
    pub session_id: i64,
    /// Runtime the blocking threads drive `recv`, timers, and DB calls on.
    pub handle: Handle,
    /// Global shutdown watch; a dying worker sends `true` so the supervisor
    /// drops both archive senders.
    pub shutdown: watch::Sender<bool>,
}

/// Join handles for the two archive writer threads (one per dataset).
pub struct ArchiveHandles {
    /// The trades-dataset writer thread.
    trades: JoinHandle<Result<(), StorageError>>,
    /// The candles-dataset writer thread.
    candles: JoinHandle<Result<(), StorageError>>,
}

impl ArchiveHandles {
    /// Joins both writer threads and surfaces the first error.
    ///
    /// A thread that returned `Err` surfaces that `StorageError`; a panicked
    /// thread surfaces [`StorageError::ArchiveThreadPanicked`]. Both threads
    /// are always joined (the candles result is computed even when trades
    /// failed) so neither is left detached.
    ///
    /// # Errors
    ///
    /// Returns the first [`StorageError`] from either thread, trades first.
    pub fn join(self) -> Result<(), StorageError> {
        let trades = join_one(self.trades, TradesDataset::NAME);
        let candles = join_one(self.candles, CandlesDataset::NAME);
        trades.and(candles)
    }
}

/// Joins one writer thread, mapping a panic to [`StorageError`].
fn join_one(
    handle: JoinHandle<Result<(), StorageError>>,
    dataset: &'static str,
) -> Result<(), StorageError> {
    match handle.join() {
        Ok(result) => result,
        Err(_panic) => {
            tracing::error!(dataset, "archive writer thread panicked");
            Err(StorageError::ArchiveThreadPanicked { dataset })
        }
    }
}

/// Spawns both archive writer threads and returns their join handles.
///
/// `config` carries the shared writer/database/runtime settings; the two
/// receivers are the per-dataset inputs the supervisor feeds. The CLI builds
/// `config.handle` from [`Handle::current`] on its multi-thread runtime, and
/// `config.shutdown` is the global watch a dying worker signals so the
/// supervisor drops both senders.
///
/// Callers MUST [`ArchiveHandles::join`] before dropping the runtime: the
/// workers `block_on` `config.handle`, and a runtime dropped mid-`block_on`
/// panics.
#[must_use]
pub fn spawn(
    config: ArchiveConfig,
    trades_rx: Receiver<TradeRecord>,
    candles_rx: Receiver<CandleRecord>,
) -> ArchiveHandles {
    let trades = spawn_one::<TradesDataset>(config.clone(), trades_rx);
    let candles = spawn_one::<CandlesDataset>(config, candles_rx);
    ArchiveHandles { trades, candles }
}

/// Spawns one dataset's writer thread.
fn spawn_one<D: Dataset>(
    config: ArchiveConfig,
    rx: Receiver<D::Record>,
) -> JoinHandle<Result<(), StorageError>>
where
    D::Record: Send + 'static,
{
    std::thread::spawn(move || {
        let writer = PartitionedWriter::<D>::new(config.data_dir.clone(), config.roll_interval);
        run_archiver::<D>(writer, rx, config)
    })
}

/// Drains `rx` into the writer, registering every finalized file in the
/// database.
///
/// Runs on a dedicated OS thread. Blocks on `config.handle` for the timed
/// `recv` and for each `record_file` call. Returns `Ok(())` once the channel
/// closes and `finalize_all` has flushed; any writer or database error
/// returns `Err` (fatal) after logging the failing dataset and signalling
/// `config.shutdown` so the supervisor drops both senders (closing the
/// quiet-market hang).
///
/// Callers MUST [`ArchiveHandles::join`] before dropping the runtime: this
/// `block_on`s `config.handle`, and a runtime dropped mid-`block_on` panics.
///
/// # Errors
///
/// Returns [`StorageError`] on the first writer or database failure.
pub fn run_archiver<D: Dataset>(
    writer: PartitionedWriter<D>,
    rx: Receiver<D::Record>,
    config: ArchiveConfig,
) -> Result<(), StorageError>
where
    D::Record: Send + 'static,
{
    let ArchiveConfig {
        db,
        session_id,
        handle,
        shutdown,
        ..
    } = config;
    let result = drive::<D>(writer, rx, &db, session_id, &handle);
    if result.is_err() {
        // Wake the supervisor so it drops both senders; otherwise the sibling
        // archiver blocks on recv forever in a quiet market and join() hangs.
        let _ = shutdown.send(true);
    }
    result
}

/// The archive drain loop. Split from [`run_archiver`] so the shutdown signal
/// fires on any `Err` exit regardless of which step failed.
fn drive<D: Dataset>(
    mut writer: PartitionedWriter<D>,
    mut rx: Receiver<D::Record>,
    db: &Db,
    session_id: i64,
    handle: &Handle,
) -> Result<(), StorageError>
where
    D::Record: Send + 'static,
{
    loop {
        let received =
            handle.block_on(async { tokio::time::timeout(RECV_TIMEOUT, rx.recv()).await });
        let finalized = match received {
            Ok(Some(record)) => fatal::<D, _>(writer.append(record, Utc::now()))?,
            Ok(None) => {
                let files = fatal::<D, _>(writer.finalize_all())?;
                record_files::<D>(&files, db, session_id, handle)?;
                return Ok(());
            }
            Err(_elapsed) => fatal::<D, _>(writer.roll_due(Utc::now()))?,
        };
        record_files::<D>(&finalized, db, session_id, handle)?;
    }
}

/// Registers each finalized file in `db`, blocking on `handle`. A database
/// failure is fatal and logged with the dataset context.
fn record_files<D: Dataset>(
    files: &[FinalizedFile],
    db: &Db,
    session_id: i64,
    handle: &Handle,
) -> Result<(), StorageError> {
    for file in files {
        let result = handle.block_on(db.record_file(session_id, file));
        fatal::<D, _>(result)?;
    }
    Ok(())
}

/// Logs `result`'s error with the dataset name and returns it unchanged, so
/// every fatal exit carries which dataset thread failed.
fn fatal<D: Dataset, T>(result: Result<T, StorageError>) -> Result<T, StorageError> {
    result.map_err(|error| {
        tracing::error!(dataset = D::NAME, %error, "archive writer thread failed");
        error
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;

    use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
    use rust_decimal::Decimal;
    use tokio::runtime::Handle;
    use tokio::sync::{mpsc, watch};

    use crate::archive_task::{ArchiveConfig, spawn};
    use crate::db::Db;
    use crate::records::{CandleRecord, Side, TradeRecord};

    type TestResult = Result<(), Box<dyn Error>>;

    const ROLL: TimeDelta = TimeDelta::minutes(15);
    const PARTITION_DATE: &str = "2026-06-11";

    fn config(
        dir: &tempfile::TempDir,
        roll_interval: TimeDelta,
        db: Db,
        session_id: i64,
        shutdown: watch::Sender<bool>,
    ) -> ArchiveConfig {
        ArchiveConfig {
            data_dir: dir.path().to_path_buf(),
            roll_interval,
            db,
            session_id,
            handle: Handle::current(),
            shutdown,
        }
    }

    fn trade(id: &str) -> Result<TradeRecord, Box<dyn Error>> {
        Ok(TradeRecord {
            exchange: "binance_spot".to_owned(),
            pair: "BTC-USDT".to_owned(),
            ts_exchange: "2026-06-11T09:59:58Z".parse::<DateTime<Utc>>()?,
            ts_received: "2026-06-11T10:00:01Z".parse::<DateTime<Utc>>()?,
            price: "104500.25".parse::<Decimal>()?,
            amount: "0.5".parse::<Decimal>()?,
            side: Side::Buy,
            trade_id: id.to_owned(),
        })
    }

    fn candle() -> Result<CandleRecord, Box<dyn Error>> {
        Ok(CandleRecord {
            exchange: "binance_spot".to_owned(),
            pair: "BTC-USDT".to_owned(),
            ts_open: "2026-06-11T09:59:00Z".parse::<DateTime<Utc>>()?,
            open: "104500".parse::<Decimal>()?,
            high: "104600".parse::<Decimal>()?,
            low: "104400".parse::<Decimal>()?,
            close: "104550".parse::<Decimal>()?,
            volume: "12.5".parse::<Decimal>()?,
            trade_count: 7,
        })
    }

    async fn open_db(dir: &tempfile::TempDir) -> Result<Db, Box<dyn Error>> {
        let db = Db::open(&dir.path().join("rr.sqlite")).await?;
        Ok(db)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn archives_trades_and_candles_on_clean_shutdown() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let (trades_tx, trades_rx) = mpsc::channel(16);
        let (candles_tx, candles_rx) = mpsc::channel(16);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let handles = spawn(
            config(&tmp, ROLL, db.clone(), session, shutdown_tx),
            trades_rx,
            candles_rx,
        );

        for id in 0..10 {
            trades_tx.send(trade(&id.to_string())?).await?;
        }
        candles_tx.send(candle()?).await?;

        drop(trades_tx);
        drop(candles_tx);
        tokio::task::spawn_blocking(move || handles.join()).await??;

        let date = NaiveDate::from_str(PARTITION_DATE)?;
        let files = db.files_for_date(date).await?;
        assert_eq!(files.len(), 2);

        let trades_file = files
            .iter()
            .find(|f| f.dataset == "trades")
            .ok_or("no trades file")?;
        assert_eq!(trades_file.rows, 10);
        assert_eq!(trades_file.exchange, "binance_spot");
        assert_eq!(trades_file.pair, "BTC-USDT");
        assert!(
            PathBuf::from(&trades_file.path).exists(),
            "trades parquet missing"
        );

        let candles_file = files
            .iter()
            .find(|f| f.dataset == "candles")
            .ok_or("no candles file")?;
        assert_eq!(candles_file.rows, 1);
        assert!(
            PathBuf::from(&candles_file.path).exists(),
            "candles parquet missing"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn timeout_tick_rolls_idle_open_file_before_shutdown() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let (trades_tx, trades_rx) = mpsc::channel(16);
        let (candles_tx, candles_rx) = mpsc::channel(16);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        // Zero roll interval: any open file is due on the first timeout tick.
        let handles = spawn(
            config(&tmp, TimeDelta::zero(), db.clone(), session, shutdown_tx),
            trades_rx,
            candles_rx,
        );

        trades_tx.send(trade("1")?).await?;
        // Poll until the timeout-tick roll_due registers the idle file, rather
        // than racing a fixed sleep against the ~1s tick + record_file latency.
        let date = NaiveDate::from_str(PARTITION_DATE)?;
        let files = poll_files(&db, date, 1).await?;
        assert_eq!(files.len(), 1, "idle file should roll before shutdown");
        assert_eq!(files[0].rows, 1);

        drop(trades_tx);
        drop(candles_tx);
        tokio::task::spawn_blocking(move || handles.join()).await??;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn unencodable_record_makes_join_surface_error() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let (trades_tx, trades_rx) = mpsc::channel(16);
        let (candles_tx, candles_rx) = mpsc::channel(16);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let handles = spawn(
            config(&tmp, ROLL, db.clone(), session, shutdown_tx),
            trades_rx,
            candles_rx,
        );

        // 19 fractional digits: rejected by TradesDataset::validate at append.
        let mut bad = trade("1")?;
        bad.price = Decimal::from_str("0.0000000000000000001")?;
        trades_tx.send(bad).await?;
        drop(trades_tx);
        drop(candles_tx);

        let result = tokio::task::spawn_blocking(move || handles.join()).await?;
        match result {
            Err(crate::error::StorageError::DecimalPrecision { scale, .. }) => {
                assert_eq!(scale, 19);
            }
            other => panic!("expected DecimalPrecision, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn dying_writer_signals_shutdown_so_join_does_not_hang() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let (trades_tx, trades_rx) = mpsc::channel(16);
        // Candles sender stays alive: this models a quiet market where the
        // supervisor never sends and so never notices a dead receiver.
        let (candles_tx, candles_rx) = mpsc::channel(16);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let handles = spawn(
            config(&tmp, ROLL, db.clone(), session, shutdown_tx),
            trades_rx,
            candles_rx,
        );

        let mut bad = trade("1")?;
        bad.price = Decimal::from_str("0.0000000000000000001")?;
        trades_tx.send(bad).await?;
        drop(trades_tx);

        // The dying trades thread must signal shutdown. In production the
        // supervisor reacts by dropping the candles sender; here we wait for
        // the signal, then drop it ourselves to let the candles thread finish.
        shutdown_rx.changed().await?;
        assert!(*shutdown_rx.borrow(), "dying writer should signal shutdown");
        drop(candles_tx);

        let result = tokio::task::spawn_blocking(move || handles.join()).await?;
        match result {
            Err(crate::error::StorageError::DecimalPrecision { scale, .. }) => {
                assert_eq!(scale, 19);
            }
            other => panic!("expected DecimalPrecision, got {other:?}"),
        }
        Ok(())
    }

    /// Polls `files_for_date` every ~50ms up to a 5s deadline, returning as
    /// soon as at least `want` files are registered. Event-driven, so it is
    /// not sensitive to the archiver's tick/record latency.
    async fn poll_files(
        db: &Db,
        date: NaiveDate,
        want: usize,
    ) -> Result<Vec<crate::db::ArchiveFileRow>, Box<dyn Error>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let files = db.files_for_date(date).await?;
            if files.len() >= want {
                return Ok(files);
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting for {want} archive file(s)").into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
