//! `rr` — trading intelligence app command-line interface.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use chrono::{NaiveDate, TimeDelta, Utc};
use clap::{Parser, Subcommand};
use rr_engine::market_data::supervisor::ArchiveSenders;
use rr_engine::market_data::{spec, supervisor};
use rr_storage::archive_task::{self, ArchiveConfig};
use rr_storage::db::Db;
use rr_storage::status::{self, PairCoverage};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

/// How long an open Parquet part file may live before it rolls (production).
const ROLL_INTERVAL: TimeDelta = TimeDelta::minutes(15);
/// Bounded trade-archive channel capacity.
const TRADE_CHANNEL_CAPACITY: usize = 4096;
/// Bounded candle-archive channel capacity.
const CANDLE_CHANNEL_CAPACITY: usize = 1024;

/// Simulation-first crypto trading agent.
#[derive(Parser)]
#[command(name = "rr", version, about)]
struct Cli {
    /// The subcommand to run.
    #[command(subcommand)]
    command: Commands,
}

/// Top-level `rr` subcommands.
#[derive(Subcommand)]
enum Commands {
    /// Ingest the M1 public-trade streams to the Parquet archive overnight.
    Stream {
        /// Archive root: holds `rr.sqlite` and the `parquet/` partition tree.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },
    /// Report M1 archive coverage for one UTC day as an aligned text table.
    ArchiveStatus {
        /// UTC day to report (`YYYY-MM-DD`); defaults to today UTC.
        #[arg(long)]
        date: Option<NaiveDate>,
        /// Archive root: holds `rr.sqlite` and the `parquet/` partition tree.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let Cli { command } = Cli::parse();
    match command {
        Commands::Stream { data_dir } => run_stream(&data_dir).await,
        Commands::ArchiveStatus { date, data_dir } => {
            let date = date.unwrap_or_else(|| Utc::now().date_naive());
            run_archive_status(&data_dir, date).await
        }
    }
}

/// Reads the archive coverage for `date` and prints an aligned text table.
///
/// Opens `<data_dir>/rr.sqlite` (created empty if absent), queries the
/// coverage report, renders it, and prints it. An empty archive is reported
/// as data, not an error.
async fn run_archive_status(data_dir: &Path, date: NaiveDate) -> anyhow::Result<()> {
    let db = Db::open(&data_dir.join("rr.sqlite"))
        .await
        .context("opening operational database")?;
    let rows = status::archive_status(data_dir, &db, date)
        .await
        .context("reading archive coverage")?;
    print_status(&render_status(date, &rows));
    Ok(())
}

/// Writes the rendered report to stdout. This is the only stdout write in the
/// binary; all other output goes through `tracing` to stderr.
#[expect(
    clippy::print_stdout,
    reason = "archive-status is a user-facing report; stdout is its output"
)]
fn print_status(report: &str) {
    println!("{report}");
}

/// Renders the coverage report for `date` as an aligned, header-prefixed text
/// table. Pure: no IO, so it is unit-testable. An empty `rows` yields a single
/// "no archive data" line for the date.
fn render_status(date: NaiveDate, rows: &[PairCoverage]) -> String {
    if rows.is_empty() {
        return format!("no archive data for {date}");
    }
    let header = [
        "EXCHANGE", "PAIR", "FILES", "TRADES", "COVERAGE", "QUIET", "GAP", "GAP_EVT", "LATE_EVT",
        "STATUS",
    ];
    let mut table: Vec<[String; 10]> = vec![header.map(ToOwned::to_owned)];
    for row in rows {
        table.push(format_row(row));
    }
    let widths = column_widths(&table);
    let mut out = format!("archive coverage for {date}\n");
    for record in &table {
        out.push_str(&format_line(record, &widths));
        out.push('\n');
    }
    out.truncate(out.trim_end().len());
    out
}

/// Formats one coverage row into its ten display cells.
fn format_row(row: &PairCoverage) -> [String; 10] {
    let coverage = format!("{}/{}", row.minutes_present, row.minutes_expected);
    let verdict = if row.minutes_gap == 0 {
        "OK".to_owned()
    } else {
        format!("GAPS:{}", row.minutes_gap)
    };
    [
        row.exchange.clone(),
        row.pair.clone(),
        row.files.to_string(),
        row.trade_rows.to_string(),
        coverage,
        row.minutes_quiet.to_string(),
        row.minutes_gap.to_string(),
        row.gap_events.to_string(),
        row.late_events.to_string(),
        verdict,
    ]
}

/// Computes the max display width of each column across every row.
fn column_widths(table: &[[String; 10]]) -> [usize; 10] {
    let mut widths = [0usize; 10];
    for record in table {
        for (width, cell) in widths.iter_mut().zip(record) {
            *width = (*width).max(cell.len());
        }
    }
    widths
}

/// Left-aligns each cell to its column width, joined by two spaces.
fn format_line(record: &[String; 10], widths: &[usize; 10]) -> String {
    let mut cells: Vec<String> = Vec::with_capacity(record.len());
    for (cell, &width) in record.iter().zip(widths) {
        cells.push(format!("{cell:<width$}"));
    }
    cells.join("  ").trim_end().to_owned()
}

/// Installs the `tracing` subscriber (stderr), honouring `RUST_LOG` and
/// defaulting to `info`.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Runs the overnight ingest: opens the store, starts a session, builds the
/// real streams, spawns the archive writers, and drives the supervisor until
/// ctrl-c (or a fatal error) shuts it down cleanly.
///
/// Errors are surfaced by precedence: an archive-writer failure is the
/// data-loss root cause and is returned first; otherwise the supervisor's
/// fatal error propagates. A clean ctrl-c shutdown returns `Ok`.
async fn run_stream(data_dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;
    let db = Db::open(&data_dir.join("rr.sqlite"))
        .await
        .context("opening operational database")?;
    let config_json = spec::config_json();
    let session_id = db
        .start_session(&config_json)
        .await
        .context("starting stream session")?;
    tracing::info!(session_id, pairs = ?spec::PAIRS, "stream session started");

    let stream = supervisor::build_stream()
        .await
        .context("building market data streams")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let Archivers { handles, senders } =
        spawn_archivers(data_dir.to_path_buf(), &db, session_id, &shutdown_tx);
    spawn_ctrl_c(shutdown_tx);

    let run_result = supervisor::run(stream, db.clone(), session_id, senders, shutdown_rx).await;
    // The runtime is still alive here, so joining the blocking writers is safe
    // (their `block_on` calls require a live runtime).
    let archive_result = handles.join();
    finalize(&db, session_id).await;
    surface(run_result, archive_result, session_id)
}

/// The archive handles plus the channel senders the supervisor will own.
struct Archivers {
    handles: archive_task::ArchiveHandles,
    senders: ArchiveSenders,
}

/// Spawns both archive writer threads and returns their handles alongside the
/// senders the supervisor takes ownership of.
fn spawn_archivers(
    data_dir: PathBuf,
    db: &Db,
    session_id: i64,
    shutdown: &watch::Sender<bool>,
) -> Archivers {
    let (trades_tx, trades_rx) = mpsc::channel(TRADE_CHANNEL_CAPACITY);
    let (candles_tx, candles_rx) = mpsc::channel(CANDLE_CHANNEL_CAPACITY);
    let config = ArchiveConfig {
        data_dir,
        roll_interval: ROLL_INTERVAL,
        db: db.clone(),
        session_id,
        handle: Handle::current(),
        shutdown: shutdown.clone(),
    };
    let handles = archive_task::spawn(config, trades_rx, candles_rx);
    Archivers {
        handles,
        senders: ArchiveSenders {
            trades: trades_tx,
            candles: candles_tx,
        },
    }
}

/// Spawns a task that signals shutdown on ctrl-c.
fn spawn_ctrl_c(shutdown: watch::Sender<bool>) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown.send(true);
        }
    });
}

/// Best-effort session close: a failure here must not mask the real ingest or
/// archive error, so it is logged, not returned.
async fn finalize(db: &Db, session_id: i64) {
    if let Err(error) = db.end_session(session_id).await {
        tracing::warn!(session_id, %error, "failed to mark stream session ended");
    }
}

/// Resolves the final exit status. An archive-writer error is the data-loss
/// root cause and takes precedence; otherwise the supervisor's fatal error
/// propagates. A clean shutdown logs a summary and returns `Ok`.
fn surface(
    run_result: Result<(), rr_engine::error::EngineError>,
    archive_result: Result<(), rr_storage::error::StorageError>,
    session_id: i64,
) -> anyhow::Result<()> {
    archive_result.context("archive writer failed; market data may be lost")?;
    run_result.context("market data ingest failed")?;
    tracing::info!(session_id, "stream session ended cleanly");
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert_cmd::Command;
    use chrono::NaiveDate;
    use clap::CommandFactory;
    use rr_storage::status::PairCoverage;

    use super::{Cli, render_status};

    /// Builds a `PairCoverage` for `binance_spot`/`BTC-USDT` with the given
    /// present minutes and gap minutes; other counters are illustrative.
    fn coverage(minutes_present: u64, minutes_gap: u64) -> PairCoverage {
        PairCoverage {
            exchange: "binance_spot".to_owned(),
            pair: "BTC-USDT".to_owned(),
            files: 2,
            trade_rows: 12_345,
            minutes_expected: 1440,
            minutes_present,
            minutes_quiet: 1440 - minutes_present - minutes_gap,
            minutes_gap,
            gap_events: 0,
            late_events: 0,
        }
    }

    /// Like [`coverage`] but with a caller-chosen `trade_rows`, so two rows can
    /// differ in the TRADES column width and exercise column alignment.
    fn coverage_with_trades(
        trade_rows: u64,
        minutes_present: u64,
        minutes_gap: u64,
    ) -> PairCoverage {
        PairCoverage {
            trade_rows,
            ..coverage(minutes_present, minutes_gap)
        }
    }

    fn date() -> NaiveDate {
        #[expect(clippy::unwrap_used, reason = "literal test date is statically valid")]
        NaiveDate::from_ymd_opt(2026, 6, 12).unwrap()
    }

    #[test]
    fn render_marks_clean_coverage_ok() {
        let out = render_status(date(), &[coverage(1440, 0)]);
        let line = out
            .lines()
            .find(|l| l.contains("binance_spot"))
            .unwrap_or_default();
        assert!(line.contains("BTC-USDT"), "missing pair: {out}");
        assert!(line.contains("OK"), "clean row not OK: {out}");
        assert!(!line.contains("GAPS:"), "clean row flagged gaps: {out}");
    }

    #[test]
    fn render_marks_gapped_coverage() {
        let out = render_status(date(), &[coverage(1437, 3)]);
        let line = out
            .lines()
            .find(|l| l.contains("binance_spot"))
            .unwrap_or_default();
        assert!(line.contains("GAPS:3"), "gap row missing GAPS:3: {out}");
        assert!(line.contains("BTC-USDT"), "missing pair: {out}");
    }

    #[test]
    fn render_shows_present_over_expected() {
        let out = render_status(date(), &[coverage(1438, 0)]);
        assert!(out.contains("1438/1440"), "coverage ratio missing: {out}");
    }

    #[test]
    fn render_empty_reports_no_data() {
        let out = render_status(date(), &[]);
        assert!(out.contains("no archive data"), "missing notice: {out}");
        assert!(out.contains("2026-06-12"), "missing date: {out}");
    }

    #[test]
    fn render_aligns_columns_with_varied_widths() {
        // Rows differ in the TRADES column width (8 digits vs 1), which sits
        // left of COVERAGE; ragged columns would shift COVERAGE's offset.
        let rows = [
            coverage_with_trades(12_345_678, 1440, 0),
            coverage_with_trades(1, 1437, 3),
        ];
        let out = render_status(date(), &rows);
        let header = out
            .lines()
            .find(|l| l.contains("EXCHANGE"))
            .unwrap_or_default();
        let data = out
            .lines()
            .find(|l| l.contains("GAPS:3"))
            .unwrap_or_default();
        let header_offset = header.find("COVERAGE");
        let data_offset = data.find("1437/1440");
        assert!(header_offset.is_some(), "header missing COVERAGE: {out}");
        assert_eq!(
            header_offset, data_offset,
            "COVERAGE column misaligned: {out}"
        );
    }

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// Runs `rr <args>`, asserts a success exit, and returns captured stdout.
    #[expect(
        clippy::unwrap_used,
        reason = "test binary is built by cargo; absence is a build failure"
    )]
    fn help_stdout(args: &[&str]) -> String {
        let assert = Command::cargo_bin("rr")
            .unwrap()
            .args(args)
            .assert()
            .success();
        String::from_utf8(assert.get_output().stdout.clone()).unwrap()
    }

    #[test]
    fn root_help_lists_stream_subcommand() {
        let stdout = help_stdout(&["--help"]);
        assert!(
            stdout.contains("stream"),
            "root help missing stream: {stdout}"
        );
    }

    #[test]
    fn stream_help_documents_data_dir() {
        let stdout = help_stdout(&["stream", "--help"]);
        assert!(
            stdout.contains("--data-dir"),
            "stream help missing --data-dir: {stdout}"
        );
    }

    #[test]
    fn archive_status_help_documents_flags() {
        let stdout = help_stdout(&["archive-status", "--help"]);
        assert!(stdout.contains("--date"), "help missing --date: {stdout}");
        assert!(
            stdout.contains("--data-dir"),
            "help missing --data-dir: {stdout}"
        );
    }

    #[test]
    #[expect(
        clippy::unwrap_used,
        reason = "test rig: tempdir creation failure is a test failure"
    )]
    fn archive_status_empty_archive_reports_no_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let stdout = help_stdout(&["archive-status", "--data-dir", path, "--date", "2026-06-12"]);
        assert!(
            stdout.contains("no archive data"),
            "empty archive notice missing: {stdout}"
        );
    }
}
