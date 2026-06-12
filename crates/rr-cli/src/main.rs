//! `rr` — trading intelligence app command-line interface.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use chrono::TimeDelta;
use clap::{Parser, Subcommand};
use rr_engine::market_data::supervisor::ArchiveSenders;
use rr_engine::market_data::{spec, supervisor};
use rr_storage::archive_task::{self, ArchiveConfig};
use rr_storage::db::Db;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let Cli { command } = Cli::parse();
    match command {
        Commands::Stream { data_dir } => run_stream(&data_dir).await,
    }
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
    use clap::CommandFactory;

    use super::Cli;

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
}
