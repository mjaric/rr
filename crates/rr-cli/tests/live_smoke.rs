//! Live overnight-path smoke test for the `rr` binary.
//!
//! This is a single `#[ignore]`d integration test that drives the real
//! `rr stream` → graceful-shutdown → `rr archive-status` path end-to-end
//! against live exchange websockets. It is for manual verification only and is
//! never run in CI.
//!
//! Run it manually with:
//! `cargo test -p rr-cli --test live_smoke -- --ignored`
//!
//! Unix only: shutdown is triggered by sending `SIGINT` via `kill -INT <pid>`,
//! which the CLI wires (ctrl-c) to a clean flush-and-end-session path. The file
//! body is gated on `#[cfg(unix)]` so it still compiles on other platforms.
#![cfg(unix)]

use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::Duration;

use assert_cmd::Command as AssertCommand;
use assert_cmd::cargo::cargo_bin;
use chrono::Utc;

/// How long to let the stream run before shutting it down. Long enough for real
/// trades to arrive and at least one candle minute boundary to pass.
const RUN_SECS: u64 = 30;

#[test]
#[ignore = "live: connects to real Binance/Coinbase websockets; run manually"]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assert! macros are the test's failure mechanism"
)]
fn live_stream_archives_trades_and_is_queryable() -> Result<(), Box<dyn Error>> {
    let tempdir = tempfile::tempdir()?;
    let data_dir = tempdir.path();

    // Spawn the real binary with std::process so we keep the child handle and
    // PID; assert_cmd's Command does not expose those, and we need the PID to
    // send SIGINT for graceful shutdown.
    let rr_path: PathBuf = cargo_bin("rr");
    let mut child = StdCommand::new(&rr_path)
        .arg("stream")
        .arg("--data-dir")
        .arg(data_dir)
        .spawn()?;

    std::thread::sleep(Duration::from_secs(RUN_SECS));

    // SIGINT (not child.kill(), which sends SIGKILL and would skip the graceful
    // flush + end_session path) so the archive writers finalize their parts.
    let signalled = StdCommand::new("kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status()?;
    assert!(signalled.success(), "failed to deliver SIGINT to rr stream");

    let status = child.wait()?;
    assert!(
        status.success(),
        "graceful shutdown did not exit 0: {status:?}"
    );

    // Trades are the reliable assertion: any live trade during the window
    // produces at least one finalized trades part on shutdown.
    //
    // Candles are intentionally NOT asserted: a candle file is only written on
    // roll or shutdown-finalize, and whether a full minute closes within a ~30s
    // window depends on minute alignment plus the aggregator's grace period, so
    // asserting it would be flaky. The trades assertion proves the path works.
    let trades_root = data_dir.join("parquet").join("trades");
    assert!(
        has_parquet_file(&trades_root)?,
        "no finalized trades parquet under {}",
        trades_root.display()
    );

    // archive-status must read the freshly archived data back and name an
    // exchange, proving the archive is queryable. Use assert_cmd here since we
    // only need the exit status and captured stdout.
    let today = Utc::now().date_naive().to_string();
    let output = AssertCommand::new(&rr_path)
        .arg("archive-status")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--date")
        .arg(&today)
        .output()?;
    assert!(
        output.status.success(),
        "archive-status did not exit 0: {:?}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains("binance_spot") || stdout.contains("coinbase"),
        "archive-status stdout names no exchange: {stdout}"
    );

    Ok(())
}

/// Recursively reports whether any `*.parquet` (not `*.parquet.corrupt`) file
/// exists under `root`. Uses `std::fs` recursion deliberately — a test does not
/// warrant the `walkdir` dependency. A missing `root` is treated as "no file",
/// which lets the caller's `assert!` produce the clear failure message.
fn has_parquet_file(root: &Path) -> Result<bool, Box<dyn Error>> {
    if !root.exists() {
        return Ok(false);
    }
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            if has_parquet_file(&path)? {
                return Ok(true);
            }
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".parquet"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}
