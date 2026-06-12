//! Archive coverage report: per (exchange, pair) candle-minute coverage for
//! one UTC day, with missing minutes classified as quiet (market silent
//! while connected) or gap (overlapping a disconnected interval). Backend
//! of `rr archive-status`.
//!
//! # Coverage model (M1)
//!
//! A minute window `[m, m+60s)` is **expected** when it has fully elapsed
//! within the day: `m >= date 00:00Z` and `m+60s <= min(now, date+1 00:00Z)`
//! — 1440 minutes for a past date, the elapsed whole minutes for today,
//! none for a future date. An expected minute is **present** when the candle
//! archive holds a `ts_open == m` row; otherwise it is **gap** when its
//! window overlaps any disconnected interval (half-open overlap:
//! `dis_start < m+60s` and `dis_end > m`), else **quiet**.
//! `expected == present + quiet + gap` always holds.
//!
//! Disconnected intervals are derived per exchange (connection events carry
//! no pair, so they apply to every pair of the exchange) from, exactly:
//!
//! 1. each explicit `disconnected` → next `connected` event pair on the day,
//!    a trailing `disconnected` extending to the end of the day;
//! 2. the start of the day up to the day's first `connected` event, unless
//!    the latest connection event before the day is `connected` (carry-in);
//!    an exchange with no connection event recorded before the day starts
//!    the day disconnected;
//! 3. for every session with `ended_at` NULL: the span from the session's
//!    last recorded event (its `started_at` if it has none) to the next
//!    session's start (end of day if none), clamped to the day and applied
//!    to every exchange — a session that never ended cleanly proves nothing
//!    about process liveness after its last write.
//!
//! The model trusts clean shutdowns to record `disconnected` events: time
//! between a cleanly ended session and the next one is classified by rules
//! 1–2 alone. Quarantined `*.corrupt` candle files are skipped with a
//! warning, so coverage may undercount after a crash. Misclassification
//! only ever skews quiet vs gap — never present vs expected.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use ::parquet::arrow::ProjectionMask;
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::{Array, TimestampMillisecondArray};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};

use crate::db::{ArchiveFileRow, Db, EventKind, StreamEvent, UnendedSession};
use crate::error::StorageError;
use crate::parquet::partition;

/// Half-open UTC time interval `[start, end)`.
type Interval = (DateTime<Utc>, DateTime<Utc>);

/// Candle-minute coverage of one (exchange, pair) for one UTC day; see the
/// module docs for the classification model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairCoverage {
    /// Exchange identifier (e.g. `binance_spot`).
    pub exchange: String,
    /// Trading pair (e.g. `BTC-USDT`).
    pub pair: String,
    /// Registered archive files for the day, both datasets.
    pub files: u64,
    /// Sum of registered `trades` file row counts for the day.
    pub trade_rows: u64,
    /// Fully elapsed minute windows of the day (1440, or fewer for today).
    pub minutes_expected: u64,
    /// Expected minutes with a candle `ts_open` in the archive.
    pub minutes_present: u64,
    /// Missing minutes while connected the whole window (quiet market).
    pub minutes_quiet: u64,
    /// Missing minutes overlapping a disconnected interval.
    pub minutes_gap: u64,
    /// `gap_detected` events recorded for the pair on the day.
    pub gap_events: u64,
    /// `late_trade` events recorded for the pair on the day.
    pub late_events: u64,
}

/// Classifies the day's minute windows from candle window starts and
/// disconnected intervals; returns `(expected, present, quiet, gap)`.
/// Pure — see the module docs for the exact rules.
#[must_use]
pub fn classify(
    date: NaiveDate,
    now: DateTime<Utc>,
    present: &BTreeSet<DateTime<Utc>>,
    disconnected: &[(DateTime<Utc>, DateTime<Utc>)],
) -> (u64, u64, u64, u64) {
    let day_start = date.and_time(NaiveTime::MIN).and_utc();
    let day_end = date.succ_opt().map_or(DateTime::<Utc>::MAX_UTC, |next| {
        next.and_time(NaiveTime::MIN).and_utc()
    });
    let horizon = now.min(day_end);
    let minute = TimeDelta::seconds(60);

    let (mut expected, mut found, mut quiet, mut gap) = (0_u64, 0_u64, 0_u64, 0_u64);
    let mut window_start = day_start;
    while let Some(window_end) = window_start.checked_add_signed(minute) {
        if window_end > horizon {
            break;
        }
        expected += 1;
        if present.contains(&window_start) {
            found += 1;
        } else if disconnected
            .iter()
            .any(|&(dis_start, dis_end)| dis_start < window_end && dis_end > window_start)
        {
            gap += 1;
        } else {
            quiet += 1;
        }
        window_start = window_end;
    }
    (expected, found, quiet, gap)
}

/// Builds the coverage report for `date` over every (exchange, pair) seen
/// in `archive_files` or in the candle archive directories for that date,
/// sorted by (exchange, pair).
///
/// # Errors
///
/// Returns [`StorageError`] if the database queries fail, a candle part
/// file cannot be read, or `date` has no successor day.
pub async fn archive_status(
    data_dir: &Path,
    db: &Db,
    date: NaiveDate,
) -> Result<Vec<PairCoverage>, StorageError> {
    archive_status_at(data_dir, db, date, Utc::now()).await
}

/// [`archive_status`] with an injected clock, the deterministic seam the
/// tests use.
async fn archive_status_at(
    data_dir: &Path,
    db: &Db,
    date: NaiveDate,
    now: DateTime<Utc>,
) -> Result<Vec<PairCoverage>, StorageError> {
    let day_start = date.and_time(NaiveTime::MIN).and_utc();
    let day_end = date
        .succ_opt()
        .ok_or(StorageError::DateOutOfRange { date })?
        .and_time(NaiveTime::MIN)
        .and_utc();

    let files = db.files_for_date(date).await?;
    let events = db.events_for_date(date).await?;
    let carry: BTreeMap<String, EventKind> = db
        .connection_states_before(day_start)
        .await?
        .into_iter()
        .collect();
    let tails = session_tails(&db.unended_sessions().await?, day_start, day_end);

    let mut keys = candle_partitions(data_dir, date)?;
    keys.extend(files.iter().map(|f| (f.exchange.clone(), f.pair.clone())));

    let mut by_exchange: BTreeMap<&str, Vec<Interval>> = BTreeMap::new();
    for exchange in keys.iter().map(|(exchange, _)| exchange.as_str()) {
        if by_exchange.contains_key(exchange) {
            continue;
        }
        let starts_connected = carry.get(exchange) == Some(&EventKind::Connected);
        let transitions: Vec<(DateTime<Utc>, bool)> = events
            .iter()
            .filter(|ev| ev.exchange.as_deref() == Some(exchange))
            .filter_map(|ev| match ev.kind {
                EventKind::Connected => Some((ev.ts, true)),
                EventKind::Disconnected => Some((ev.ts, false)),
                EventKind::GapDetected | EventKind::LateTrade | EventKind::Error => None,
            })
            .collect();
        let mut intervals =
            disconnected_intervals(day_start, day_end, starts_connected, &transitions);
        intervals.extend_from_slice(&tails);
        by_exchange.insert(exchange, intervals);
    }

    let mut report = Vec::with_capacity(keys.len());
    for (exchange, pair) in &keys {
        let present = candle_minutes(data_dir, exchange, pair, date)?;
        let disconnected = by_exchange
            .get(exchange.as_str())
            .map_or(&[][..], Vec::as_slice);
        let (minutes_expected, minutes_present, minutes_quiet, minutes_gap) =
            classify(date, now, &present, disconnected);
        let (file_count, trade_rows) = file_totals(&files, exchange, pair);
        report.push(PairCoverage {
            exchange: exchange.clone(),
            pair: pair.clone(),
            files: file_count,
            trade_rows,
            minutes_expected,
            minutes_present,
            minutes_quiet,
            minutes_gap,
            gap_events: count_pair_events(&events, EventKind::GapDetected, exchange, pair),
            late_events: count_pair_events(&events, EventKind::LateTrade, exchange, pair),
        });
    }
    Ok(report)
}

/// Disconnected intervals of one exchange within `[day_start, day_end)`
/// from its carry-in state and the day's connection transitions
/// (`(ts, connected)`, ascending).
fn disconnected_intervals(
    day_start: DateTime<Utc>,
    day_end: DateTime<Utc>,
    starts_connected: bool,
    transitions: &[(DateTime<Utc>, bool)],
) -> Vec<Interval> {
    let mut intervals = Vec::new();
    let mut down_since = (!starts_connected).then_some(day_start);
    for &(ts, connected) in transitions {
        if connected {
            if let Some(since) = down_since.take()
                && since < ts
            {
                intervals.push((since, ts));
            }
        } else if down_since.is_none() {
            down_since = Some(ts);
        }
    }
    if let Some(since) = down_since
        && since < day_end
    {
        intervals.push((since, day_end));
    }
    intervals
}

/// Process-dead spans of unended sessions, clamped to the day (rule 3 of
/// the module-doc model). Applied to every exchange.
fn session_tails(
    sessions: &[UnendedSession],
    day_start: DateTime<Utc>,
    day_end: DateTime<Utc>,
) -> Vec<Interval> {
    let mut tails = Vec::new();
    for session in sessions {
        let from = session
            .last_event_ts
            .unwrap_or(session.started_at)
            .max(day_start);
        let to = session.next_session_start.unwrap_or(day_end).min(day_end);
        if from < to {
            tails.push((from, to));
        }
    }
    tails
}

/// Registered file count (both datasets) and `trades` row sum of one
/// (exchange, pair) for the day.
fn file_totals(files: &[ArchiveFileRow], exchange: &str, pair: &str) -> (u64, u64) {
    let mut count = 0_u64;
    let mut trade_rows = 0_u64;
    for file in files {
        if file.exchange != exchange || file.pair != pair {
            continue;
        }
        count += 1;
        if file.dataset != "trades" {
            continue;
        }
        if let Ok(rows) = u64::try_from(file.rows) {
            trade_rows += rows;
        } else {
            // record_file never stores a negative count; a negative value
            // means external corruption — surface it, keep reporting.
            tracing::warn!(
                path = %file.path,
                rows = file.rows,
                "negative row count in archive_files; not counted"
            );
        }
    }
    (count, trade_rows)
}

/// Events of `kind` recorded for exactly this (exchange, pair); events with
/// `pair` NULL never match (connection events are exchange-level).
fn count_pair_events(events: &[StreamEvent], kind: EventKind, exchange: &str, pair: &str) -> u64 {
    let mut count = 0_u64;
    for event in events {
        if event.kind == kind
            && event.exchange.as_deref() == Some(exchange)
            && event.pair.as_deref() == Some(pair)
        {
            count += 1;
        }
    }
    count
}

/// (exchange, pair) keys with a candle partition directory for `date`,
/// discovered from the Hive-style archive layout. A missing archive root
/// yields the empty set.
fn candle_partitions(
    data_dir: &Path,
    date: NaiveDate,
) -> Result<BTreeSet<(String, String)>, StorageError> {
    let mut keys = BTreeSet::new();
    let root = data_dir.join("parquet").join("candles");
    for exchange_dir in read_dir_paths(&root)? {
        let Some(exchange) = hive_component(&exchange_dir, "exchange=") else {
            continue;
        };
        for pair_dir in read_dir_paths(&exchange_dir)? {
            let Some(pair) = hive_component(&pair_dir, "pair=") else {
                continue;
            };
            if pair_dir.join(format!("date={date}")).is_dir() {
                keys.insert((exchange.clone(), pair));
            }
        }
    }
    Ok(keys)
}

/// Candle window starts (`ts_open`) found in the partition's part files.
/// Quarantined `*.corrupt` files are skipped with a warning; a missing
/// partition directory yields the empty set.
fn candle_minutes(
    data_dir: &Path,
    exchange: &str,
    pair: &str,
    date: NaiveDate,
) -> Result<BTreeSet<DateTime<Utc>>, StorageError> {
    let dir = partition::partition_dir(data_dir, "candles", exchange, pair, date);
    let mut minutes = BTreeSet::new();
    for path in read_dir_paths(&dir)? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            tracing::warn!(
                file = %path.display(),
                "skipping non-UTF-8 file name in candle partition"
            );
            continue;
        };
        if name.ends_with(".corrupt") {
            tracing::warn!(
                file = %path.display(),
                "skipping quarantined candle file; coverage may undercount"
            );
            continue;
        }
        if !name.ends_with(".parquet") {
            continue;
        }
        read_ts_open_into(&path, &mut minutes)?;
    }
    Ok(minutes)
}

/// Reads only the `ts_open` column of one candle part file into `minutes`.
fn read_ts_open_into(
    path: &Path,
    minutes: &mut BTreeSet<DateTime<Utc>>,
) -> Result<(), StorageError> {
    let file = File::open(path).map_err(|source| io_error(path, source))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let ts_open_index = builder.schema().index_of("ts_open")?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [ts_open_index]);
    let reader = builder.with_projection(mask).build()?;
    for batch in reader {
        let batch = batch?;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                arrow::error::ArrowError::SchemaError(format!(
                    "ts_open in {} is not a millisecond timestamp column",
                    path.display()
                ))
            })?;
        // The writer declares ts_open non-nullable, so null slots mean a
        // damaged or foreign file — skip them instead of reading the
        // zero-filled value buffer underneath.
        if column.null_count() != 0 {
            tracing::warn!(
                file = %path.display(),
                nulls = column.null_count(),
                "null ts_open slots in candle file; skipped — coverage may undercount"
            );
        }
        for ms in column.iter().flatten() {
            let Some(ts) = DateTime::from_timestamp_millis(ms) else {
                tracing::warn!(
                    file = %path.display(),
                    ms,
                    "ts_open out of chrono range; row skipped"
                );
                continue;
            };
            minutes.insert(ts);
        }
    }
    Ok(())
}

/// Paths inside `dir`, sorted; a missing directory yields the empty list.
fn read_dir_paths(dir: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(io_error(dir, source)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| io_error(dir, source))?;
        paths.push(entry.path());
    }
    paths.sort();
    Ok(paths)
}

/// `prefix`-stripped name of a Hive-style partition directory (`None` for
/// files, other names, or non-UTF-8, which is skipped with a warning).
fn hive_component(path: &Path, prefix: &str) -> Option<String> {
    if !path.is_dir() {
        return None;
    }
    let name = path.file_name()?;
    let Some(name) = name.to_str() else {
        tracing::warn!(
            dir = %path.display(),
            "skipping non-UTF-8 directory name in candle archive"
        );
        return None;
    };
    name.strip_prefix(prefix).map(str::to_owned)
}

fn io_error(path: &Path, source: std::io::Error) -> StorageError {
    StorageError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::error::Error;

    use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
    use rust_decimal::Decimal;

    use crate::db::{Db, EventKind, StreamEvent, UnendedSession};
    use crate::error::StorageError;
    use crate::parquet::encode::CandlesDataset;
    use crate::parquet::{FinalizedFile, PartitionedWriter, partition};
    use crate::records::CandleRecord;
    use crate::status::{
        PairCoverage, archive_status, archive_status_at, classify, disconnected_intervals,
        session_tails,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    /// The report day used throughout: 2026-06-10 UTC.
    const DAY: &str = "2026-06-10";

    #[expect(clippy::unwrap_used, reason = "test helper; inputs are literals")]
    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[expect(clippy::unwrap_used, reason = "test helper; input is a literal")]
    fn day() -> NaiveDate {
        DAY.parse().unwrap()
    }

    fn day_start() -> DateTime<Utc> {
        ts("2026-06-10T00:00:00Z")
    }

    fn day_end() -> DateTime<Utc> {
        ts("2026-06-11T00:00:00Z")
    }

    fn minute(index: u32) -> DateTime<Utc> {
        day_start() + TimeDelta::minutes(i64::from(index))
    }

    fn minutes(indices: impl IntoIterator<Item = u32>) -> BTreeSet<DateTime<Utc>> {
        indices.into_iter().map(minute).collect()
    }

    // --- classify -------------------------------------------------------

    #[test]
    fn classify_full_day_present_is_fully_covered() {
        let present = minutes(0..1440);
        let report = classify(day(), ts("2026-06-12T08:00:00Z"), &present, &[]);
        assert_eq!(report, (1440, 1440, 0, 0));
    }

    #[test]
    fn classify_missing_minute_is_gap_inside_disconnect_quiet_outside() {
        // 10:00 missing inside the disconnect window, 12:00 missing outside;
        // 10:01–10:04 are present and stay present despite the disconnect.
        let present = minutes((0..1440).filter(|i| *i != 600 && *i != 720));
        let disconnected = [(ts("2026-06-10T10:00:00Z"), ts("2026-06-10T10:05:00Z"))];
        let report = classify(day(), day_end(), &present, &disconnected);
        assert_eq!(report, (1440, 1438, 1, 1));
    }

    #[test]
    fn classify_disconnect_overlap_is_half_open_on_both_ends() {
        // Disconnected [10:00, 10:05). Missing minutes: 09:59 (window ends
        // exactly at the disconnect start) and 10:05 (window starts exactly
        // at the disconnect end) are quiet; 10:00 and 10:04 are gap.
        let missing = [599, 600, 604, 605];
        let present = minutes((0..1440).filter(|i| !missing.contains(i)));
        let disconnected = [(ts("2026-06-10T10:00:00Z"), ts("2026-06-10T10:05:00Z"))];
        let report = classify(day(), day_end(), &present, &disconnected);
        assert_eq!(report, (1440, 1436, 2, 2));
    }

    #[test]
    fn classify_today_truncates_expected_to_elapsed_whole_minutes() {
        // now mid-minute: the 10:30 window has not fully elapsed.
        let present = minutes(0..631);
        let report = classify(day(), ts("2026-06-10T10:30:30Z"), &present, &[]);
        assert_eq!(report, (630, 630, 0, 0));
        // now exactly on a minute boundary: the 10:29 window just closed.
        let report = classify(day(), ts("2026-06-10T10:30:00Z"), &present, &[]);
        assert_eq!(report, (630, 630, 0, 0));
    }

    #[test]
    fn classify_future_date_expects_nothing() {
        let report = classify(day(), ts("2026-06-09T23:59:59Z"), &minutes(0..10), &[]);
        assert_eq!(report, (0, 0, 0, 0));
    }

    #[test]
    fn classify_ignores_ts_open_outside_the_expected_range() {
        let mut present = minutes([300]);
        present.insert(ts("2026-06-09T23:59:00Z"));
        present.insert(ts("2026-06-11T00:00:00Z"));
        let report = classify(day(), day_end(), &present, &[]);
        assert_eq!(report, (1440, 1, 1439, 0));
    }

    mod props {
        use std::collections::BTreeSet;

        use chrono::{DateTime, TimeDelta, Utc};
        use proptest::prelude::{prop, proptest};
        use proptest::prop_assert_eq;

        use crate::status::classify;
        use crate::status::tests::{day, day_start};

        proptest! {
            #[test]
            fn classify_partitions_expected_minutes(
                present_idx in prop::collection::btree_set(0u32..1440, 0..64),
                raw_intervals in prop::collection::vec((0i64..86_400, 0i64..86_400), 0..6),
                now_offset_s in -600i64..90_000,
            ) {
                let start = day_start();
                let present: BTreeSet<DateTime<Utc>> = present_idx
                    .iter()
                    .map(|&i| start + TimeDelta::minutes(i64::from(i)))
                    .collect();
                let disconnected: Vec<(DateTime<Utc>, DateTime<Utc>)> = raw_intervals
                    .iter()
                    .map(|&(a, b)| {
                        let (from, to) = if a <= b { (a, b) } else { (b, a) };
                        (start + TimeDelta::seconds(from), start + TimeDelta::seconds(to))
                    })
                    .collect();
                let now = start + TimeDelta::seconds(now_offset_s);

                let (expected, present_n, quiet, gap) =
                    classify(day(), now, &present, &disconnected);

                let elapsed_minutes = (now_offset_s / 60).clamp(0, 1440);
                #[expect(clippy::cast_sign_loss, reason = "clamped to non-negative")]
                let elapsed_minutes = elapsed_minutes as u64;
                prop_assert_eq!(expected, elapsed_minutes);
                prop_assert_eq!(expected, present_n + quiet + gap);
            }
        }
    }

    // --- disconnected_intervals ------------------------------------------

    #[test]
    fn intervals_unknown_carry_in_and_no_events_is_disconnected_all_day() {
        let intervals = disconnected_intervals(day_start(), day_end(), false, &[]);
        assert_eq!(intervals, vec![(day_start(), day_end())]);
    }

    #[test]
    fn intervals_connected_carry_in_and_no_events_is_empty() {
        let intervals = disconnected_intervals(day_start(), day_end(), true, &[]);
        assert!(intervals.is_empty());
    }

    #[test]
    fn intervals_pairs_and_trailing_disconnect() {
        let transitions = [
            (ts("2026-06-10T02:00:00Z"), false),
            (ts("2026-06-10T03:00:00Z"), true),
            (ts("2026-06-10T22:00:00Z"), false),
        ];
        let intervals = disconnected_intervals(day_start(), day_end(), true, &transitions);
        assert_eq!(
            intervals,
            vec![
                (ts("2026-06-10T02:00:00Z"), ts("2026-06-10T03:00:00Z")),
                (ts("2026-06-10T22:00:00Z"), day_end()),
            ]
        );
    }

    #[test]
    fn intervals_duplicate_transitions_collapse() {
        let transitions = [
            (ts("2026-06-10T01:00:00Z"), true),
            (ts("2026-06-10T02:00:00Z"), true),
            (ts("2026-06-10T03:00:00Z"), false),
            (ts("2026-06-10T04:00:00Z"), false),
        ];
        let intervals = disconnected_intervals(day_start(), day_end(), false, &transitions);
        assert_eq!(
            intervals,
            vec![
                (day_start(), ts("2026-06-10T01:00:00Z")),
                (ts("2026-06-10T03:00:00Z"), day_end()),
            ]
        );
    }

    #[test]
    fn intervals_connected_at_day_start_yields_no_leading_interval() {
        let transitions = [(day_start(), true)];
        let intervals = disconnected_intervals(day_start(), day_end(), false, &transitions);
        assert!(intervals.is_empty());
    }

    // --- session_tails ----------------------------------------------------

    fn unended(
        started_at: &str,
        last_event_ts: Option<&str>,
        next_session_start: Option<&str>,
    ) -> UnendedSession {
        UnendedSession {
            started_at: ts(started_at),
            last_event_ts: last_event_ts.map(ts),
            next_session_start: next_session_start.map(ts),
        }
    }

    #[test]
    fn tail_runs_from_last_event_to_day_end() {
        let sessions = [unended(
            "2026-06-09T20:00:00Z",
            Some("2026-06-10T06:00:00Z"),
            None,
        )];
        let tails = session_tails(&sessions, day_start(), day_end());
        assert_eq!(tails, vec![(ts("2026-06-10T06:00:00Z"), day_end())]);
    }

    #[test]
    fn tail_clips_at_next_session_start_and_day_bounds() {
        let sessions = [unended(
            "2026-06-09T20:00:00Z",
            Some("2026-06-09T23:00:00Z"),
            Some("2026-06-10T08:00:00Z"),
        )];
        let tails = session_tails(&sessions, day_start(), day_end());
        assert_eq!(tails, vec![(day_start(), ts("2026-06-10T08:00:00Z"))]);
    }

    #[test]
    fn tail_of_eventless_session_starts_at_session_start() {
        let sessions = [unended("2026-06-10T05:00:00Z", None, None)];
        let tails = session_tails(&sessions, day_start(), day_end());
        assert_eq!(tails, vec![(ts("2026-06-10T05:00:00Z"), day_end())]);
    }

    #[test]
    fn tail_outside_the_day_is_dropped() {
        let sessions = [
            unended("2026-06-11T05:00:00Z", None, None),
            unended("2026-06-09T05:00:00Z", Some("2026-06-11T01:00:00Z"), None),
        ];
        assert!(session_tails(&sessions, day_start(), day_end()).is_empty());
    }

    // --- archive_status ----------------------------------------------------

    fn candle(exchange: &str, pair: &str, ts_open: DateTime<Utc>) -> CandleRecord {
        CandleRecord {
            exchange: exchange.to_owned(),
            pair: pair.to_owned(),
            ts_open,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            trade_count: 1,
        }
    }

    fn event(ts_s: &str, exchange: &str, pair: Option<&str>, kind: EventKind) -> StreamEvent {
        StreamEvent {
            ts: ts(ts_s),
            exchange: Some(exchange.to_owned()),
            pair: pair.map(str::to_owned),
            kind,
            details: None,
        }
    }

    fn trades_file(exchange: &str, pair: &str, rows: u64) -> FinalizedFile {
        FinalizedFile {
            dataset: "trades",
            exchange: exchange.to_owned(),
            pair: pair.to_owned(),
            date: day(),
            path: format!("/data/trades/{exchange}/{pair}/part-0000.parquet").into(),
            rows,
            ts_min: day_start(),
            ts_max: day_end() - TimeDelta::milliseconds(1),
        }
    }

    /// Writes the given candle minutes for (exchange, pair) under `data_dir`
    /// and returns the finalized files.
    fn write_candles(
        data_dir: &std::path::Path,
        exchange: &str,
        pair: &str,
        indices: impl IntoIterator<Item = u32>,
    ) -> Result<Vec<FinalizedFile>, StorageError> {
        let mut writer = PartitionedWriter::<CandlesDataset>::new(data_dir, TimeDelta::minutes(15));
        for index in indices {
            writer.append(candle(exchange, pair, minute(index)), day_start())?;
        }
        writer.finalize_all()
    }

    async fn open_db(dir: &tempfile::TempDir) -> Result<Db, StorageError> {
        Db::open(&dir.path().join("rr.sqlite")).await
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn report_covers_present_quiet_and_gap_minutes() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let events = [
            event(
                "2026-06-10T00:00:00Z",
                "binance_spot",
                None,
                EventKind::Connected,
            ),
            event(
                "2026-06-10T10:00:00Z",
                "binance_spot",
                None,
                EventKind::Disconnected,
            ),
            event(
                "2026-06-10T10:05:00Z",
                "binance_spot",
                None,
                EventKind::Connected,
            ),
            event(
                "2026-06-10T10:06:00Z",
                "binance_spot",
                Some("BTC-USDT"),
                EventKind::GapDetected,
            ),
            event(
                "2026-06-10T10:07:00Z",
                "binance_spot",
                Some("BTC-USDT"),
                EventKind::GapDetected,
            ),
            event(
                "2026-06-10T11:00:00Z",
                "binance_spot",
                Some("BTC-USDT"),
                EventKind::LateTrade,
            ),
        ];
        for ev in &events {
            db.record_event(session, ev).await?;
        }
        // A pair-less gap_detected should not exist (the detector is
        // per-pair) but must be ignored, not crash the report.
        let stray = StreamEvent {
            ts: ts("2026-06-10T13:00:00Z"),
            exchange: Some("binance_spot".to_owned()),
            pair: None,
            kind: EventKind::GapDetected,
            details: None,
        };
        db.record_event(session, &stray).await?;

        // Candles: everything except 10:00–10:04 (disconnected → gap) and
        // 12:00–12:02 (connected → quiet).
        let missing = [600, 601, 602, 603, 604, 720, 721, 722];
        let candle_files = write_candles(
            tmp.path(),
            "binance_spot",
            "BTC-USDT",
            (0..1440).filter(|i| !missing.contains(i)),
        )?;
        for file in &candle_files {
            db.record_file(session, file).await?;
        }
        db.record_file(session, &trades_file("binance_spot", "BTC-USDT", 5000))
            .await?;
        db.end_session(session).await?;

        let report = archive_status_at(tmp.path(), &db, day(), ts("2026-06-11T08:00:00Z")).await?;
        let candle_file_count = u64::try_from(candle_files.len())?;
        assert_eq!(
            report,
            vec![PairCoverage {
                exchange: "binance_spot".to_owned(),
                pair: "BTC-USDT".to_owned(),
                files: candle_file_count + 1,
                trade_rows: 5000,
                minutes_expected: 1440,
                minutes_present: 1432,
                minutes_quiet: 3,
                minutes_gap: 5,
                gap_events: 2,
                late_events: 1,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn pair_known_only_from_archive_files_with_no_events_is_all_gap() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;
        db.record_file(session, &trades_file("coinbase", "ETH-USD", 123))
            .await?;
        db.end_session(session).await?;

        let report = archive_status_at(tmp.path(), &db, day(), day_end()).await?;
        assert_eq!(
            report,
            vec![PairCoverage {
                exchange: "coinbase".to_owned(),
                pair: "ETH-USD".to_owned(),
                files: 1,
                trade_rows: 123,
                minutes_expected: 1440,
                minutes_present: 0,
                minutes_quiet: 0,
                minutes_gap: 1440,
                gap_events: 0,
                late_events: 0,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn unended_session_marks_time_after_its_last_event_as_gap() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;
        db.record_event(
            session,
            &event(
                "2026-06-10T00:00:00Z",
                "binance_spot",
                None,
                EventKind::Connected,
            ),
        )
        .await?;
        db.record_event(
            session,
            &event(
                "2026-06-10T06:00:00Z",
                "binance_spot",
                Some("BTC-USDT"),
                EventKind::GapDetected,
            ),
        )
        .await?;
        // Session never ends: everything after its last event (06:00) is
        // unproven liveness, hence gap.
        write_candles(tmp.path(), "binance_spot", "BTC-USDT", 0..360)?;

        let report = archive_status_at(tmp.path(), &db, day(), ts("2026-06-12T00:00:00Z")).await?;
        assert_eq!(
            report,
            vec![PairCoverage {
                exchange: "binance_spot".to_owned(),
                pair: "BTC-USDT".to_owned(),
                files: 0,
                trade_rows: 0,
                minutes_expected: 1440,
                minutes_present: 360,
                minutes_quiet: 0,
                minutes_gap: 1080,
                gap_events: 1,
                late_events: 0,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn carry_in_connection_state_classifies_leading_minutes() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;
        // The day before: binance_spot ends disconnected, coinbase connected.
        let events = [
            event(
                "2026-06-09T22:00:00Z",
                "binance_spot",
                None,
                EventKind::Connected,
            ),
            event(
                "2026-06-09T23:00:00Z",
                "binance_spot",
                None,
                EventKind::Disconnected,
            ),
            event(
                "2026-06-09T23:30:00Z",
                "coinbase",
                None,
                EventKind::Connected,
            ),
        ];
        for ev in &events {
            db.record_event(session, ev).await?;
        }
        db.end_session(session).await?;

        write_candles(tmp.path(), "binance_spot", "BTC-USDT", [0])?;
        write_candles(tmp.path(), "coinbase", "ETH-USD", 0..10)?;

        let report = archive_status_at(tmp.path(), &db, day(), day_end()).await?;
        assert_eq!(
            report,
            vec![
                PairCoverage {
                    exchange: "binance_spot".to_owned(),
                    pair: "BTC-USDT".to_owned(),
                    files: 0,
                    trade_rows: 0,
                    minutes_expected: 1440,
                    minutes_present: 1,
                    minutes_quiet: 0,
                    minutes_gap: 1439,
                    gap_events: 0,
                    late_events: 0,
                },
                PairCoverage {
                    exchange: "coinbase".to_owned(),
                    pair: "ETH-USD".to_owned(),
                    files: 0,
                    trade_rows: 0,
                    minutes_expected: 1440,
                    minutes_present: 10,
                    minutes_quiet: 1430,
                    minutes_gap: 0,
                    gap_events: 0,
                    late_events: 0,
                },
            ]
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn empty_archive_and_database_yield_an_empty_report() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        // Exercise the public wrapper: the date is in the past relative to
        // any test clock, and an empty universe is empty regardless of now.
        assert_eq!(archive_status(tmp.path(), &db, day()).await?, vec![]);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn corrupt_only_partition_is_reported_not_read() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let dir =
            partition::partition_dir(tmp.path(), "candles", "binance_spot", "BTC-USDT", day());
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("part-0000.parquet.corrupt"), b"not parquet")?;
        std::fs::write(dir.join("notes.txt"), b"ignored")?;

        let report = archive_status_at(tmp.path(), &db, day(), day_end()).await?;
        assert_eq!(
            report,
            vec![PairCoverage {
                exchange: "binance_spot".to_owned(),
                pair: "BTC-USDT".to_owned(),
                files: 0,
                trade_rows: 0,
                minutes_expected: 1440,
                minutes_present: 0,
                minutes_quiet: 0,
                minutes_gap: 1440,
                gap_events: 0,
                late_events: 0,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn damaged_parquet_file_fails_the_report() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let dir =
            partition::partition_dir(tmp.path(), "candles", "binance_spot", "BTC-USDT", day());
        std::fs::create_dir_all(&dir)?;
        // Correctly named but not parquet: never quarantined (only the
        // writer quarantines on open), so reading it must surface an error.
        std::fs::write(dir.join("part-0000.parquet"), b"junk junk junk junk")?;

        match archive_status_at(tmp.path(), &db, day(), day_end()).await {
            Err(StorageError::Parquet(_)) => {}
            other => panic!("expected StorageError::Parquet, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn null_ts_open_slots_are_skipped_not_read_as_garbage() -> TestResult {
        use std::sync::Arc;

        use ::parquet::arrow::ArrowWriter;
        use arrow::array::{RecordBatch, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        // The parquet reader zero-fills the value slots of an all-null
        // column, and zero milliseconds is 1970-01-01T00:00Z — report on
        // that day so a reader ignoring the null bitmap would count a null
        // slot as a present minute.
        let epoch_day: NaiveDate = "1970-01-01".parse()?;
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let dir =
            partition::partition_dir(tmp.path(), "candles", "binance_spot", "BTC-USDT", epoch_day);
        std::fs::create_dir_all(&dir)?;

        let ts_open = TimestampMillisecondArray::from(vec![None, None]).with_timezone("UTC");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts_open",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_open)])?;
        let file = std::fs::File::create(dir.join("part-0000.parquet"))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        let report =
            archive_status_at(tmp.path(), &db, epoch_day, ts("1970-01-02T00:00:00Z")).await?;
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].minutes_present, 0);
        Ok(())
    }
}
