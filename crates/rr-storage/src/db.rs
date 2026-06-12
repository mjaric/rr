//! `SQLite` operational store (schema v1): stream sessions, stream events,
//! and the archive-file registry.
//!
//! Connections open in WAL mode so the single writer (the engine process)
//! never blocks readers (`rr archive-status`, later the server). Foreign
//! keys are enforced per connection via
//! `SqliteConnectOptions::foreign_keys(true)` — `SQLite` defaults them OFF.
//! Pool sizing uses sqlx defaults (max 10 connections): this process is the
//! only writer and `SQLite` serializes writes itself, so no tuning is needed.
//!
//! Timestamps are RFC3339 TEXT in UTC, written by sqlx's chrono encoder
//! (`to_rfc3339_opts(SecondsFormat::AutoSi, false)`, e.g.
//! `2026-06-12T10:00:00.123456+00:00`). Because every stored value carries
//! the same fixed `+00:00` offset, the encoding sorts lexicographically in
//! timestamp order, so day queries use TEXT range predicates with bound
//! `DateTime<Utc>` endpoints — sqlx applies the same encoder to the bound
//! parameters, keeping the comparison exact. Dates are `%Y-%m-%d` TEXT.

use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use sqlx::Row;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
};

use crate::error::StorageError;
use crate::parquet::FinalizedFile;

/// What a stream event records. Canonical string encoding via
/// [`EventKind::as_str`] / [`EventKind::parse`] — never invent another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// A market-data stream (re)connected.
    Connected,
    /// A market-data stream disconnected.
    Disconnected,
    /// The gap detector saw missing trade ids.
    GapDetected,
    /// A trade arrived for an already-finalized window or partition.
    LateTrade,
    /// A non-fatal error worth surfacing in coverage reports.
    Error,
}

impl EventKind {
    /// Canonical string encoding stored in `stream_events.kind`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Connected => "connected",
            EventKind::Disconnected => "disconnected",
            EventKind::GapDetected => "gap_detected",
            EventKind::LateTrade => "late_trade",
            EventKind::Error => "error",
        }
    }

    /// Parses the canonical encoding produced by [`EventKind::as_str`].
    #[must_use]
    pub fn parse(s: &str) -> Option<EventKind> {
        match s {
            "connected" => Some(EventKind::Connected),
            "disconnected" => Some(EventKind::Disconnected),
            "gap_detected" => Some(EventKind::GapDetected),
            "late_trade" => Some(EventKind::LateTrade),
            "error" => Some(EventKind::Error),
            _ => None,
        }
    }
}

/// One operational event of a stream session (`stream_events` row, minus
/// the database-assigned id and session id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEvent {
    /// When the event happened, UTC.
    pub ts: DateTime<Utc>,
    /// Exchange the event concerns, if any (e.g. `binance_spot`).
    pub exchange: Option<String>,
    /// Pair the event concerns, if any (e.g. `BTC-USDT`).
    pub pair: Option<String>,
    /// What happened.
    pub kind: EventKind,
    /// Free-form detail (gap sizes, error messages).
    pub details: Option<String>,
}

/// One `archive_files` row: a finalized Parquet part file registered under
/// the session that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveFileRow {
    /// Database-assigned row id.
    pub id: i64,
    /// Session that produced the file.
    pub session_id: i64,
    /// Dataset directory name (`trades` / `candles`).
    pub dataset: String,
    /// Exchange partition component.
    pub exchange: String,
    /// Pair partition component.
    pub pair: String,
    /// Date partition component.
    pub date: NaiveDate,
    /// Absolute path of the part file.
    pub path: String,
    /// Number of rows in the file.
    pub rows: i64,
    /// Smallest dataset timestamp in the file.
    pub ts_min: DateTime<Utc>,
    /// Largest dataset timestamp in the file.
    pub ts_max: DateTime<Utc>,
    /// When the file was finalized and registered.
    pub closed_at: DateTime<Utc>,
}

/// Handle to the operational `SQLite` database; see the module docs for
/// connection and encoding invariants.
pub struct Db {
    pool: SqlitePool,
}

impl Db {
    /// Opens (creating if missing) the database at `path` in WAL mode with
    /// foreign keys enforced, and runs the embedded migrations. Migrations
    /// are tracked in `_sqlx_migrations`, so reopening an up-to-date file
    /// is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Sqlx`] if connecting or migrating fails.
    pub async fn open(path: &Path) -> Result<Self, StorageError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(op_err("open database"))?;
        sqlx::migrate!()
            .run(&pool)
            .await
            .map_err(|source| StorageError::Sqlx {
                operation: "run migrations",
                source: source.into(),
            })?;
        Ok(Self { pool })
    }

    /// Inserts a `stream_sessions` row with `started_at = now` and the
    /// given config JSON; returns the new session id.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Sqlx`] if the insert fails.
    pub async fn start_session(&self, config_json: &str) -> Result<i64, StorageError> {
        let result = sqlx::query("INSERT INTO stream_sessions (started_at, config) VALUES (?, ?)")
            .bind(Utc::now())
            .bind(config_json)
            .execute(&self.pool)
            .await
            .map_err(op_err("start session"))?;
        Ok(result.last_insert_rowid())
    }

    /// Sets `ended_at = now` on the session.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SessionNotFound`] if no such session exists,
    /// or [`StorageError::Sqlx`] if the update fails.
    pub async fn end_session(&self, session_id: i64) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE stream_sessions SET ended_at = ? WHERE id = ?")
            .bind(Utc::now())
            .bind(session_id)
            .execute(&self.pool)
            .await
            .map_err(op_err("end session"))?;
        if result.rows_affected() == 0 {
            return Err(StorageError::SessionNotFound { session_id });
        }
        Ok(())
    }

    /// Inserts one `stream_events` row under the session.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Sqlx`] if the insert fails (including a
    /// foreign-key violation for an unknown session).
    pub async fn record_event(
        &self,
        session_id: i64,
        ev: &StreamEvent,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO stream_events (session_id, ts, exchange, pair, kind, details) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(session_id)
        .bind(ev.ts)
        .bind(ev.exchange.as_deref())
        .bind(ev.pair.as_deref())
        .bind(ev.kind.as_str())
        .bind(ev.details.as_deref())
        .execute(&self.pool)
        .await
        .map_err(op_err("record event"))?;
        Ok(())
    }

    /// Registers a finalized Parquet part file in `archive_files` with
    /// `closed_at = now`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::NonUtf8Path`] / [`StorageError::RowCountOverflow`]
    /// if the file metadata cannot be stored as TEXT/INTEGER, or
    /// [`StorageError::Sqlx`] if the insert fails.
    pub async fn record_file(
        &self,
        session_id: i64,
        f: &FinalizedFile,
    ) -> Result<(), StorageError> {
        let path = f.path.to_str().ok_or_else(|| StorageError::NonUtf8Path {
            path: f.path.clone(),
        })?;
        let rows =
            i64::try_from(f.rows).map_err(|_| StorageError::RowCountOverflow { rows: f.rows })?;
        sqlx::query(
            "INSERT INTO archive_files \
             (session_id, dataset, exchange, pair, date, path, \"rows\", ts_min, ts_max, closed_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(session_id)
        .bind(f.dataset)
        .bind(&f.exchange)
        .bind(&f.pair)
        .bind(f.date)
        .bind(path)
        .bind(rows)
        .bind(f.ts_min)
        .bind(f.ts_max)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(op_err("record file"))?;
        Ok(())
    }

    /// Returns every stream event whose `ts` falls within the UTC day
    /// `date` (`[date 00:00Z, date+1 00:00Z)`), ascending by `ts`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Sqlx`] if the query fails,
    /// [`StorageError::UnknownEventKind`] for an unrecognized `kind`, or
    /// [`StorageError::DateOutOfRange`] for `NaiveDate::MAX`.
    pub async fn events_for_date(&self, date: NaiveDate) -> Result<Vec<StreamEvent>, StorageError> {
        let (start, end) = utc_day_bounds(date)?;
        let rows = sqlx::query(
            "SELECT ts, exchange, pair, kind, details FROM stream_events \
             WHERE ts >= ? AND ts < ? ORDER BY ts, id",
        )
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(op_err("query events for date"))?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(stream_event_from_row(&row)?);
        }
        Ok(events)
    }

    /// Returns every registered archive file whose `date` partition column
    /// equals `date`, ascending by id (registration order).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Sqlx`] if the query fails.
    pub async fn files_for_date(
        &self,
        date: NaiveDate,
    ) -> Result<Vec<ArchiveFileRow>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, session_id, dataset, exchange, pair, date, path, \"rows\", \
             ts_min, ts_max, closed_at \
             FROM archive_files WHERE date = ? ORDER BY id",
        )
        .bind(date)
        .fetch_all(&self.pool)
        .await
        .map_err(op_err("query files for date"))?;
        let mut files = Vec::with_capacity(rows.len());
        for row in rows {
            files.push(archive_file_from_row(&row)?);
        }
        Ok(files)
    }
}

/// Half-open UTC day range `[date 00:00Z, date+1 00:00Z)` for TEXT range
/// predicates (see the module docs for why lexicographic comparison is
/// sound).
fn utc_day_bounds(date: NaiveDate) -> Result<(DateTime<Utc>, DateTime<Utc>), StorageError> {
    let start = date.and_time(NaiveTime::MIN).and_utc();
    let end = date
        .succ_opt()
        .ok_or(StorageError::DateOutOfRange { date })?
        .and_time(NaiveTime::MIN)
        .and_utc();
    Ok((start, end))
}

fn stream_event_from_row(row: &SqliteRow) -> Result<StreamEvent, StorageError> {
    let kind: String = col(row, "kind")?;
    let kind = EventKind::parse(&kind).ok_or(StorageError::UnknownEventKind { kind })?;
    Ok(StreamEvent {
        ts: col(row, "ts")?,
        exchange: col(row, "exchange")?,
        pair: col(row, "pair")?,
        kind,
        details: col(row, "details")?,
    })
}

fn archive_file_from_row(row: &SqliteRow) -> Result<ArchiveFileRow, StorageError> {
    Ok(ArchiveFileRow {
        id: col(row, "id")?,
        session_id: col(row, "session_id")?,
        dataset: col(row, "dataset")?,
        exchange: col(row, "exchange")?,
        pair: col(row, "pair")?,
        date: col(row, "date")?,
        path: col(row, "path")?,
        rows: col(row, "rows")?,
        ts_min: col(row, "ts_min")?,
        ts_max: col(row, "ts_max")?,
        closed_at: col(row, "closed_at")?,
    })
}

/// [`Row::try_get`] with the storage-layer error wrapping applied.
fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T, StorageError>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get(name).map_err(op_err("decode row column"))
}

/// Wraps an [`sqlx::Error`] with the name of the failing repository
/// operation.
fn op_err(operation: &'static str) -> impl FnOnce(sqlx::Error) -> StorageError {
    move |source| StorageError::Sqlx { operation, source }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use chrono::{DateTime, NaiveDate, Utc};
    use sqlx::Row;

    use crate::db::{ArchiveFileRow, Db, EventKind, StreamEvent};
    use crate::error::StorageError;
    use crate::parquet::FinalizedFile;

    type TestResult = Result<(), Box<dyn Error>>;

    async fn open_db(dir: &tempfile::TempDir) -> Result<Db, StorageError> {
        Db::open(&dir.path().join("rr.sqlite")).await
    }

    fn event(ts: &str, kind: EventKind) -> Result<StreamEvent, Box<dyn Error>> {
        Ok(StreamEvent {
            ts: ts.parse::<DateTime<Utc>>()?,
            exchange: Some("binance_spot".to_owned()),
            pair: Some("BTC-USDT".to_owned()),
            kind,
            details: Some("detail".to_owned()),
        })
    }

    fn finalized_file(date: &str) -> Result<FinalizedFile, Box<dyn Error>> {
        Ok(FinalizedFile {
            dataset: "trades",
            exchange: "binance_spot".to_owned(),
            pair: "BTC-USDT".to_owned(),
            date: date.parse::<NaiveDate>()?,
            path: format!("/data/parquet/trades/date={date}/part-0000.parquet").into(),
            rows: 1234,
            ts_min: format!("{date}T00:00:00.001Z").parse::<DateTime<Utc>>()?,
            ts_max: format!("{date}T23:59:59.999Z").parse::<DateTime<Utc>>()?,
        })
    }

    #[test]
    fn event_kind_canonical_encoding_round_trips() {
        let kinds = [
            (EventKind::Connected, "connected"),
            (EventKind::Disconnected, "disconnected"),
            (EventKind::GapDetected, "gap_detected"),
            (EventKind::LateTrade, "late_trade"),
            (EventKind::Error, "error"),
        ];
        for (kind, encoded) in kinds {
            assert_eq!(kind.as_str(), encoded);
            assert_eq!(EventKind::parse(encoded), Some(kind));
        }
        assert_eq!(EventKind::parse("Connected"), None);
        assert_eq!(EventKind::parse(""), None);
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn open_runs_migration_idempotently_and_sets_pragmas() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;

        let mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&db.pool)
            .await?
            .try_get(0)?;
        assert_eq!(mode, "wal");
        let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(&db.pool)
            .await?
            .try_get(0)?;
        assert_eq!(foreign_keys, 1);
        drop(db);

        // Reopening the same file must rerun migrations as a no-op and
        // leave a usable database.
        let reopened = open_db(&tmp).await?;
        let session = reopened.start_session("{}").await?;
        assert!(session >= 1);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn session_lifecycle_assigns_increasing_ids_and_sets_ended_at() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;

        let first = db.start_session(r#"{"pairs":["BTC-USDT"]}"#).await?;
        let second = db.start_session("{}").await?;
        assert!(second > first);

        db.end_session(first).await?;

        let row = sqlx::query("SELECT ended_at, config FROM stream_sessions WHERE id = ?")
            .bind(first)
            .fetch_one(&db.pool)
            .await?;
        let ended_at: Option<DateTime<Utc>> = row.try_get("ended_at")?;
        assert!(ended_at.is_some());
        assert_eq!(
            row.try_get::<String, _>("config")?,
            r#"{"pairs":["BTC-USDT"]}"#
        );

        let row = sqlx::query("SELECT ended_at FROM stream_sessions WHERE id = ?")
            .bind(second)
            .fetch_one(&db.pool)
            .await?;
        let still_open: Option<DateTime<Utc>> = row.try_get("ended_at")?;
        assert!(still_open.is_none());
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn end_session_of_unknown_id_fails() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        match db.end_session(424_242).await {
            Err(StorageError::SessionNotFound { session_id }) => assert_eq!(session_id, 424_242),
            other => panic!("expected SessionNotFound, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn events_round_trip_exactly_and_return_ascending_by_ts() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        // Sub-second precision; recorded out of ts order on purpose.
        let later = event("2026-06-12T10:00:00.123456Z", EventKind::GapDetected)?;
        let earlier = StreamEvent {
            ts: "2026-06-12T09:00:00Z".parse::<DateTime<Utc>>()?,
            exchange: None,
            pair: None,
            kind: EventKind::Connected,
            details: None,
        };
        db.record_event(session, &later).await?;
        db.record_event(session, &earlier).await?;

        let date = "2026-06-12".parse::<NaiveDate>()?;
        assert_eq!(db.events_for_date(date).await?, vec![earlier, later]);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn event_with_null_optionals_round_trips_as_none() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let bare = StreamEvent {
            ts: "2026-06-12T12:00:00Z".parse::<DateTime<Utc>>()?,
            exchange: None,
            pair: None,
            kind: EventKind::Disconnected,
            details: None,
        };
        db.record_event(session, &bare).await?;

        let read = db
            .events_for_date("2026-06-12".parse::<NaiveDate>()?)
            .await?;
        assert_eq!(read, vec![bare]);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn events_for_date_filters_by_utc_day_boundaries() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        // Boundary probes: 00:00:00 stores without a fractional part,
        // 23:59:59.999 with one — both must compare correctly against the
        // bound day endpoints.
        let last_of_prev = event("2026-06-11T23:59:59.999Z", EventKind::Error)?;
        let first_of_day = event("2026-06-12T00:00:00Z", EventKind::Connected)?;
        let last_of_day = event("2026-06-12T23:59:59.999Z", EventKind::LateTrade)?;
        let first_of_next = event("2026-06-13T00:00:00Z", EventKind::Disconnected)?;
        for ev in [&last_of_prev, &first_of_day, &last_of_day, &first_of_next] {
            db.record_event(session, ev).await?;
        }

        let day = db
            .events_for_date("2026-06-12".parse::<NaiveDate>()?)
            .await?;
        assert_eq!(day, vec![first_of_day, last_of_day]);

        let prev = db
            .events_for_date("2026-06-11".parse::<NaiveDate>()?)
            .await?;
        assert_eq!(prev, vec![last_of_prev]);

        let next = db
            .events_for_date("2026-06-13".parse::<NaiveDate>()?)
            .await?;
        assert_eq!(next, vec![first_of_next]);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn unknown_event_kind_in_database_surfaces_parse_error() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        sqlx::query("INSERT INTO stream_events (session_id, ts, kind) VALUES (?, ?, 'bogus')")
            .bind(session)
            .bind("2026-06-12T10:00:00Z".parse::<DateTime<Utc>>()?)
            .execute(&db.pool)
            .await?;

        match db.events_for_date("2026-06-12".parse::<NaiveDate>()?).await {
            Err(StorageError::UnknownEventKind { kind }) => assert_eq!(kind, "bogus"),
            other => panic!("expected UnknownEventKind, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn record_event_for_unknown_session_fails_foreign_key() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let ev = event("2026-06-12T10:00:00Z", EventKind::Connected)?;
        match db.record_event(424_242, &ev).await {
            Err(StorageError::Sqlx { operation, source }) => {
                assert_eq!(operation, "record event");
                assert!(
                    source.to_string().contains("FOREIGN KEY"),
                    "expected a FOREIGN KEY violation, got: {source}"
                );
            }
            other => panic!("expected Sqlx foreign-key error, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::panic,
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    async fn files_round_trip_exactly_and_filter_by_date() -> TestResult {
        let tmp = tempfile::tempdir()?;
        let db = open_db(&tmp).await?;
        let session = db.start_session("{}").await?;

        let on_day = finalized_file("2026-06-12")?;
        let other_day = finalized_file("2026-06-13")?;
        let before = Utc::now();
        db.record_file(session, &on_day).await?;
        db.record_file(session, &other_day).await?;
        let after = Utc::now();

        let files = db
            .files_for_date("2026-06-12".parse::<NaiveDate>()?)
            .await?;
        let [row] = files.as_slice() else {
            panic!("expected exactly one file for 2026-06-12, got {files:?}");
        };
        let expected = ArchiveFileRow {
            id: row.id,
            session_id: session,
            dataset: "trades".to_owned(),
            exchange: on_day.exchange.clone(),
            pair: on_day.pair.clone(),
            date: on_day.date,
            path: on_day.path.to_str().ok_or("non-utf8 test path")?.to_owned(),
            rows: 1234,
            ts_min: on_day.ts_min,
            ts_max: on_day.ts_max,
            closed_at: row.closed_at,
        };
        assert_eq!(*row, expected);
        assert!(
            before <= row.closed_at && row.closed_at <= after,
            "closed_at {} outside [{before}, {after}]",
            row.closed_at
        );

        let empty = db
            .files_for_date("2026-06-14".parse::<NaiveDate>()?)
            .await?;
        assert!(empty.is_empty());
        Ok(())
    }
}
