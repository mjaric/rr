//! Storage-layer errors. Fail fast: every variant carries enough context to
//! identify the failing operation and input.

use std::path::PathBuf;

use chrono::NaiveDate;
use rust_decimal::Decimal;

/// Errors raised by the storage layer (Parquet archive, `SQLite`, filesystem).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A filesystem operation failed; `path` is the file or directory involved.
    #[error("io error at {path}: {source}")]
    Io {
        /// File or directory the failing operation touched.
        path: PathBuf,
        /// Underlying io error.
        #[source]
        source: std::io::Error,
    },
    /// Arrow schema or array construction failed.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet file writing or reading failed.
    #[error("parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),
    /// A decimal has more than 18 fractional digits; the archive stores
    /// `Decimal128(38, 18)` and never rounds.
    #[error("decimal {value} has {scale} fractional digits; archive stores at most 18")]
    DecimalPrecision {
        /// The offending value, unmodified.
        value: Decimal,
        /// Its (normalized) number of fractional digits.
        scale: u32,
    },
    /// A decimal's magnitude does not fit `Decimal128(38, 18)`.
    #[error("decimal {value} does not fit Decimal128(38, 18)")]
    DecimalOverflow {
        /// The offending value, unmodified.
        value: Decimal,
    },
    /// A `SQLite` operation failed; `operation` names the repository call.
    #[error("sqlite operation `{operation}` failed: {source}")]
    Sqlx {
        /// The repository operation that failed (e.g. `record event`).
        operation: &'static str,
        /// Underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },
    /// `stream_events.kind` held a string [`crate::db::EventKind::parse`]
    /// does not recognize — the database was written by something newer
    /// (or corrupted); never silently skipped.
    #[error("unknown stream event kind {kind:?} in database")]
    UnknownEventKind {
        /// The unrecognized kind string, unmodified.
        kind: String,
    },
    /// `end_session` targeted a session id that does not exist.
    #[error("stream session {session_id} not found")]
    SessionNotFound {
        /// The id that matched no `stream_sessions` row.
        session_id: i64,
    },
    /// An archive file path is not valid UTF-8 and cannot be stored as TEXT.
    #[error("archive file path {path} is not valid UTF-8")]
    NonUtf8Path {
        /// The offending path, unmodified.
        path: PathBuf,
    },
    /// A row count exceeds `SQLite`'s signed 64-bit INTEGER range.
    #[error("row count {rows} does not fit a sqlite INTEGER")]
    RowCountOverflow {
        /// The offending row count, unmodified.
        rows: u64,
    },
    /// A day-query date has no successor (`NaiveDate::MAX`), so the
    /// half-open day range cannot be built.
    #[error("date {date} is out of range for day queries")]
    DateOutOfRange {
        /// The offending date, unmodified.
        date: NaiveDate,
    },
    /// An archive writer thread panicked; its join surfaced no `StorageError`
    /// but the thread did not finish normally. Treated as process-fatal.
    #[error("archive writer thread for {dataset} dataset panicked")]
    ArchiveThreadPanicked {
        /// Dataset whose thread panicked (`trades` / `candles`).
        dataset: &'static str,
    },
}
