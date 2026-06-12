//! Storage-layer errors. Fail fast: every variant carries enough context to
//! identify the failing operation and input.

use std::path::PathBuf;

use rust_decimal::Decimal;

/// Errors raised by the storage layer (Parquet archive, filesystem).
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
}
