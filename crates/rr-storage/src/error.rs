//! Storage-layer errors. Fail fast: every variant carries enough context to
//! identify the failing operation and input.

use std::path::PathBuf;

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
}
