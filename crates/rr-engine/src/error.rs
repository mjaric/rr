//! Engine-level errors. Every variant is fatal for the running session:
//! the supervisor restarts rather than continue without persistence.

use rr_storage::error::StorageError;

/// Fatal errors raised by the engine's ingest path.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// An operational-database write failed; losing observability silently
    /// is worse than restarting.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// An archive channel receiver is gone — the writer task died, so
    /// records would be lost if ingest continued.
    #[error("archive channel for dataset `{dataset}` closed; writer task is gone")]
    ArchiveChannelClosed {
        /// Dataset whose channel closed (`trades` / `candles`).
        dataset: &'static str,
    },
    /// The merged market stream yielded `None`; barter streams reconnect
    /// internally and should never end.
    #[error("market data stream ended unexpectedly")]
    StreamEnded,
    /// Subscribing to the exchanges failed during stream construction.
    #[error("market data stream initialisation failed: {0}")]
    StreamInit(#[from] barter_data::error::DataError),
}
