//! Persistence: `SQLite` operational state + Parquet market-data archive (M1).

pub mod archive_task;
pub mod db;
pub mod error;
pub mod parquet;
pub mod records;
pub mod status;
