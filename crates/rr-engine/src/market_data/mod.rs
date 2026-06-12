//! Market-data ingest: candle aggregation and trade-id gap detection over
//! the public trade stream.

pub mod candle;
pub mod convert;
pub mod gap;
pub mod spec;
pub mod supervisor;
