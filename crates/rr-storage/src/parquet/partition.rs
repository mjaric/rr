//! Partition path layout for the Parquet market-data archive:
//! `<data_dir>/parquet/<dataset>/exchange=<e>/pair=<p>/date=<YYYY-MM-DD>/part-NNNN.parquet`.

use std::path::{Path, PathBuf};

use chrono::NaiveDate;

use crate::error::StorageError;

/// Directory holding the part files of one (dataset, exchange, pair, date)
/// partition, Hive-style.
#[must_use]
pub fn partition_dir(
    data_dir: &Path,
    dataset: &str,
    exchange: &str,
    pair: &str,
    date: NaiveDate,
) -> PathBuf {
    data_dir
        .join("parquet")
        .join(dataset)
        .join(format!("exchange={exchange}"))
        .join(format!("pair={pair}"))
        .join(format!("date={date}"))
}

/// File name of part `part` within a partition directory (`part-NNNN.parquet`).
#[must_use]
pub fn part_file_name(part: u32) -> String {
    format!("part-{part:04}.parquet")
}

/// Next free part number in `dir`: one past the highest existing
/// `part-NNNN.parquet`, counting quarantined `part-NNNN.parquet.corrupt`
/// files so a fresh part never collides with a future quarantine rename.
/// A missing or empty directory yields 0.
///
/// # Errors
///
/// Returns [`StorageError::Io`] if the directory exists but cannot be read.
pub fn next_part_number(dir: &Path) -> Result<u32, StorageError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(StorageError::Io {
                path: dir.to_path_buf(),
                source,
            });
        }
    };
    let mut next = 0;
    for entry in entries {
        let entry = entry.map_err(|source| StorageError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            tracing::warn!(
                file = %entry.path().display(),
                "skipping non-UTF-8 file name in partition directory"
            );
            continue;
        };
        let Some(part) = parse_part_number(name) else {
            continue;
        };
        next = next.max(part + 1);
    }
    Ok(next)
}

/// Parses `part-NNNN.parquet` (or its quarantined `.corrupt` form) to `NNNN`.
fn parse_part_number(name: &str) -> Option<u32> {
    let name = name.strip_suffix(".corrupt").unwrap_or(name);
    let digits = name.strip_prefix("part-")?.strip_suffix(".parquet")?;
    if digits.len() != 4 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::NaiveDate;

    use crate::error::StorageError;
    use crate::parquet::partition::{next_part_number, part_file_name, partition_dir};

    fn date(s: &str) -> Result<NaiveDate, chrono::ParseError> {
        s.parse::<NaiveDate>()
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?`"
    )]
    fn partition_dir_builds_hive_style_layout() -> Result<(), chrono::ParseError> {
        let dir = partition_dir(
            &PathBuf::from("/data"),
            "trades",
            "binance_spot",
            "BTC-USDT",
            date("2026-06-12")?,
        );
        assert_eq!(
            dir,
            PathBuf::from(
                "/data/parquet/trades/exchange=binance_spot/pair=BTC-USDT/date=2026-06-12"
            )
        );
        Ok(())
    }

    #[test]
    fn part_file_name_is_zero_padded() {
        assert_eq!(part_file_name(0), "part-0000.parquet");
        assert_eq!(part_file_name(42), "part-0042.parquet");
        assert_eq!(part_file_name(9999), "part-9999.parquet");
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?`"
    )]
    fn next_part_number_missing_dir_is_zero() -> Result<(), StorageError> {
        let tmp = tempfile::tempdir().map_err(|source| StorageError::Io {
            path: PathBuf::from("tempdir"),
            source,
        })?;
        assert_eq!(next_part_number(&tmp.path().join("does-not-exist"))?, 0);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?`"
    )]
    fn next_part_number_empty_dir_is_zero() -> Result<(), StorageError> {
        let tmp = tempfile::tempdir().map_err(|source| StorageError::Io {
            path: PathBuf::from("tempdir"),
            source,
        })?;
        assert_eq!(next_part_number(tmp.path())?, 0);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?`"
    )]
    fn next_part_number_continues_after_max_part() -> Result<(), std::io::Error> {
        let tmp = tempfile::tempdir()?;
        std::fs::write(tmp.path().join("part-0000.parquet"), b"x")?;
        std::fs::write(tmp.path().join("part-0003.parquet"), b"x")?;
        let next = next_part_number(tmp.path()).map_err(std::io::Error::other)?;
        assert_eq!(next, 4);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?`"
    )]
    fn next_part_number_counts_quarantined_files() -> Result<(), std::io::Error> {
        let tmp = tempfile::tempdir()?;
        std::fs::write(tmp.path().join("part-0005.parquet.corrupt"), b"x")?;
        let next = next_part_number(tmp.path()).map_err(std::io::Error::other)?;
        assert_eq!(next, 6);
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertion; Result is for `?`"
    )]
    fn next_part_number_ignores_non_matching_files() -> Result<(), std::io::Error> {
        let tmp = tempfile::tempdir()?;
        std::fs::write(tmp.path().join("notes.txt"), b"x")?;
        std::fs::write(tmp.path().join("part-12.parquet"), b"x")?;
        std::fs::write(tmp.path().join("part-abcd.parquet"), b"x")?;
        std::fs::write(tmp.path().join("part-0007.snappy"), b"x")?;
        let next = next_part_number(tmp.path()).map_err(std::io::Error::other)?;
        assert_eq!(next, 0);
        Ok(())
    }
}
