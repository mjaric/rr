//! Pure per-key trade-id gap detection over `(exchange, pair)` streams.
//!
//! Binance Spot and Coinbase emit strictly consecutive (+1) per-pair u64
//! trade IDs, so any deviation is an anomaly worth recording. The detector
//! holds no clock and performs no IO: the caller drives it with `observe`
//! per trade and turns non-`Ok` results into stream events.

use std::collections::BTreeMap;

/// Result of observing one trade ID for a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapCheck {
    /// ID is the expected successor (or the first one seen for the key).
    Ok,
    /// ID jumped past the expected successor: trades were missed.
    Gap { expected: u64, got: u64 },
    /// ID is at or below the last one seen (exchange reset, snapshot
    /// replay, or duplicate — exchanges must not repeat IDs).
    Regression { last: u64, got: u64 },
    /// ID didn't parse as u64; this key degrades to disconnect-tracking only.
    NonSequential,
    /// Key previously degraded; no checking performed.
    Untracked,
}

/// Sequencing state for one `(exchange, pair)` key.
#[derive(Debug)]
enum KeyState {
    /// IDs parse as u64; `last` is the most recent one seen.
    Tracking { last: u64 },
    /// A non-numeric ID was seen once; the key is never checked again.
    Degraded,
}

/// Per-key trade-id sequence checker.
///
/// Keys are nested `exchange → pair` so steady-state lookups borrow the
/// caller's `&str`s; owned keys are allocated only on first observation.
#[derive(Debug, Default)]
pub struct GapDetector {
    keys: BTreeMap<String, BTreeMap<String, KeyState>>,
}

impl GapDetector {
    /// Checks one trade ID against the key's last seen ID.
    ///
    /// The first observation for a key starts tracking and returns `Ok`.
    /// After a `Gap` or `Regression` the detector resumes from the new ID,
    /// so each anomaly is reported once. A non-numeric ID degrades the key
    /// permanently: `NonSequential` once, then `Untracked` forever.
    pub fn observe(&mut self, exchange: &str, pair: &str, trade_id: &str) -> GapCheck {
        let Some(state) = self
            .keys
            .get_mut(exchange)
            .and_then(|pairs| pairs.get_mut(pair))
        else {
            let (state, check) = match trade_id.parse::<u64>() {
                Ok(id) => (KeyState::Tracking { last: id }, GapCheck::Ok),
                Err(_) => (KeyState::Degraded, GapCheck::NonSequential),
            };
            self.keys
                .entry(exchange.to_owned())
                .or_default()
                .insert(pair.to_owned(), state);
            return check;
        };
        match state {
            KeyState::Degraded => GapCheck::Untracked,
            KeyState::Tracking { last } => {
                let Ok(got) = trade_id.parse::<u64>() else {
                    *state = KeyState::Degraded;
                    return GapCheck::NonSequential;
                };
                let prev = std::mem::replace(last, got);
                check_sequence(prev, got)
            }
        }
    }
}

/// Compares `got` against the successor of `prev`.
///
/// `checked_add` keeps `prev == u64::MAX` panic-free: there is no valid
/// successor, and since every u64 `got` is then at or below `prev`, the
/// observation is a regression by definition.
fn check_sequence(prev: u64, got: u64) -> GapCheck {
    let Some(expected) = prev.checked_add(1) else {
        return GapCheck::Regression { last: prev, got };
    };
    match got.cmp(&expected) {
        std::cmp::Ordering::Equal => GapCheck::Ok,
        std::cmp::Ordering::Greater => GapCheck::Gap { expected, got },
        // `got <= prev` covers duplicates too: exchanges must not repeat IDs.
        std::cmp::Ordering::Less => GapCheck::Regression { last: prev, got },
    }
}

#[cfg(test)]
mod tests {
    use crate::market_data::gap::{GapCheck, GapDetector};

    fn observe(detector: &mut GapDetector, id: &str) -> GapCheck {
        detector.observe("binance_spot", "BTC-USDT", id)
    }

    #[test]
    fn first_observation_is_ok() {
        let mut detector = GapDetector::default();
        assert_eq!(observe(&mut detector, "5"), GapCheck::Ok);
    }

    #[test]
    fn consecutive_ids_are_ok() {
        let mut detector = GapDetector::default();
        assert_eq!(observe(&mut detector, "5"), GapCheck::Ok);
        assert_eq!(observe(&mut detector, "6"), GapCheck::Ok);
        assert_eq!(observe(&mut detector, "7"), GapCheck::Ok);
    }

    #[test]
    fn jump_reports_gap_with_expected_and_got() {
        let mut detector = GapDetector::default();
        assert_eq!(observe(&mut detector, "5"), GapCheck::Ok);
        assert_eq!(
            observe(&mut detector, "8"),
            GapCheck::Gap {
                expected: 6,
                got: 8
            }
        );
    }

    #[test]
    fn tracking_resumes_after_gap() {
        let mut detector = GapDetector::default();
        observe(&mut detector, "5");
        observe(&mut detector, "8");
        // The gap was reported once; tracking continues from 8.
        assert_eq!(observe(&mut detector, "9"), GapCheck::Ok);
    }

    #[test]
    fn lower_id_reports_regression() {
        let mut detector = GapDetector::default();
        observe(&mut detector, "10");
        assert_eq!(
            observe(&mut detector, "3"),
            GapCheck::Regression { last: 10, got: 3 }
        );
    }

    #[test]
    fn tracking_resumes_after_regression() {
        let mut detector = GapDetector::default();
        observe(&mut detector, "10");
        observe(&mut detector, "3");
        // Reported once; tracking continues from the new ID.
        assert_eq!(observe(&mut detector, "4"), GapCheck::Ok);
    }

    #[test]
    fn duplicate_id_is_a_regression() {
        // Exchanges must not repeat IDs; got == last is a regression-class
        // anomaly, reported with last == got so it's identifiable.
        let mut detector = GapDetector::default();
        observe(&mut detector, "7");
        assert_eq!(
            observe(&mut detector, "7"),
            GapCheck::Regression { last: 7, got: 7 }
        );
        assert_eq!(observe(&mut detector, "8"), GapCheck::Ok);
    }

    #[test]
    fn non_numeric_id_degrades_key_permanently() {
        let mut detector = GapDetector::default();
        observe(&mut detector, "5");
        assert_eq!(observe(&mut detector, "abc-123"), GapCheck::NonSequential);
        // Degraded for good: even numeric IDs are no longer checked.
        assert_eq!(observe(&mut detector, "6"), GapCheck::Untracked);
        assert_eq!(observe(&mut detector, "xyz"), GapCheck::Untracked);
        assert_eq!(observe(&mut detector, "1"), GapCheck::Untracked);
    }

    #[test]
    fn non_numeric_first_observation_degrades_key() {
        let mut detector = GapDetector::default();
        assert_eq!(observe(&mut detector, "abc"), GapCheck::NonSequential);
        assert_eq!(observe(&mut detector, "1"), GapCheck::Untracked);
    }

    #[test]
    fn keys_are_independent_across_pairs_and_exchanges() {
        let mut detector = GapDetector::default();
        assert_eq!(
            detector.observe("binance_spot", "BTC-USDT", "5"),
            GapCheck::Ok
        );
        assert_eq!(
            detector.observe("binance_spot", "ETH-USDT", "100"),
            GapCheck::Ok
        );
        assert_eq!(
            detector.observe("coinbase", "BTC-USDT", "900"),
            GapCheck::Ok
        );

        // A gap on one key leaves the others tracking normally.
        assert_eq!(
            detector.observe("binance_spot", "BTC-USDT", "9"),
            GapCheck::Gap {
                expected: 6,
                got: 9
            }
        );
        assert_eq!(
            detector.observe("binance_spot", "ETH-USDT", "101"),
            GapCheck::Ok
        );
        assert_eq!(
            detector.observe("coinbase", "BTC-USDT", "901"),
            GapCheck::Ok
        );

        // Degrading one key leaves the others tracking normally.
        assert_eq!(
            detector.observe("coinbase", "BTC-USDT", "n/a"),
            GapCheck::NonSequential
        );
        assert_eq!(
            detector.observe("binance_spot", "BTC-USDT", "10"),
            GapCheck::Ok
        );
        assert_eq!(
            detector.observe("coinbase", "BTC-USDT", "902"),
            GapCheck::Untracked
        );
    }

    #[test]
    fn max_id_then_any_id_is_regression_without_panicking() {
        // With last == u64::MAX there is no valid successor: every further
        // u64 ID is at or below it, so it is a regression by definition and
        // `expected = last + 1` must not be computed.
        let max = u64::MAX.to_string();
        let mut detector = GapDetector::default();
        assert_eq!(observe(&mut detector, &max), GapCheck::Ok);
        assert_eq!(
            observe(&mut detector, &max),
            GapCheck::Regression {
                last: u64::MAX,
                got: u64::MAX
            }
        );
        assert_eq!(
            observe(&mut detector, "0"),
            GapCheck::Regression {
                last: u64::MAX,
                got: 0
            }
        );
        // Tracking resumed from 0.
        assert_eq!(observe(&mut detector, "1"), GapCheck::Ok);
    }
}
