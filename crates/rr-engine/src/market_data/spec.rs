//! The fixed M1 subscription universe: which exchange/pair trade streams the
//! ingest supervisor subscribes to, and the canonical string conventions
//! (`snake_case` exchange ids, uppercase `BASE-QUOTE` pairs) shared with the
//! archive and the operational database.

use std::collections::BTreeSet;

/// One subscribed market: exchange id plus base/quote assets in barter's
/// lowercase convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PairSpec {
    /// Exchange identifier, `snake_case` (matches `ExchangeId::as_str`).
    pub exchange: &'static str,
    /// Base asset, lowercase (barter subscription convention).
    pub base: &'static str,
    /// Quote asset, lowercase (barter subscription convention).
    pub quote: &'static str,
}

/// The four M1 public-trade subscriptions (compile-time scope per design).
pub const PAIRS: [PairSpec; 4] = [
    PairSpec {
        exchange: "binance_spot",
        base: "btc",
        quote: "usdt",
    },
    PairSpec {
        exchange: "binance_spot",
        base: "eth",
        quote: "usdt",
    },
    PairSpec {
        exchange: "coinbase",
        base: "btc",
        quote: "usd",
    },
    PairSpec {
        exchange: "coinbase",
        base: "eth",
        quote: "usd",
    },
];

/// Canonical pair string: uppercase dash-separated, e.g. `BTC-USDT`.
#[must_use]
pub fn pair_string(base: &str, quote: &str) -> String {
    format!("{}-{}", base.to_uppercase(), quote.to_uppercase())
}

/// Distinct exchanges in [`PAIRS`], ascending.
#[must_use]
pub fn exchanges() -> BTreeSet<&'static str> {
    let mut set = BTreeSet::new();
    for pair in &PAIRS {
        set.insert(pair.exchange);
    }
    set
}

/// [`PAIRS`] serialized as the session config JSON stored by
/// `Db::start_session`: `{"pairs":[{"exchange":..,"base":..,"quote":..},..]}`.
#[must_use]
pub fn config_json() -> String {
    let mut pairs = Vec::with_capacity(PAIRS.len());
    for pair in &PAIRS {
        pairs.push(serde_json::json!({
            "exchange": pair.exchange,
            "base": pair.base,
            "quote": pair.quote,
        }));
    }
    serde_json::json!({ "pairs": pairs }).to_string()
}

#[cfg(test)]
mod tests {
    use crate::market_data::spec::{PAIRS, PairSpec, config_json, exchanges, pair_string};

    #[test]
    fn pair_string_is_uppercase_dash_separated() {
        assert_eq!(pair_string("btc", "usdt"), "BTC-USDT");
        assert_eq!(pair_string("eth", "usd"), "ETH-USD");
        // Already-uppercase input is preserved, not double-transformed.
        assert_eq!(pair_string("BTC", "USDT"), "BTC-USDT");
    }

    #[test]
    fn pairs_are_the_four_m1_subscriptions() {
        let expected = [
            ("binance_spot", "btc", "usdt"),
            ("binance_spot", "eth", "usdt"),
            ("coinbase", "btc", "usd"),
            ("coinbase", "eth", "usd"),
        ];
        let actual: Vec<(&str, &str, &str)> = PAIRS
            .iter()
            .map(
                |PairSpec {
                     exchange,
                     base,
                     quote,
                 }| (*exchange, *base, *quote),
            )
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn exchanges_are_the_distinct_pair_exchanges() {
        let actual: Vec<&str> = exchanges().into_iter().collect();
        assert_eq!(actual, vec!["binance_spot", "coinbase"]);
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions; Result is for `?`"
    )]
    fn config_json_round_trips_pairs() -> Result<(), Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(&config_json())?;
        let pairs = value["pairs"].as_array().ok_or("pairs not an array")?;
        assert_eq!(pairs.len(), 4);
        assert_eq!(pairs[0]["exchange"], "binance_spot");
        assert_eq!(pairs[0]["base"], "btc");
        assert_eq!(pairs[0]["quote"], "usdt");
        assert_eq!(pairs[3]["exchange"], "coinbase");
        assert_eq!(pairs[3]["base"], "eth");
        assert_eq!(pairs[3]["quote"], "usd");
        Ok(())
    }
}
