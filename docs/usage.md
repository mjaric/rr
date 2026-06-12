# Operating the market data ingest (M1)

How to record market data with `rr stream` and check it with `rr archive-status`.
Build the binary first: `cargo build --release` (or use `./target/debug/rr` after
`cargo build`).

## `rr stream` — record live market data

```sh
rr stream [--data-dir ./data]
```

Connects to Binance (BTC/ETH-USDT) and Coinbase (BTC/ETH-USD) public-trade
WebSockets, derives 1-minute candles locally, and archives both to the `data-dir`.
It runs in the foreground until you stop it with **Ctrl-C**. The `data-dir` (default
`./data`) holds `rr.sqlite` (operational metadata) and a `parquet/` tree of the
archived trades and candles.

**Heartbeat.** Individual trades are not logged — that would be thousands of lines a
minute. Instead, one `INFO` line prints every 30 seconds:

```
ingest heartbeat trades_total=1571 candles_total=4 trades_since_last=1289 \
                 candles_since_last=0 last_trade="BTC-USD@63495.05"
```

- `trades_since_last` > 0 each interval means data is actively flowing. A `0` is the
  signal that nothing is arriving (check the network, or the logs above for
  `disconnected` / `error` lines).
- `trades_total` / `candles_total` are running totals for the session.

**Stop it cleanly.** Ctrl-C (or `kill -INT <pid>`) runs a graceful shutdown: it
flushes open candle windows, `fsync`-closes every Parquet file, and marks the session
ended. Do **not** use `kill -9` — it skips the flush and leaves the last file
unfinished (it is quarantined to `*.corrupt` on the next start, and its data is lost).

**Run it overnight.** Detach it from the terminal:

```sh
nohup rr stream --data-dir ./data > stream.log 2>&1 &
echo $! > stream.pid           # remember the PID
# ... let it run overnight ...
kill -INT "$(cat stream.pid)"  # stop cleanly in the morning
```

(`tmux`/`screen` work too.) There is no service-manager integration yet; `rr stream`
handles SIGINT only, so avoid `systemctl`/`docker stop` (SIGTERM) for now — they would
hard-kill it and truncate the final Parquet part.

**When is data queryable?** A Parquet file only becomes visible to `archive-status`
once it is *finalized* — which happens on a roll (every 15 minutes) or on a clean
Ctrl-C. During the first 15 minutes of a fresh run, the data is buffered in an open
file with no footer yet, so `archive-status` will show nothing until the first roll or
until you stop the stream.

## `rr archive-status` — check coverage

```sh
rr archive-status [--date YYYY-MM-DD] [--data-dir ./data]
```

Reports, per (exchange, pair), how completely the day's 1-minute candles were
captured. `--date` defaults to **today (UTC)**; dates are always UTC, and a trade
belongs to the UTC day of its exchange timestamp.

```
archive coverage for 2026-06-12
EXCHANGE      PAIR      FILES  TRADES  COVERAGE  QUIET  GAP   GAP_EVT  LATE_EVT  STATUS
binance_spot  BTC-USDT  2      1320    2/1370    0      1368  0        0         GAPS:1368
```

| Column | Meaning |
|--------|---------|
| `EXCHANGE` / `PAIR` | The market. |
| `FILES` | Registered Parquet files for the pair that day (trades + candles). More than two means the 15-minute roll produced extra `part-*` files. |
| `TRADES` | Total trade rows archived for the pair that day. |
| `COVERAGE` | `present / expected` candle **minutes**. `expected` is the number of fully elapsed minutes of the day so far (1440 for a past day); `present` is how many have a candle in the archive. |
| `QUIET` | Missing minutes during which the stream **was connected** but the market simply had no trades. Not a problem. |
| `GAP` | Missing minutes that overlap a period when the stream **was not connected or not running**. This is where data may be lost. |
| `GAP_EVT` | Count of trade-id sequence jumps detected *within* the captured data (a skipped trade). |
| `LATE_EVT` | Count of trades that arrived after their candle minute had already been finalized. |
| `STATUS` | `OK` when `GAP == 0`, otherwise `GAPS:<n>`. |

### Reading `GAP` correctly

`GAP` counts minutes the process **was not capturing**, which includes all the time
before you started the stream. So a short run queried against a whole day shows a huge
`GAP` (e.g. `GAPS:1368` after a one-minute run) — that is **not data loss**, it just
means the stream was not running for those minutes.

The distinction that matters:

- **`QUIET`** — connected, market silent → expected, fine.
- **`GAP`** — not connected → potential data loss; this is what "no gaps" refers to.

`GAP_EVT` / `LATE_EVT` are finer anomalies *within* the data you did capture; both
should normally be `0`. Every anomaly behind these counts is also a row in the
`stream_events` table and a `tracing` line in the stream's log.

## Overnight verification (the M1 "done" criterion)

M1 is "done" when an unattended overnight run produces gap-free coverage. The recipe:

1. Start `rr stream` overnight (see above) and let it run the full day.
2. In the morning, stop it cleanly, then:
   ```sh
   rr archive-status --date <yesterday-UTC>
   ```
3. Expect, per pair, roughly `COVERAGE 1438/1440`, `GAP 0` (or only minutes that line
   up with a real `disconnected`/`error` in the log), and `STATUS OK`. Investigate any
   `GAP > 0` or `GAP_EVT > 0` against the stream log and the `stream_events` table.
