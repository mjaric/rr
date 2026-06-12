-- Schema v1: stream sessions, stream events, archive-file registry.
--
-- Timestamps (started_at, ended_at, ts, ts_min, ts_max, closed_at) are
-- RFC3339 TEXT in UTC with a fixed +00:00 offset, written by sqlx's chrono
-- encoder; with a fixed offset the encoding sorts lexicographically in
-- timestamp order. `date` columns are YYYY-MM-DD TEXT.
--
-- "rows" is double-quoted because ROWS is a reserved word in standard SQL
-- (and PostgreSQL, the upgrade path); SQLite accepts the quoted form too.
--
-- `INTEGER PRIMARY KEY AUTOINCREMENT` is the single deliberate SQLite-specific
-- construct here. The PostgreSQL upgrade path uses its own identity/serial
-- columns for these surrogate keys; migrations are per-database anyway, so this
-- file is never replayed against Postgres. Every other construct is portable.

CREATE TABLE stream_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    config TEXT NOT NULL
);

CREATE TABLE stream_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES stream_sessions(id),
    ts TEXT NOT NULL,
    exchange TEXT,
    pair TEXT,
    kind TEXT NOT NULL,
    details TEXT
);

CREATE TABLE archive_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES stream_sessions(id),
    dataset TEXT NOT NULL,
    exchange TEXT NOT NULL,
    pair TEXT NOT NULL,
    date TEXT NOT NULL,
    path TEXT NOT NULL,
    "rows" INTEGER NOT NULL,
    ts_min TEXT NOT NULL,
    ts_max TEXT NOT NULL,
    closed_at TEXT NOT NULL
);

CREATE INDEX idx_stream_events_session ON stream_events(session_id, ts);
CREATE INDEX idx_archive_files_date ON archive_files(date, exchange, pair);
