"""SQLite-backed persistence for reconciled event histories.

A history stores its ordered ``replica_ids`` and every merged event keyed by
``(history_id, replica_id, seq)``.  Connections are short lived and opened
per request; write transactions use ``BEGIN IMMEDIATE`` so that concurrent
writers are serialized by SQLite's reserved lock instead of racing.  Combined
with the per-history event primary key this gives the all-or-nothing,
no-lost-event / no-double-seq guarantee under concurrency:

* a writer takes the reserved lock before reading the current state, so the
  versions/seqs it validates against are still current at commit time;
* either the whole statement's writes commit or none do (single transaction);
* a duplicate ``(replica_id, seq)`` insert fails the transaction.

The database file location comes from the ``HISTORY_DB_PATH`` environment
variable (default ``data/history.db``); Docker Compose mounts a named volume
at that path so histories survive container restarts.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

DEFAULT_DB_PATH = "data/history.db"
BUSY_TIMEOUT_MS = 15_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS histories (
    history_id TEXT PRIMARY KEY,
    replicas   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    history_id TEXT NOT NULL,
    replica_id TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    key        TEXT NOT NULL,
    -- put events carry the value (the empty string is a legitimate value);
    -- delete events store NULL.
    put_value  TEXT,
    clock      TEXT NOT NULL,
    PRIMARY KEY (history_id, replica_id, seq),
    FOREIGN KEY (history_id) REFERENCES histories(history_id)
) WITHOUT ROWID;
"""


def db_path() -> str:
    return os.environ.get("HISTORY_DB_PATH", DEFAULT_DB_PATH)


def _connect() -> sqlite3.Connection:
    path = db_path()
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # WAL lets readers keep a consistent snapshot while a writer is committed;
    # busy_timeout makes lock contention wait rather than error immediately.
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables (idempotent) and switch the file to WAL journaling."""

    conn = _connect()
    try:
        if db_path() != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_SCHEMA)
    finally:
        conn.close()


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """Serialised read/write transaction (``BEGIN IMMEDIATE``).

    The reserved lock is acquired up front, so every row read inside the
    transaction is still the committed state when this transaction commits.
    """

    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


@contextmanager
def read_tx() -> Iterator[sqlite3.Connection]:
    """Read-only snapshot transaction (deferred ``BEGIN``)."""

    conn = _connect()
    try:
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def _event_row_to_wire(row: sqlite3.Row) -> dict[str, Any]:
    out: dict[str, Any] = {
        "replica_id": row["replica_id"],
        "seq": row["seq"],
        "key": row["key"],
    }
    if row["put_value"] is not None:
        out["put"] = row["put_value"]
    else:
        out["delete"] = "delete"
    out["clock"] = json.loads(row["clock"])
    return out


def insert_history(
    conn: sqlite3.Connection,
    history_id: str,
    replica_ids: list[str],
    events: list[Any],
) -> None:
    conn.execute(
        "INSERT INTO histories (history_id, replicas) VALUES (?, ?)",
        (history_id, json.dumps(replica_ids)),
    )
    insert_events(conn, history_id, events)


def insert_events(
    conn: sqlite3.Connection, history_id: str, events: list[Any]
) -> None:
    conn.executemany(
        "INSERT INTO events "
        "(history_id, replica_id, seq, key, put_value, clock) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                history_id,
                ev.replica_id,
                ev.seq,
                ev.key,
                ev.put,
                json.dumps(ev.clock),
            )
            for ev in events
        ],
    )


def load_history(
    conn: sqlite3.Connection, history_id: str
) -> Optional[dict[str, Any]]:
    """Return replicas + events sorted by (replica_id, seq), or ``None``.

    Runs inside the caller's transaction so the verdict computed from the
    returned events is derived from the same committed snapshot.
    """

    row = conn.execute(
        "SELECT replicas FROM histories WHERE history_id = ?",
        (history_id,),
    ).fetchone()
    if row is None:
        return None

    event_rows = conn.execute(
        "SELECT replica_id, seq, key, put_value, clock "
        "FROM events WHERE history_id = ? "
        "ORDER BY replica_id ASC, seq ASC",
        (history_id,),
    ).fetchall()

    return {
        "replica_ids": json.loads(row["replicas"]),
        "events": [_event_row_to_wire(r) for r in event_rows],
    }
