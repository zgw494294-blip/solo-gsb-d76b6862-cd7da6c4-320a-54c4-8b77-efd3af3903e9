"""Test isolation: point the history store at a fresh temp SQLite database."""

import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_TMP_DB = Path(tempfile.mkdtemp(prefix="history-tests-")) / "test.db"
os.environ["HISTORY_DB_PATH"] = str(_TMP_DB)

from app import db  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_database():
    """Recreate an empty database file before every test."""

    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db.db_path()) + suffix)
        if path.exists():
            path.unlink()
    db.init_db()
    yield
