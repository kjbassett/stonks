"""Migration 004: add Exchange table and seed known exchanges.

Creates the Exchange table if absent and inserts the seven known market_id
values.  Existing rows are left untouched (INSERT OR IGNORE), so re-running is
safe.

Note: the FOREIGN KEY from Company.primary_exchange → Exchange.market_id
cannot be added to an existing table in SQLite via ALTER TABLE.  It is present
in create_db.sql for new databases only; existing DBs rely on application-level
filtering instead.

Run directly:  python src/data_access/db/migration_004.py
"""

import json
import sqlite3
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _PROJECT_ROOT / "config.json"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS Exchange (
    market_id   TEXT PRIMARY KEY,
    market_name TEXT,
    active      BIT NOT NULL DEFAULT 1
)
"""

_EXCHANGES = [
    ("XNAS", "NASDAQ", 1),
    ("XNYS", "New York Stock Exchange", 1),
    ("XASE", "NYSE American", 1),
    ("ARCX", "NYSE Arca", 1),
    ("BATS", "CBOE BZX (BATS)", 1),
    ("OTC Link", "OTC Link", 0),
    ("Grey Market", "Grey Market", 0),
]


def _db_path() -> str:
    """Return the absolute database path from config.json."""
    cfg = json.loads(_CONFIG_PATH.read_text())
    return str(Path(cfg["db_folder"]) / cfg["db_name"])


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    """Return True if ``table`` already exists in the database."""
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def main() -> None:
    """Create Exchange table and seed exchange rows."""
    db = _db_path()
    print(f"DB: {db}\n")
    con = sqlite3.connect(db)
    try:
        already_existed = _table_exists(con, "Exchange")

        con.execute(_CREATE_TABLE_SQL)
        if already_existed:
            print("  Exchange table already existed — skipping CREATE")
        else:
            print("  Created Exchange table")

        con.executemany(
            "INSERT OR IGNORE INTO Exchange (market_id, market_name, active) VALUES (?,?,?)",
            _EXCHANGES,
        )
        con.commit()

        total = con.execute("SELECT COUNT(*) FROM Exchange").fetchone()[0]
        active = con.execute("SELECT COUNT(*) FROM Exchange WHERE active = 1").fetchone()[0]
        inactive = con.execute("SELECT COUNT(*) FROM Exchange WHERE active = 0").fetchone()[0]
        print(f"\nDone. Exchange table has {total} row(s): {active} active, {inactive} inactive.")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
