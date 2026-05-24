"""Migration 003: add StockSplit table to track processed corporate actions.

Run directly:  python src/data_access/db/migration_003.py
"""

import json
import sqlite3
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _PROJECT_ROOT / "config.json"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS StockSplit (
  ticker          TEXT NOT NULL,
  execution_date  TEXT NOT NULL,
  split_from      REAL NOT NULL,
  split_to        REAL NOT NULL,
  PRIMARY KEY (ticker, execution_date)
)
"""


def _db_path() -> str:
    """Return the absolute database path from config.json."""
    cfg = json.loads(_CONFIG_PATH.read_text())
    return str(Path(cfg["db_folder"]) / cfg["db_name"])


def main() -> None:
    """Create the StockSplit table if it does not already exist."""
    db = _db_path()
    print(f"DB: {db}")
    con = sqlite3.connect(db)
    try:
        con.execute(_CREATE_TABLE_SQL)
        con.commit()
        count = con.execute("SELECT COUNT(*) FROM StockSplit").fetchone()[0]
        print(f"Done. StockSplit table ready ({count:,} rows).")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
