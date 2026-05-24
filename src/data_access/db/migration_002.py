"""Migration 002: add NewsEmbedding table for pre-computed sentence-transformer embeddings.

Run directly:  python src/data_access/db/migration_002.py
"""

import json
import sqlite3
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _PROJECT_ROOT / "config.json"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS NewsEmbedding (
  news_id   TEXT NOT NULL,
  model     TEXT NOT NULL,
  embedding BLOB NOT NULL,
  PRIMARY KEY (news_id, model),
  FOREIGN KEY (news_id) REFERENCES News(id) ON DELETE CASCADE
)
"""


def _db_path() -> str:
    """Return the absolute database path from config.json."""
    cfg = json.loads(_CONFIG_PATH.read_text())
    return str(Path(cfg["db_folder"]) / cfg["db_name"])


def main() -> None:
    """Create the NewsEmbedding table if it does not already exist."""
    db = _db_path()
    print(f"DB: {db}")
    con = sqlite3.connect(db)
    try:
        con.execute(_CREATE_TABLE_SQL)
        con.commit()
        count = con.execute("SELECT COUNT(*) FROM NewsEmbedding").fetchone()[0]
        print(f"Done. NewsEmbedding table ready ({count:,} rows).")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
