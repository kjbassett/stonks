"""One-time migration: add Company.enabled + Company.primary_exchange, disable
ETF/OS ticker types, disable OTC-listed companies, and delete stale tickers.

Run directly:  python src/data_access/db/migration_001.py
"""

import csv
import json
import sqlite3
from pathlib import Path
from typing import List, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _PROJECT_ROOT / "config.json"
_CSV_PATH = _PROJECT_ROOT / "company_screen.csv"
_OTC_EXCHANGES = {"OTC Link", "Grey Market"}


def _db_path() -> str:
    """Return the absolute database path from config.json."""
    cfg = json.loads(_CONFIG_PATH.read_text())
    return str(Path(cfg["db_folder"]) / cfg["db_name"])


def _column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    """Return True if ``column`` already exists in ``table``."""
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def _add_columns(con: sqlite3.Connection) -> None:
    """Add primary_exchange and enabled columns to Company if absent."""
    if not _column_exists(con, "Company", "primary_exchange"):
        con.execute("ALTER TABLE Company ADD COLUMN primary_exchange TEXT")
        print("  Added Company.primary_exchange")
    else:
        print("  Company.primary_exchange already present — skipping")
    if not _column_exists(con, "Company", "enabled"):
        con.execute(
            "ALTER TABLE Company ADD COLUMN enabled BIT NOT NULL DEFAULT 1"
        )
        print("  Added Company.enabled (all rows default to 1)")
    else:
        print("  Company.enabled already present — skipping")


def _disable_ticker_types(con: sqlite3.Connection) -> None:
    """Disable ETF and OS ticker types."""
    n = con.execute(
        "UPDATE TickerType SET enabled = 0 WHERE id IN ('ETF', 'OS')"
    ).rowcount
    print(f"  Disabled {n} TickerType rows (ETF, OS)")


def _read_csv() -> Tuple[List[Tuple[str, str]], List[str], List[str]]:
    """Parse company_screen.csv into three lists.

    Returns:
        exchange_updates: (exchange, symbol) pairs for all found companies.
        otc_symbols: symbols whose exchange is OTC Link or Grey Market.
        not_found_symbols: symbols Polygon returned NOT_FOUND for.
    """
    exchange_updates, otc_symbols, not_found_symbols = [], [], []
    with open(_CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"]
            found = (row.get("found") or "True").strip()
            exchange = (row.get("primary_exchange") or "").strip()
            if found == "False":
                not_found_symbols.append(sym)
            else:
                if exchange:
                    exchange_updates.append((exchange, sym))
                if exchange in _OTC_EXCHANGES:
                    otc_symbols.append(sym)
    return exchange_updates, otc_symbols, not_found_symbols


def _populate_primary_exchange(
    con: sqlite3.Connection, updates: List[Tuple[str, str]]
) -> int:
    """Bulk-set primary_exchange for all companies that have one in the CSV.

    Args:
        con: Open database connection.
        updates: List of (exchange, symbol) pairs.

    Returns:
        Number of rows updated.
    """
    if not updates:
        return 0
    con.executemany(
        "UPDATE Company SET primary_exchange = ? WHERE symbol = ?", updates
    )
    return len(updates)


def _disable_otc_companies(con: sqlite3.Connection, symbols: List[str]) -> int:
    """Set Company.enabled = 0 for every symbol in ``symbols``.

    Args:
        con: Open database connection.
        symbols: Ticker symbols to disable.

    Returns:
        Number of rows updated.
    """
    if not symbols:
        return 0
    placeholders = ", ".join("?" * len(symbols))
    return con.execute(
        f"UPDATE Company SET enabled = 0 WHERE symbol IN ({placeholders})",
        symbols,
    ).rowcount


def _delete_stale_companies(con: sqlite3.Connection, symbols: List[str]) -> int:
    """Delete companies that Polygon no longer recognises.

    Args:
        con: Open database connection.
        symbols: Ticker symbols to remove.

    Returns:
        Number of rows deleted.
    """
    if not symbols:
        return 0
    placeholders = ", ".join("?" * len(symbols))
    return con.execute(
        f"DELETE FROM Company WHERE symbol IN ({placeholders})", symbols
    ).rowcount


def _print_summary(con: sqlite3.Connection) -> None:
    """Print final enabled/total company counts."""
    total = con.execute("SELECT COUNT(*) FROM Company").fetchone()[0]
    active = con.execute(
        "SELECT COUNT(*) FROM Company WHERE enabled = 1"
    ).fetchone()[0]
    print(f"\nDone. {active:,} of {total:,} companies remain enabled.")


def main() -> None:
    """Run all migration steps in order and commit on success."""
    print(f"DB:  {_db_path()}")
    print(f"CSV: {_CSV_PATH}\n")

    con = sqlite3.connect(_db_path())
    try:
        print("1. Adding columns to Company…")
        _add_columns(con)

        print("2. Disabling ETF and OS ticker types…")
        _disable_ticker_types(con)

        print("3. Reading company_screen.csv…")
        exchange_updates, otc_symbols, not_found_symbols = _read_csv()
        print(f"  {len(exchange_updates)} exchange records")
        print(f"  {len(otc_symbols)} OTC companies to disable")
        print(f"  {len(not_found_symbols)} stale (NOT_FOUND) companies to delete")

        print("4. Populating primary_exchange…")
        n = _populate_primary_exchange(con, exchange_updates)
        print(f"  Updated {n} rows")

        print("5. Disabling OTC companies…")
        n = _disable_otc_companies(con, otc_symbols)
        print(f"  Disabled {n} companies")

        print("6. Deleting stale companies…")
        n = _delete_stale_companies(con, not_found_symbols)
        print(f"  Deleted {n} companies")

        con.commit()
        _print_summary(con)
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
