import time
from typing import List

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS Watchlist (
    company_id  INTEGER PRIMARY KEY,
    avg_volume  REAL NOT NULL,
    updated_at  INTEGER NOT NULL,
    FOREIGN KEY (company_id) REFERENCES Company(id)
)
"""


class Watchlist(BaseDAO):
    """DAO for the dynamic watchlist of symbols to stream and trade."""

    def __init__(self, db: AsyncDatabase) -> None:
        super().__init__(db, "Watchlist")

    async def init2(self) -> None:
        """Create the Watchlist table if absent, then load column metadata."""
        await self.db.execute_query(_CREATE_TABLE, query_type="INSERT")
        await super().init2()

    async def save_symbols(self, rows: List[dict]) -> None:
        """Replace all watchlist entries with a fresh set.

        Args:
            rows: List of dicts with keys ``company_id`` and ``avg_volume``.
        """
        now = int(time.time())
        for row in rows:
            row["updated_at"] = now
        await self.db.execute_query("DELETE FROM Watchlist", query_type="DELETE")
        if rows:
            await self.insert(rows, on_conflict=None)

    async def get_symbols(self) -> List[str]:
        """Return current watchlist tickers ordered by descending average volume.

        Returns:
            List of ticker symbols.
        """
        rows = await self.db.execute_query(
            """
            SELECT c.symbol FROM Watchlist w
            JOIN Company c ON c.id = w.company_id
            ORDER BY w.avg_volume DESC
            """,
            return_type="list",
        )
        return [row[0] for row in rows]
