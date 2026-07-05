import time
from typing import List, Optional

import pandas as pd

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS InferencePredictions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT NOT NULL,
    organism_name    TEXT NOT NULL,
    organism_version TEXT NOT NULL,
    timestamp        INTEGER NOT NULL,
    close            REAL,
    prediction       REAL,
    portfolio_weight REAL,
    created_at       INTEGER NOT NULL
)
"""


class InferencePredictions(BaseDAO):
    """DAO bridging run_inference and run_rebalance across webrock job boundaries."""

    def __init__(self, db: AsyncDatabase) -> None:
        super().__init__(db, "InferencePredictions")

    async def init2(self) -> None:
        """Create the InferencePredictions table if absent, then load column metadata."""
        await self.db.execute_query(_CREATE_TABLE, query_type="INSERT")
        await super().init2()

    async def save(
        self, df: pd.DataFrame, organism_name: str, organism_version: str
    ) -> None:
        """Replace stored predictions for the given organism with fresh results.

        Args:
            df: Recommendations DataFrame with symbol, timestamp, close, portfolio_weight.
            organism_name: Organism name that produced the predictions.
            organism_version: Organism version string.
        """
        await self.db.execute_query(
            "DELETE FROM InferencePredictions WHERE organism_name = ? AND organism_version = ?",
            (organism_name, organism_version),
            query_type="DELETE",
        )
        rows = _df_to_rows(df, organism_name, organism_version, int(time.time()))
        if rows:
            await self.insert(rows, on_conflict=None)

    async def load_latest(
        self, organism_name: str, organism_version: str
    ) -> Optional[pd.DataFrame]:
        """Load the most recent predictions for the given organism.

        Args:
            organism_name: Organism name to filter by.
            organism_version: Organism version to filter by.

        Returns:
            DataFrame with symbol, timestamp, close, prediction, portfolio_weight,
            or None if no predictions are stored.
        """
        df = await self.db.execute_query(
            """
            SELECT symbol, timestamp, close, prediction, portfolio_weight
            FROM InferencePredictions
            WHERE organism_name = ? AND organism_version = ?
            """,
            (organism_name, organism_version),
            return_type="DataFrame",
        )
        return df if not df.empty else None


def _df_to_rows(
    df: pd.DataFrame, organism_name: str, organism_version: str, now: int
) -> List[dict]:
    """Convert a recommendations DataFrame to a list of insertion dicts.

    Args:
        df: Recommendations DataFrame.
        organism_name: Organism name label.
        organism_version: Organism version label.
        now: Unix timestamp for created_at.

    Returns:
        List of dicts ready for BaseDAO.insert().
    """
    return [
        {
            "symbol": row["symbol"],
            "organism_name": organism_name,
            "organism_version": organism_version,
            "timestamp": int(row["timestamp"]),
            "close": float(row.get("close", 0.0)),
            "prediction": float(row.get("prediction", 0.0)),
            "portfolio_weight": float(row.get("portfolio_weight", 0.0)),
            "created_at": now,
        }
        for _, row in df.iterrows()
    ]
