import time
from typing import List, Optional

import pandas as pd

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS Prediction (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id         INTEGER NOT NULL REFERENCES Model(id),
    symbol           TEXT NOT NULL,
    timestamp        INTEGER NOT NULL,
    close            REAL,
    prediction       REAL,
    variance         REAL,
    portfolio_weight REAL,
    created_at       INTEGER NOT NULL
)
"""


class Prediction(BaseDAO):
    """DAO bridging run_inference and run_rebalance across webrock job boundaries.

    Predictions link to their producing organism version via model_id (a
    Model.id foreign key) rather than duplicating organism_name/organism_version
    strings on every row. Every row from one save() call shares the same
    created_at, so a "batch" is identifiable without a separate run id.
    """

    def __init__(self, db: AsyncDatabase) -> None:
        super().__init__(db, "Prediction")

    async def init2(self) -> None:
        """Create the Prediction table if absent, dropping a stale pre-migration schema first."""
        if await self.table_exists():
            columns = await self.db.execute_query("PRAGMA table_info(Prediction)", query_type="SELECT")
            column_names = {row[1] for row in columns}
            # Both the old dead schema (model_id, company_id, prediction, timestamp)
            # and the new one have a "model_id" column with different meaning — check
            # for "symbol" instead, which only the new schema has.
            if "symbol" not in column_names:
                await self.db.execute_query("DROP TABLE Prediction", query_type="DELETE")
            elif "variance" not in column_names:
                # Additive: preserves existing rows, just adds the new column.
                await self.db.execute_query(
                    "ALTER TABLE Prediction ADD COLUMN variance REAL", query_type="INSERT"
                )
        await self.db.execute_query(_CREATE_TABLE, query_type="INSERT")
        await super().init2()

    async def save(self, df: pd.DataFrame, model_id: int) -> None:
        """Append this run's predictions for the given model as a new batch.

        Older batches are retained (not deleted) so predictions can later be
        compared against actual outcomes; see clean_data() for retention.

        Args:
            df: Recommendations DataFrame with symbol, timestamp, close,
                prediction, variance, portfolio_weight.
            model_id: Model.id of the organism version that produced the predictions.
        """
        rows = _df_to_rows(df, model_id, int(time.time()))
        if rows:
            await self.insert(rows, on_conflict=None)

    async def load_latest(self, model_id: int) -> Optional[pd.DataFrame]:
        """Load only the most recent prediction batch for the given model.

        Args:
            model_id: Model.id of the organism version to filter by.

        Returns:
            DataFrame with symbol, timestamp, close, prediction, variance,
            portfolio_weight, or None if no predictions are stored.
        """
        df = await self.db.execute_query(
            """
            SELECT symbol, timestamp, close, prediction, variance, portfolio_weight
            FROM Prediction
            WHERE model_id = ?
              AND created_at = (SELECT MAX(created_at) FROM Prediction WHERE model_id = ?)
            """,
            (model_id, model_id),
            return_type="DataFrame",
        )
        return df if not df.empty else None

    async def clean_data(self, min_timestamp: int) -> None:
        """Delete prediction batches saved before min_timestamp.

        Picked up automatically by DAOManager.clean_data() (the clean_data
        webrock plugin), same as TradingData/TradingDataAggregation/News.

        Args:
            min_timestamp: Unix timestamp cutoff; older prediction batches are removed.
        """
        await self.db.execute_query(
            "DELETE FROM Prediction WHERE created_at < ?",
            (min_timestamp,),
            query_type="DELETE",
        )


def _df_to_rows(df: pd.DataFrame, model_id: int, now: int) -> List[dict]:
    """Convert a recommendations DataFrame to a list of insertion dicts.

    Args:
        df: Recommendations DataFrame.
        model_id: Model.id label.
        now: Unix timestamp for created_at.

    Returns:
        List of dicts ready for BaseDAO.insert().
    """
    return [
        {
            "symbol": row["symbol"],
            "model_id": model_id,
            "timestamp": int(row["timestamp"]),
            "close": float(row.get("close", 0.0)),
            "prediction": float(row.get("prediction", 0.0)),
            "variance": float(row.get("variance", 0.0)),
            "portfolio_weight": float(row.get("portfolio_weight", 0.0)),
            "created_at": now,
        }
        for _, row in df.iterrows()
    ]
