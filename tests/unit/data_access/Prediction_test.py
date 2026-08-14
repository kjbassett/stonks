"""Unit tests for the Prediction DAO: history retention, variance, and cleanup."""

import os
import tempfile
import time
import unittest

import pandas as pd

from src.data_access.db.async_database import AsyncDatabase
from src.data_access.Prediction import Prediction


def _make_recommendations(symbols, prediction=0.01, variance=0.02, weight=0.1):
    return pd.DataFrame([
        {
            "symbol": s,
            "timestamp": 1700000000,
            "close": 100.0,
            "prediction": prediction,
            "variance": variance,
            "portfolio_weight": weight,
        }
        for s in symbols
    ])


class _TempDbTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(self._tmpdir.name, "test.db")
        self.db = AsyncDatabase(db_path, read_pool_size=1)
        self.dao = Prediction(self.db)
        await self.dao.init2()

    async def asyncTearDown(self):
        await self.db.close()
        self._tmpdir.cleanup()


class TestSaveAndLoadLatest(_TempDbTestCase):
    async def test_save_then_load_latest_round_trips_variance(self):
        await self.dao.save(_make_recommendations(["AAPL"], variance=0.0345), model_id=1)
        result = await self.dao.load_latest(1)
        self.assertAlmostEqual(result.iloc[0]["variance"], 0.0345)

    async def test_load_latest_returns_none_when_empty(self):
        result = await self.dao.load_latest(999)
        self.assertIsNone(result)

    async def test_save_does_not_delete_prior_batch(self):
        """Unlike the old delete-then-replace behavior, history must persist."""
        await self.dao.save(_make_recommendations(["AAPL"]), model_id=1)
        await self.dao.save(_make_recommendations(["MSFT"]), model_id=1)
        all_rows = await self.db.execute_query(
            "SELECT symbol FROM Prediction WHERE model_id = ?", (1,), return_type="DataFrame"
        )
        self.assertEqual(set(all_rows["symbol"]), {"AAPL", "MSFT"})

    async def test_load_latest_returns_only_most_recent_batch(self):
        await self.dao.save(_make_recommendations(["AAPL"]), model_id=1)
        # Force a distinct created_at second so the two batches are distinguishable.
        await self.db.execute_query(
            "UPDATE Prediction SET created_at = created_at - 10 WHERE model_id = 1",
            query_type="UPDATE",
        )
        await self.dao.save(_make_recommendations(["MSFT"]), model_id=1)

        result = await self.dao.load_latest(1)

        self.assertEqual(list(result["symbol"]), ["MSFT"])

    async def test_load_latest_scoped_to_model_id(self):
        await self.dao.save(_make_recommendations(["AAPL"]), model_id=1)
        await self.dao.save(_make_recommendations(["MSFT"]), model_id=2)

        result = await self.dao.load_latest(1)

        self.assertEqual(list(result["symbol"]), ["AAPL"])

    async def test_save_with_empty_dataframe_inserts_nothing(self):
        await self.dao.save(pd.DataFrame(columns=["symbol", "timestamp"]), model_id=1)
        result = await self.dao.load_latest(1)
        self.assertIsNone(result)


class TestCleanData(_TempDbTestCase):
    async def test_removes_batches_older_than_cutoff(self):
        old_time = int(time.time()) - 1000
        await self.dao.save(_make_recommendations(["AAPL"]), model_id=1)
        await self.db.execute_query(
            "UPDATE Prediction SET created_at = ? WHERE model_id = 1",
            (old_time,),
            query_type="UPDATE",
        )
        await self.dao.save(_make_recommendations(["MSFT"]), model_id=1)

        await self.dao.clean_data(min_timestamp=old_time + 500)

        remaining = await self.db.execute_query(
            "SELECT symbol FROM Prediction", return_type="DataFrame"
        )
        self.assertEqual(list(remaining["symbol"]), ["MSFT"])

    async def test_keeps_batches_newer_than_cutoff(self):
        await self.dao.save(_make_recommendations(["AAPL"]), model_id=1)

        await self.dao.clean_data(min_timestamp=0)

        remaining = await self.db.execute_query(
            "SELECT symbol FROM Prediction", return_type="DataFrame"
        )
        self.assertEqual(list(remaining["symbol"]), ["AAPL"])


class TestSchemaMigration(_TempDbTestCase):
    async def test_variance_column_present_on_fresh_table(self):
        columns = await self.db.execute_query("PRAGMA table_info(Prediction)", query_type="SELECT")
        column_names = {row[1] for row in columns}
        self.assertIn("variance", column_names)

    async def test_migration_adds_variance_column_to_existing_table_non_destructively(self):
        # Simulate a pre-migration table (no variance column) with an existing row,
        # then confirm init2() adds the column additively without losing data.
        await self.db.execute_query("DROP TABLE Prediction", query_type="DELETE")
        await self.db.execute_query(
            """
            CREATE TABLE Prediction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                close REAL,
                prediction REAL,
                portfolio_weight REAL,
                created_at INTEGER NOT NULL
            )
            """,
            query_type="INSERT",
        )
        await self.db.execute_query(
            "INSERT INTO Prediction (model_id, symbol, timestamp, created_at) VALUES (1, 'AAPL', 1700000000, 1700000000)",
            query_type="INSERT",
        )

        fresh_dao = Prediction(self.db)
        await fresh_dao.init2()

        columns = await self.db.execute_query("PRAGMA table_info(Prediction)", query_type="SELECT")
        column_names = {row[1] for row in columns}
        self.assertIn("variance", column_names)
        rows = await self.db.execute_query("SELECT symbol FROM Prediction", return_type="DataFrame")
        self.assertEqual(list(rows["symbol"]), ["AAPL"])


if __name__ == "__main__":
    unittest.main()
