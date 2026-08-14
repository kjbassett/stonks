"""Unit tests for AsyncDatabase's read-connection pool and slow-query diagnostic."""

import asyncio
import os
import tempfile
import unittest
from unittest.mock import patch

import aiosqlite

from src.data_access.db.async_database import AsyncDatabase


class _TempDbTestCase(unittest.IsolatedAsyncioTestCase):
    """Base class: gives each test a fresh scratch SQLite file."""

    async def asyncSetUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "test.db")
        self.db = AsyncDatabase(self.db_path, read_pool_size=3)

    async def asyncTearDown(self):
        await self.db.close()
        self._tmpdir.cleanup()


class TestReadPool(_TempDbTestCase):
    async def test_connect_populates_configured_pool_size(self):
        await self.db.connect()
        self.assertEqual(len(self.db._read_conns), 3)
        self.assertEqual(self.db._read_pool.qsize(), 3)

    async def test_connect_is_idempotent(self):
        await self.db.connect()
        await self.db.connect()
        self.assertEqual(len(self.db._read_conns), 3)

    async def test_read_connections_are_read_only(self):
        await self.db.connect()
        rconn = await self.db._read_pool.get()
        try:
            with self.assertRaises(aiosqlite.OperationalError):
                await rconn.execute("CREATE TABLE t (id INTEGER)")
        finally:
            await self.db._read_pool.put(rconn)

    async def test_close_closes_read_pool(self):
        await self.db.connect()
        await self.db.close()
        self.assertEqual(len(self.db._read_conns), 0)


class TestExecuteQueryReadWrite(_TempDbTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.db.execute_query(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", query_type="INSERT"
        )

    async def test_insert_then_select_round_trips(self):
        await self.db.execute_query(
            "INSERT INTO t (v) VALUES (?)", ("hello",), query_type="INSERT"
        )
        result = await self.db.execute_query("SELECT v FROM t", return_type="DataFrame")
        self.assertEqual(result.iloc[0]["v"], "hello")

    async def test_concurrent_reads_all_succeed(self):
        await self.db.execute_query(
            "INSERT INTO t (v) VALUES (?)", ("x",), query_type="INSERT"
        )
        results = await asyncio.gather(*[
            self.db.execute_query("SELECT * FROM t", return_type="DataFrame")
            for _ in range(5)
        ])
        self.assertTrue(all(len(r) == 1 for r in results))


class TestSlowQueryDiagnostic(_TempDbTestCase):
    def _make_execute_with_real_delay(self):
        """Wrap self.db._execute with a real (tiny) delay so elapsed time is
        deterministically nonzero — a trivial query can otherwise complete
        within the monotonic timer's resolution, making `elapsed > 0.0` flaky.
        """
        original_execute = self.db._execute

        async def _slow_execute(*args, **kwargs):
            await asyncio.sleep(0.01)
            return await original_execute(*args, **kwargs)

        return _slow_execute

    async def test_logs_warning_when_over_threshold(self):
        await self.db.connect()
        with patch.object(self.db, "_execute", side_effect=self._make_execute_with_real_delay()), \
             patch("src.data_access.db.async_database._SLOW_QUERY_THRESHOLD_S", 0.0):
            with self.assertLogs("data_access.db", level="WARNING") as cm:
                await self.db.execute_query("SELECT 1")
        self.assertTrue(any("Slow query" in m for m in cm.output))

    async def test_no_warning_under_threshold(self):
        await self.db.connect()
        with self.assertNoLogs("data_access.db", level="WARNING"):
            await self.db.execute_query("SELECT 1")

    async def test_warning_truncates_long_query_text(self):
        await self.db.connect()
        long_query = "SELECT " + ", ".join(f"{i} AS c{i}" for i in range(200))
        with patch.object(self.db, "_execute", side_effect=self._make_execute_with_real_delay()), \
             patch("src.data_access.db.async_database._SLOW_QUERY_THRESHOLD_S", 0.0):
            with self.assertLogs("data_access.db", level="WARNING") as cm:
                await self.db.execute_query(long_query)
        warning_line = next(m for m in cm.output if "Slow query" in m)
        self.assertLess(len(warning_line), len(long_query))


if __name__ == "__main__":
    unittest.main()
