import datetime
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd


class TestGetRecentSplits(unittest.IsolatedAsyncioTestCase):
    async def test_returns_splits_from_client(self):
        client = MagicMock()
        expected = [{"ticker": "CTNT", "execution_date": "2026-04-28"}]
        client.get_stock_splits = AsyncMock(return_value=expected)

        from src.data_sources.corporate_actions import _get_recent_splits

        result = await _get_recent_splits(client, "2026-01-01")

        client.get_stock_splits.assert_awaited_once_with(
            execution_date_gte="2026-01-01", all_pages=True
        )
        self.assertEqual(result, expected)

    async def test_returns_empty_list_when_no_splits(self):
        client = MagicMock()
        client.get_stock_splits = AsyncMock(return_value=[])

        from src.data_sources.corporate_actions import _get_recent_splits

        result = await _get_recent_splits(client, "2026-01-01")

        self.assertEqual(result, [])

    async def test_passes_since_date_correctly(self):
        client = MagicMock()
        client.get_stock_splits = AsyncMock(return_value=[])

        from src.data_sources.corporate_actions import _get_recent_splits

        await _get_recent_splits(client, "2025-06-15")

        client.get_stock_splits.assert_awaited_once_with(
            execution_date_gte="2025-06-15", all_pages=True
        )

    async def test_propagates_client_exception(self):
        client = MagicMock()
        client.get_stock_splits = AsyncMock(side_effect=RuntimeError("API error"))

        from src.data_sources.corporate_actions import _get_recent_splits

        with self.assertRaises(RuntimeError):
            await _get_recent_splits(client, "2026-01-01")


class TestIsSplitProcessed(unittest.IsolatedAsyncioTestCase):
    async def _run(self, rows, ticker="CTNT", execution_date="2026-04-28"):
        mock_dao = MagicMock()
        mock_dao.get = AsyncMock(return_value=rows)

        with patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_dm.get_dao.return_value = mock_dao
            from src.data_sources.corporate_actions import _is_split_processed

            return await _is_split_processed(ticker, execution_date)

    async def test_returns_true_when_row_exists(self):
        result = await self._run(rows=pd.DataFrame([{"ticker": "CTNT"}]))
        self.assertTrue(result)

    async def test_returns_false_when_no_rows(self):
        result = await self._run(rows=pd.DataFrame())
        self.assertFalse(result)

    async def test_queries_correct_dao(self):
        mock_dao = MagicMock()
        mock_dao.get = AsyncMock(return_value=pd.DataFrame())

        with patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_dm.get_dao.return_value = mock_dao
            from src.data_sources.corporate_actions import _is_split_processed

            await _is_split_processed("AAPL", "2025-01-01")

        mock_dm.get_dao.assert_called_with("StockSplit")
        mock_dao.get.assert_awaited_once_with(
            ticker="AAPL", execution_date="2025-01-01"
        )

    async def test_empty_ticker_still_queries(self):
        result = await self._run(rows=pd.DataFrame(), ticker="", execution_date="")
        self.assertFalse(result)


class TestClearCompanyPriceData(unittest.IsolatedAsyncioTestCase):
    async def test_deletes_all_three_tables(self):
        mock_db = MagicMock()
        mock_db.execute_query = AsyncMock()

        with patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_dm.db = mock_db
            from src.data_sources.corporate_actions import _clear_company_price_data

            await _clear_company_price_data(42)

        calls = mock_db.execute_query.await_args_list
        self.assertEqual(len(calls), 3)
        tables_hit = {c.args[0].split("FROM ")[1].split(" ")[0] for c in calls}
        self.assertEqual(
            tables_hit,
            {"TradingData", "TradingDataAggregation", "TradingDataAttemptedQueries"},
        )

    async def test_uses_parameterized_query(self):
        mock_db = MagicMock()
        mock_db.execute_query = AsyncMock()

        with patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_dm.db = mock_db
            from src.data_sources.corporate_actions import _clear_company_price_data

            await _clear_company_price_data(99)

        for call in mock_db.execute_query.await_args_list:
            params = call.args[1]
            self.assertEqual(params, (99,))

    async def test_uses_delete_query_type(self):
        mock_db = MagicMock()
        mock_db.execute_query = AsyncMock()

        with patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_dm.db = mock_db
            from src.data_sources.corporate_actions import _clear_company_price_data

            await _clear_company_price_data(1)

        for call in mock_db.execute_query.await_args_list:
            self.assertEqual(call.kwargs.get("query_type") or call.args[2], "DELETE")


class TestSyncSplitAdjustments(unittest.IsolatedAsyncioTestCase):
    def _make_split(self, ticker="CTNT", date="2026-04-28", frm=1, to=2):
        return {
            "ticker": ticker,
            "execution_date": date,
            "split_from": frm,
            "split_to": to,
        }

    def _patch_all(
        self,
        splits=None,
        is_processed=False,
        company_rows=None,
        since_date="2020-01-01",
    ):
        if splits is None:
            splits = []
        if company_rows is None:
            company_rows = pd.DataFrame([{"id": 1}])

        patches = {
            "get_recent_splits": patch(
                "src.data_sources.corporate_actions._get_recent_splits",
                new=AsyncMock(return_value=splits),
            ),
            "is_processed": patch(
                "src.data_sources.corporate_actions._is_split_processed",
                new=AsyncMock(return_value=is_processed),
            ),
            "clear_data": patch(
                "src.data_sources.corporate_actions._clear_company_price_data",
                new=AsyncMock(),
            ),
            "call_limiter": patch(
                "src.data_sources.corporate_actions.call_limiter",
            ),
            "async_ref_client": patch(
                "src.data_sources.corporate_actions.AsyncReferenceClient"
            ),
            "earliest_market_time": patch(
                "src.data_sources.corporate_actions.earliest_market_time",
                return_value=datetime.date(2020, 1, 1).timetuple(),
            ),
            "dao_manager": patch(
                "src.data_sources.corporate_actions.dao_manager",
            ),
        }
        return patches

    async def test_clears_tracked_company_and_records_split(self):
        split = self._make_split()
        mock_split_dao = MagicMock()
        mock_split_dao.insert = AsyncMock()
        mock_company_dao = MagicMock()
        mock_company_dao.get = AsyncMock(return_value=pd.DataFrame([{"id": 3957}]))

        with patch(
            "src.data_sources.corporate_actions._get_recent_splits",
            new=AsyncMock(return_value=[split]),
        ), patch(
            "src.data_sources.corporate_actions._is_split_processed",
            new=AsyncMock(return_value=False),
        ), patch(
            "src.data_sources.corporate_actions._clear_company_price_data",
            new=AsyncMock(),
        ) as mock_clear, patch(
            "src.data_sources.corporate_actions.call_limiter",
        ) as mock_limiter, patch(
            "src.data_sources.corporate_actions.AsyncReferenceClient",
        ), patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_limiter.__aenter__ = AsyncMock(return_value=None)
            mock_limiter.__aexit__ = AsyncMock(return_value=None)
            mock_dm.get_dao.side_effect = lambda name: (
                mock_split_dao if name == "StockSplit" else mock_company_dao
            )

            from src.data_sources.corporate_actions import sync_split_adjustments

            result = await sync_split_adjustments(since_date="2026-01-01")

        mock_clear.assert_awaited_once_with(3957)
        mock_split_dao.insert.assert_awaited_once()
        self.assertIn("CTNT", result)
        self.assertIn("1", result)

    async def test_skips_already_processed_split(self):
        split = self._make_split()

        with patch(
            "src.data_sources.corporate_actions._get_recent_splits",
            new=AsyncMock(return_value=[split]),
        ), patch(
            "src.data_sources.corporate_actions._is_split_processed",
            new=AsyncMock(return_value=True),
        ), patch(
            "src.data_sources.corporate_actions._clear_company_price_data",
            new=AsyncMock(),
        ) as mock_clear, patch(
            "src.data_sources.corporate_actions.call_limiter",
        ) as mock_limiter, patch(
            "src.data_sources.corporate_actions.AsyncReferenceClient",
        ):
            mock_limiter.__aenter__ = AsyncMock(return_value=None)
            mock_limiter.__aexit__ = AsyncMock(return_value=None)

            from src.data_sources.corporate_actions import sync_split_adjustments

            result = await sync_split_adjustments(since_date="2026-01-01")

        mock_clear.assert_not_awaited()
        self.assertIn("Skipped 1", result)

    async def test_skips_untracked_company_but_records_split(self):
        split = self._make_split(ticker="UNKN")
        mock_split_dao = MagicMock()
        mock_split_dao.insert = AsyncMock()
        mock_company_dao = MagicMock()
        mock_company_dao.get = AsyncMock(return_value=pd.DataFrame())

        with patch(
            "src.data_sources.corporate_actions._get_recent_splits",
            new=AsyncMock(return_value=[split]),
        ), patch(
            "src.data_sources.corporate_actions._is_split_processed",
            new=AsyncMock(return_value=False),
        ), patch(
            "src.data_sources.corporate_actions._clear_company_price_data",
            new=AsyncMock(),
        ) as mock_clear, patch(
            "src.data_sources.corporate_actions.call_limiter",
        ) as mock_limiter, patch(
            "src.data_sources.corporate_actions.AsyncReferenceClient",
        ), patch(
            "src.data_sources.corporate_actions.dao_manager"
        ) as mock_dm:
            mock_limiter.__aenter__ = AsyncMock(return_value=None)
            mock_limiter.__aexit__ = AsyncMock(return_value=None)
            mock_dm.get_dao.side_effect = lambda name: (
                mock_split_dao if name == "StockSplit" else mock_company_dao
            )

            from src.data_sources.corporate_actions import sync_split_adjustments

            result = await sync_split_adjustments(since_date="2026-01-01")

        mock_clear.assert_not_awaited()
        mock_split_dao.insert.assert_awaited_once()
        self.assertIn("Skipped 1", result)
        self.assertIn("none", result)

    async def test_defaults_since_date_to_earliest_market_time(self):
        fixed_ts = datetime.datetime(2020, 3, 1).timestamp()

        with patch(
            "src.data_sources.corporate_actions._get_recent_splits",
            new=AsyncMock(return_value=[]),
        ) as mock_get, patch(
            "src.data_sources.corporate_actions.call_limiter",
        ) as mock_limiter, patch(
            "src.data_sources.corporate_actions.AsyncReferenceClient",
        ), patch(
            "src.data_sources.corporate_actions.earliest_market_time",
            return_value=fixed_ts,
        ):
            mock_limiter.__aenter__ = AsyncMock(return_value=None)
            mock_limiter.__aexit__ = AsyncMock(return_value=None)

            from src.data_sources.corporate_actions import sync_split_adjustments

            await sync_split_adjustments()

        _client_arg, since_arg = mock_get.await_args.args
        self.assertEqual(since_arg, "2020-03-01")

    async def test_empty_splits_returns_zero_cleared(self):
        with patch(
            "src.data_sources.corporate_actions._get_recent_splits",
            new=AsyncMock(return_value=[]),
        ), patch(
            "src.data_sources.corporate_actions.call_limiter",
        ) as mock_limiter, patch(
            "src.data_sources.corporate_actions.AsyncReferenceClient",
        ):
            mock_limiter.__aenter__ = AsyncMock(return_value=None)
            mock_limiter.__aexit__ = AsyncMock(return_value=None)

            from src.data_sources.corporate_actions import sync_split_adjustments

            result = await sync_split_adjustments(since_date="2026-01-01")

        self.assertIn("0 companies", result)
        self.assertIn("Skipped 0", result)


if __name__ == "__main__":
    unittest.main()
