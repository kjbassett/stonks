"""Unit tests for src/trading/brokers/paper_broker.py."""

import unittest

import pandas as pd

from src.trading.brokers.paper_broker import PaperBroker
from src.trading.portfolio import Position


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_price_df(*rows) -> pd.DataFrame:
    """Build a minimal price DataFrame: rows are (symbol, timestamp, close)."""
    return pd.DataFrame(rows, columns=["symbol", "timestamp", "close"])


def _make_broker(price_df: pd.DataFrame = None, starting_cash: float = 1_000.0) -> PaperBroker:
    if price_df is None:
        price_df = pd.DataFrame(columns=["symbol", "timestamp", "close"])
    return PaperBroker(price_df, starting_cash=starting_cash)


# ---------------------------------------------------------------------------
# get_price — price lookup at a given _current_ts
# ---------------------------------------------------------------------------


class TestGetPrice(unittest.IsolatedAsyncioTestCase):
    """Test get_price() by setting _current_ts directly (unit testing the lookup logic)."""

    def setUp(self):
        self.price_df = _make_price_df(
            ("AAPL", 1_000, 100.0),
            ("AAPL", 2_000, 110.0),
            ("AAPL", 3_000, 120.0),
        )
        self.broker = _make_broker(self.price_df)

    async def test_get_price_at_exact_timestamp(self):
        self.broker._current_ts = 2_000
        price = await self.broker.get_price("AAPL")
        self.assertAlmostEqual(price, 110.0)

    async def test_get_price_uses_most_recent_before_timestamp(self):
        """Timestamp between two ticks → should return the earlier tick's price."""
        self.broker._current_ts = 1_500
        price = await self.broker.get_price("AAPL")
        self.assertAlmostEqual(price, 100.0)

    async def test_get_price_before_all_data_returns_none(self):
        self.broker._current_ts = 500
        price = await self.broker.get_price("AAPL")
        self.assertIsNone(price)

    async def test_get_price_at_last_timestamp(self):
        self.broker._current_ts = 3_000
        price = await self.broker.get_price("AAPL")
        self.assertAlmostEqual(price, 120.0)

    async def test_get_price_after_last_timestamp(self):
        """Timestamp past all data → return the last known price."""
        self.broker._current_ts = 9_999
        price = await self.broker.get_price("AAPL")
        self.assertAlmostEqual(price, 120.0)

    async def test_get_price_unknown_symbol_returns_none(self):
        self.broker._current_ts = 2_000
        price = await self.broker.get_price("MSFT")
        self.assertIsNone(price)

    async def test_get_price_with_multiple_symbols(self):
        df = _make_price_df(
            ("AAPL", 1_000, 100.0),
            ("MSFT", 1_000, 200.0),
        )
        broker = _make_broker(df)
        broker._current_ts = 1_000
        self.assertAlmostEqual(await broker.get_price("AAPL"), 100.0)
        self.assertAlmostEqual(await broker.get_price("MSFT"), 200.0)


# ---------------------------------------------------------------------------
# advance_time — sequential tick stepping
# ---------------------------------------------------------------------------


class TestAdvanceTime(unittest.IsolatedAsyncioTestCase):

    def test_first_call_returns_first_timestamp(self):
        df = _make_price_df(("AAPL", 1_000, 50.0), ("AAPL", 2_000, 75.0))
        broker = _make_broker(df)
        ts = broker.advance_time()
        self.assertAlmostEqual(ts, 1_000.0)

    def test_sequential_calls_return_timestamps_in_order(self):
        df = _make_price_df(
            ("AAPL", 1_000, 50.0),
            ("AAPL", 2_000, 75.0),
            ("AAPL", 3_000, 90.0),
        )
        broker = _make_broker(df)
        self.assertAlmostEqual(broker.advance_time(), 1_000.0)
        self.assertAlmostEqual(broker.advance_time(), 2_000.0)
        self.assertAlmostEqual(broker.advance_time(), 3_000.0)

    def test_returns_none_when_exhausted(self):
        df = _make_price_df(("AAPL", 1_000, 50.0))
        broker = _make_broker(df)
        broker.advance_time()
        self.assertIsNone(broker.advance_time())

    def test_empty_price_history_returns_none_immediately(self):
        broker = _make_broker()
        self.assertIsNone(broker.advance_time())

    def test_advance_time_deduplicates_across_symbols(self):
        """Two symbols sharing a timestamp produce only one tick at that timestamp."""
        df = _make_price_df(
            ("AAPL", 1_000, 100.0),
            ("MSFT", 1_000, 200.0),
            ("AAPL", 2_000, 110.0),
        )
        broker = _make_broker(df)
        timestamps = []
        while (ts := broker.advance_time()) is not None:
            timestamps.append(ts)
        self.assertEqual(timestamps, [1_000.0, 2_000.0])

    async def test_get_price_updates_after_advance(self):
        df = _make_price_df(
            ("AAPL", 1_000, 50.0),
            ("AAPL", 2_000, 75.0),
        )
        broker = _make_broker(df)
        broker.advance_time()
        self.assertAlmostEqual(await broker.get_price("AAPL"), 50.0)
        broker.advance_time()
        self.assertAlmostEqual(await broker.get_price("AAPL"), 75.0)


# ---------------------------------------------------------------------------
# get_prices_for_positions
# ---------------------------------------------------------------------------


class TestGetPricesForPositions(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        df = _make_price_df(
            ("AAPL", 1_000, 100.0),
            ("MSFT", 1_000, 200.0),
        )
        self.broker = _make_broker(df)
        self.broker._current_ts = 1_000

    async def test_returns_prices_for_all_held_positions(self):
        positions = {
            "AAPL": Position(shares=5, avg_price=90.0),
            "MSFT": Position(shares=2, avg_price=180.0),
        }
        prices = await self.broker.get_prices_for_positions(positions)
        self.assertAlmostEqual(prices["AAPL"], 100.0)
        self.assertAlmostEqual(prices["MSFT"], 200.0)

    async def test_symbol_without_price_data_excluded(self):
        positions = {
            "AAPL": Position(shares=5, avg_price=90.0),
            "TSLA": Position(shares=1, avg_price=300.0),  # not in price_df
        }
        prices = await self.broker.get_prices_for_positions(positions)
        self.assertIn("AAPL", prices)
        self.assertNotIn("TSLA", prices)

    async def test_empty_positions_returns_empty_dict(self):
        prices = await self.broker.get_prices_for_positions({})
        self.assertEqual(prices, {})


# ---------------------------------------------------------------------------
# get_equity
# ---------------------------------------------------------------------------


class TestGetEquity(unittest.IsolatedAsyncioTestCase):

    async def test_equity_equals_cash_when_no_positions(self):
        broker = _make_broker(starting_cash=1_000.0)
        broker._current_ts = 1_000
        equity = await broker.get_equity()
        self.assertAlmostEqual(equity, 1_000.0)

    async def test_equity_includes_position_market_value(self):
        df = _make_price_df(("AAPL", 1_000, 50.0))
        broker = _make_broker(df, starting_cash=500.0)
        broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=40.0)
        broker._current_ts = 1_000
        # equity = 500 cash + 10 * 50 = 1000
        equity = await broker.get_equity()
        self.assertAlmostEqual(equity, 1_000.0)

    async def test_equity_uses_internal_price_not_caller_price(self):
        """get_equity() must not require the caller to supply prices."""
        df = _make_price_df(("AAPL", 1_000, 60.0))
        broker = _make_broker(df, starting_cash=0.0)
        broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=40.0)
        broker._current_ts = 1_000
        equity = await broker.get_equity()
        self.assertAlmostEqual(equity, 600.0)

    async def test_equity_with_no_price_data_counts_only_cash(self):
        """Positions whose symbol has no price data are excluded from equity."""
        broker = _make_broker(starting_cash=1_000.0)
        broker.portfolio.positions["AAPL"] = Position(shares=5, avg_price=100.0)
        broker._current_ts = 1_000
        equity = await broker.get_equity()
        self.assertAlmostEqual(equity, 1_000.0)


# ---------------------------------------------------------------------------
# fill_order
# ---------------------------------------------------------------------------


class TestFillOrder(unittest.IsolatedAsyncioTestCase):

    async def test_buy_creates_position(self):
        broker = _make_broker(starting_cash=1_000.0)
        result = await broker.fill_order("AAPL", 5.0, 100.0)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(broker.portfolio.positions["AAPL"].shares, 5.0)
        self.assertAlmostEqual(broker.portfolio.cash, 500.0)

    async def test_buy_capped_to_available_cash(self):
        broker = _make_broker(starting_cash=300.0)
        # Requesting 5 shares @ $100 = $500, but only $300 available
        result = await broker.fill_order("AAPL", 5.0, 100.0)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(broker.portfolio.positions["AAPL"].shares, 3.0)
        self.assertAlmostEqual(broker.portfolio.cash, 0.0, places=4)

    async def test_sell_reduces_position(self):
        broker = _make_broker(starting_cash=0.0)
        broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=100.0)
        result = await broker.fill_order("AAPL", -5.0, 100.0)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(broker.portfolio.positions["AAPL"].shares, 5.0)
        self.assertAlmostEqual(broker.portfolio.cash, 500.0)

    async def test_zero_shares_delta_returns_no_change(self):
        broker = _make_broker()
        result = await broker.fill_order("AAPL", 0.0, 100.0)
        self.assertTrue(result.filled)
        self.assertEqual(result.reason, "no_change")

    async def test_fees_deducted_from_cash(self):
        broker = PaperBroker(
            pd.DataFrame(columns=["symbol", "timestamp", "close"]),
            starting_cash=1_000.0,
            flat_fee=1.0,
            percent_fee=0.01,
        )
        # buy 1 share @ $100: cost = $100, flat_fee = $1, percent_fee = $1 → total $102
        await broker.fill_order("AAPL", 1.0, 100.0)
        self.assertAlmostEqual(broker.portfolio.cash, 898.0)
        self.assertAlmostEqual(broker.portfolio.fees_paid, 2.0)


# ---------------------------------------------------------------------------
# empty price_history edge case
# ---------------------------------------------------------------------------


class TestEmptyPriceHistory(unittest.IsolatedAsyncioTestCase):

    async def test_broker_works_with_empty_price_df(self):
        broker = _make_broker()
        broker._current_ts = 1_000
        price = await broker.get_price("AAPL")
        self.assertIsNone(price)

    async def test_equity_is_cash_with_empty_price_df(self):
        broker = _make_broker(starting_cash=500.0)
        broker._current_ts = 1_000
        equity = await broker.get_equity()
        self.assertAlmostEqual(equity, 500.0)

    def test_advance_time_returns_none_with_empty_price_df(self):
        broker = _make_broker()
        self.assertIsNone(broker.advance_time())


if __name__ == "__main__":
    unittest.main()
