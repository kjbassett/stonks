import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from src.trading.brokers.schwab_client import SchwabClient
from src.trading.brokers.schwab_broker import SchwabBroker


def _make_broker(orders=None, positions=None, equity=100_000.0, dry_run=True):
    """Return a SchwabBroker backed by a mocked SchwabClient."""
    client = MagicMock(spec=SchwabClient)
    # Async client methods need AsyncMock so they can be awaited
    client.get_orders = AsyncMock(return_value=orders or [])
    client.get_positions = AsyncMock(return_value=positions or [])
    client.get_account_equity = AsyncMock(return_value=equity)
    client.place_order = AsyncMock(return_value=None)
    client.get_order = AsyncMock(return_value={})
    broker = SchwabBroker(client=client, account_number="12345", dry_run=dry_run)
    return broker, client


class TestGetTodayFills(unittest.IsolatedAsyncioTestCase):
    """
    SchwabBroker.get_today_fills() parses today's filled orders from the API
    into (symbol, 'BUY'|'SELL') pairs for OrderExecutor's PDT state restoration.
    """

    def _today(self):
        return datetime.now(timezone.utc).date()

    async def test_empty_order_history_returns_empty_list(self):
        broker, _ = _make_broker(orders=[])
        self.assertEqual(await broker.get_today_fills(), [])

    async def test_buy_order_returned_as_buy_pair(self):
        orders = [{"orderLegCollection": [
            {"instruction": "BUY", "instrument": {"symbol": "AAPL"}}
        ]}]
        broker, _ = _make_broker(orders=orders)
        self.assertIn(("AAPL", "BUY"), await broker.get_today_fills())

    async def test_sell_order_returned_as_sell_pair(self):
        orders = [{"orderLegCollection": [
            {"instruction": "SELL", "instrument": {"symbol": "MSFT"}}
        ]}]
        broker, _ = _make_broker(orders=orders)
        self.assertIn(("MSFT", "SELL"), await broker.get_today_fills())

    async def test_multiple_legs_all_returned(self):
        orders = [
            {"orderLegCollection": [{"instruction": "BUY",  "instrument": {"symbol": "AAPL"}}]},
            {"orderLegCollection": [{"instruction": "BUY",  "instrument": {"symbol": "AAPL"}}]},
            {"orderLegCollection": [{"instruction": "SELL", "instrument": {"symbol": "MSFT"}}]},
        ]
        broker, _ = _make_broker(orders=orders)
        fills = await broker.get_today_fills()
        self.assertEqual(fills.count(("AAPL", "BUY")), 2)
        self.assertEqual(fills.count(("MSFT", "SELL")), 1)

    async def test_unknown_instruction_excluded(self):
        orders = [{"orderLegCollection": [
            {"instruction": "EXCHANGE", "instrument": {"symbol": "AAPL"}}
        ]}]
        broker, _ = _make_broker(orders=orders)
        self.assertEqual(await broker.get_today_fills(), [])

    async def test_leg_with_no_symbol_excluded(self):
        orders = [{"orderLegCollection": [{"instruction": "BUY", "instrument": {}}]}]
        broker, _ = _make_broker(orders=orders)
        self.assertEqual(await broker.get_today_fills(), [])

    async def test_empty_legs_excluded(self):
        orders = [{"orderLegCollection": []}]
        broker, _ = _make_broker(orders=orders)
        self.assertEqual(await broker.get_today_fills(), [])

    async def test_api_error_propagates(self):
        client = MagicMock(spec=SchwabClient)
        client.get_orders = AsyncMock(side_effect=Exception("network error"))
        broker = SchwabBroker(client=client, account_number="12345", dry_run=True)
        with self.assertRaises(Exception):
            await broker.get_today_fills()


class TestFillOrder(unittest.IsolatedAsyncioTestCase):
    """SchwabBroker.fill_order() — dry_run behaviour and whole-share enforcement."""

    def setUp(self):
        self.broker, self.client = _make_broker()

    async def test_dry_run_does_not_call_place_order(self):
        await self.broker.fill_order("AAPL", 20.0, 10.0)
        self.client.place_order.assert_not_called()

    async def test_dry_run_returns_filled_with_dry_run_reason(self):
        result = await self.broker.fill_order("AAPL", 20.0, 10.0)
        self.assertTrue(result.filled)
        self.assertEqual(result.reason, "dry_run")

    async def test_buy_returns_positive_shares_delta(self):
        result = await self.broker.fill_order("AAPL", 20.0, 10.0)
        self.assertEqual(result.shares_delta, 20.0)

    async def test_sell_returns_negative_shares_delta(self):
        result = await self.broker.fill_order("AAPL", -15.0, 10.0)
        self.assertEqual(result.shares_delta, -15.0)

    async def test_fractional_shares_truncated_to_whole(self):
        # 20.9 shares requested -> int(20.9) = 20 shares filled
        result = await self.broker.fill_order("AAPL", 20.9, 10.0)
        self.assertEqual(result.shares_delta, 20.0)

    async def test_below_one_share_returns_no_change(self):
        result = await self.broker.fill_order("AAPL", 0.5, 10.0)
        self.assertEqual(result.reason, "no_change")
        self.assertEqual(result.shares_delta, 0.0)
        self.client.place_order.assert_not_called()

    async def test_live_mode_calls_place_order(self):
        broker, client = _make_broker(dry_run=False)
        client.place_order.return_value = "order-123"
        client.get_order.return_value = {"status": "FILLED"}
        await broker.fill_order("AAPL", 10.0, 150.0)
        self.assertTrue(client.place_order.called)

    async def test_live_mode_buy_places_correct_instruction(self):
        broker, client = _make_broker(dry_run=False)
        client.place_order.return_value = "order-abc"
        client.get_order.return_value = {"status": "FILLED"}
        await broker.fill_order("AAPL", 10.0, 150.0)
        placed_order = client.place_order.call_args[0][1]
        self.assertEqual(placed_order["orderLegCollection"][0]["instruction"], "BUY")

    async def test_live_mode_sell_places_correct_instruction(self):
        broker, client = _make_broker(dry_run=False)
        client.place_order.return_value = "order-abc"
        client.get_order.return_value = {"status": "FILLED"}
        await broker.fill_order("AAPL", -10.0, 150.0)
        placed_order = client.place_order.call_args[0][1]
        self.assertEqual(placed_order["orderLegCollection"][0]["instruction"], "SELL")


class TestGetPositions(unittest.IsolatedAsyncioTestCase):
    """SchwabBroker.get_positions() converts API response to Position dict."""

    async def test_empty_positions(self):
        broker, _ = _make_broker(positions=[])
        self.assertEqual(await broker.get_positions(), {})

    async def test_position_shares_and_avg_price_parsed(self):
        positions = [
            {"instrument": {"symbol": "AAPL"}, "longQuantity": 10.0, "averagePrice": 150.0}
        ]
        broker, _ = _make_broker(positions=positions)
        result = await broker.get_positions()
        self.assertIn("AAPL", result)
        self.assertAlmostEqual(result["AAPL"].shares, 10.0)
        self.assertAlmostEqual(result["AAPL"].avg_price, 150.0)

    async def test_symbol_uppercased(self):
        positions = [
            {"instrument": {"symbol": "aapl"}, "longQuantity": 5.0, "averagePrice": 100.0}
        ]
        broker, _ = _make_broker(positions=positions)
        self.assertIn("AAPL", await broker.get_positions())

    async def test_missing_avg_price_defaults_to_zero(self):
        positions = [{"instrument": {"symbol": "AAPL"}, "longQuantity": 5.0}]
        broker, _ = _make_broker(positions=positions)
        self.assertAlmostEqual((await broker.get_positions())["AAPL"].avg_price, 0.0)

    async def test_multiple_positions_all_returned(self):
        positions = [
            {"instrument": {"symbol": "AAPL"}, "longQuantity": 10.0, "averagePrice": 150.0},
            {"instrument": {"symbol": "MSFT"}, "longQuantity": 5.0,  "averagePrice": 300.0},
        ]
        broker, _ = _make_broker(positions=positions)
        result = await broker.get_positions()
        self.assertIn("AAPL", result)
        self.assertIn("MSFT", result)


if __name__ == "__main__":
    unittest.main()
