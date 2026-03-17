import unittest

from src.trading.trading_engine import TradingEngine
from src.trading.brokers.paper_broker import PaperBroker
from src.trading.brokers.base_broker import TradeResult


class TestTradeResult(unittest.TestCase):

    def test_trigger_defaults_to_none(self):
        r = TradeResult("AAPL", 10.0, 150.0, True)
        self.assertIsNone(r.trigger)

    def test_trigger_and_reason_are_independent(self):
        r = TradeResult("AAPL", 0.0, 150.0, False, "pdt_blocked", "stop_loss")
        self.assertEqual(r.reason, "pdt_blocked")
        self.assertEqual(r.trigger, "stop_loss")

    def test_order_id_defaults_to_none(self):
        r = TradeResult("AAPL", 10.0, 150.0, True)
        self.assertIsNone(r.order_id)


class TestExecuteTargetExposure(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = TradingEngine(broker=PaperBroker(None, starting_cash=1000.0))

    async def test_buy_increases_shares(self):
        # equity=1000, target=0.5, price=10 -> delta=500 -> 50 shares
        result = await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(result.shares_delta, 50.0)
        self.assertAlmostEqual(self.engine.broker.portfolio.positions["AAPL"].shares, 50.0)

    async def test_buy_reduces_cash(self):
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        self.assertAlmostEqual(self.engine.broker.portfolio.cash, 500.0)

    async def test_buy_sets_avg_price(self):
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        self.assertAlmostEqual(self.engine.broker.portfolio.positions["AAPL"].avg_price, 10.0)

    async def test_sell_reduces_shares_and_returns_cash(self):
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        result = await self.engine.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)
        self.assertAlmostEqual(self.engine.broker.portfolio.positions["AAPL"].shares, 0.0)
        self.assertAlmostEqual(self.engine.broker.portfolio.cash, 1000.0)

    async def test_no_change_when_already_at_target(self):
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        result = await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        self.assertEqual(result.reason, "no_change")

    async def test_flat_fee_deducted_from_cash_and_recorded(self):
        engine = TradingEngine(broker=PaperBroker(None, starting_cash=1000.0, flat_fee=5.0))
        await engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        # delta_value=500, fee=5 -> cash = 1000 - 500 - 5 = 495
        self.assertAlmostEqual(engine.broker.portfolio.cash, 495.0)
        self.assertAlmostEqual(engine.broker.portfolio.fees_paid, 5.0)

    async def test_percent_fee_deducted(self):
        engine = TradingEngine(broker=PaperBroker(None, starting_cash=1000.0, percent_fee=0.01))
        await engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        # delta_value=500, fee=5 -> cash = 1000 - 500 - 5 = 495
        self.assertAlmostEqual(engine.broker.portfolio.cash, 495.0)
        self.assertAlmostEqual(engine.broker.portfolio.fees_paid, 5.0)

    async def test_avg_price_updates_on_second_buy(self):
        # Buy 50 @ $10 -> cash=$500, equity=$1500 @ $20
        # Then target 100% @ $20 -> delta=25 shares -> avg = (500 + 500) / 75 ~= 13.33
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        await self.engine.execute_target_exposure("AAPL", 1.0, 20.0, 1500.0)
        avg = self.engine.broker.portfolio.positions["AAPL"].avg_price
        self.assertAlmostEqual(avg, (50 * 10 + 25 * 20) / 75, places=5)

    async def test_get_equity_returns_cash_plus_positions(self):
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        # 50 shares * $12 + $500 cash = $1100
        equity = await self.engine.broker.get_equity()
        self.assertAlmostEqual(equity, 1000)


class TestCashCap(unittest.IsolatedAsyncioTestCase):

    async def test_buy_capped_to_available_cash_no_fees(self):
        engine = TradingEngine(broker=PaperBroker(starting_cash=1000.0), paper_trading=False)
        # Requesting 3x exposure, but only $1000 available at $10/share = 100 shares max
        await engine.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0)
        self.assertAlmostEqual(engine.broker.portfolio.cash, 0.0, places=6)
        self.assertAlmostEqual(engine.broker.portfolio.positions["AAPL"].shares, 100.0, places=6)

    async def test_buy_capped_with_percent_fee(self):
        engine = TradingEngine(broker=PaperBroker(starting_cash=1000.0, percent_fee=0.01), paper_trading=False)
        # affordable = 1000 / 1.01 ~= 990.099; shares = 990.099 / 10 ~= 99.009
        await engine.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0)
        self.assertAlmostEqual(engine.broker.portfolio.cash, 0.0, places=4)
        expected_shares = (1000.0 / 1.01) / 10.0
        self.assertAlmostEqual(engine.broker.portfolio.positions["AAPL"].shares, expected_shares, places=4)

    async def test_buy_capped_with_flat_fee(self):
        engine = TradingEngine(broker=PaperBroker(starting_cash=1000.0, flat_fee=10.0), paper_trading=False)
        # affordable = (1000 - 10) / 1.0 = 990; shares = 990 / 10 = 99
        await engine.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0)
        self.assertAlmostEqual(engine.broker.portfolio.cash, 0.0, places=4)
        self.assertAlmostEqual(engine.broker.portfolio.positions["AAPL"].shares, 99.0, places=4)


class TestLogging(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = TradingEngine(broker=PaperBroker(starting_cash=1000.0), paper_trading=False)

    async def test_executed_buy_logged_at_info(self):
        with self.assertLogs("trading.engine", level="INFO") as cm:
            await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        self.assertTrue(any("Trade executed" in m and "BUY" in m for m in cm.output))

    async def test_executed_sell_logged_at_info(self):
        await self.engine.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0)
        with self.assertLogs("trading.engine", level="INFO") as cm:
            await self.engine.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0)
        self.assertTrue(any("Trade executed" in m and "SELL" in m for m in cm.output))


if __name__ == "__main__":
    unittest.main()
