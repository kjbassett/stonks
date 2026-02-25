import unittest
from datetime import date, datetime

from src.trading.executor import OrderExecutor
from src.trading.brokers.paper_broker import PaperBroker
from src.trading.brokers.base_broker import TradeResult
from src.trading.portfolio import Position


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


class TestPaperOrderExecutorBuySell(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0))
        self.today = date(2026, 2, 15)

    async def test_buy_increases_shares(self):
        # equity=1000, target=0.5, price=10 -> delta=500 -> 50 shares
        result = await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(result.shares_delta, 50.0)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 50.0)

    async def test_buy_reduces_cash(self):
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 500.0)

    async def test_buy_sets_avg_price(self):
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].avg_price, 10.0)

    async def test_sell_reduces_shares_and_returns_cash(self):
        yesterday = date(2026, 2, 14)
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=yesterday)
        result = await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 0.0)
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 1000.0)

    async def test_no_change_when_already_at_target(self):
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        result = await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertEqual(result.reason, "no_change")

    async def test_flat_fee_deducted_from_cash_and_recorded(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, flat_fee=5.0))
        await ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        # delta_value=500, fee=5 -> cash = 1000 - 500 - 5 = 495
        self.assertAlmostEqual(ex.broker.portfolio.cash, 495.0)
        self.assertAlmostEqual(ex.broker.portfolio.fees_paid, 5.0)

    async def test_percent_fee_deducted(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, percent_fee=0.01))
        await ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        # delta_value=500, fee=5 -> cash = 1000 - 500 - 5 = 495
        self.assertAlmostEqual(ex.broker.portfolio.cash, 495.0)
        self.assertAlmostEqual(ex.broker.portfolio.fees_paid, 5.0)

    async def test_avg_price_updates_on_second_buy(self):
        yesterday = date(2026, 2, 14)
        # Buy 50 @ $10 -> cash=$500, equity=$1500 @ $20
        # Then target 100% @ $20 -> delta=25 shares -> avg = (500 + 500) / 75 ~= 13.33
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=yesterday)
        await self.ex.execute_target_exposure("AAPL", 1.0, 20.0, 1500.0, trade_date=self.today)
        avg = self.ex.broker.portfolio.positions["AAPL"].avg_price
        self.assertAlmostEqual(avg, (50 * 10 + 25 * 20) / 75, places=5)

    async def test_get_equity_returns_cash_plus_positions(self):
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        # 50 shares * $12 + $500 cash = $1100
        equity = await self.ex.get_equity({"AAPL": 12.0})
        self.assertAlmostEqual(equity, 1100.0)


class TestCashCap(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)

    async def test_buy_capped_to_available_cash_no_fees(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0))
        # Requesting 3x exposure, but only $1000 available at $10/share = 100 shares max
        await ex.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(ex.broker.portfolio.cash, 0.0, places=6)
        self.assertAlmostEqual(ex.broker.portfolio.positions["AAPL"].shares, 100.0, places=6)

    async def test_buy_capped_with_percent_fee(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, percent_fee=0.01))
        # affordable = 1000 / 1.01 ~= 990.099; shares = 990.099 / 10 ~= 99.009
        await ex.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(ex.broker.portfolio.cash, 0.0, places=4)
        expected_shares = (1000.0 / 1.01) / 10.0
        self.assertAlmostEqual(ex.broker.portfolio.positions["AAPL"].shares, expected_shares, places=4)

    async def test_buy_capped_with_flat_fee(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, flat_fee=10.0))
        # affordable = (1000 - 10) / 1.0 = 990; shares = 990 / 10 = 99
        await ex.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(ex.broker.portfolio.cash, 0.0, places=4)
        self.assertAlmostEqual(ex.broker.portfolio.positions["AAPL"].shares, 99.0, places=4)


class TestDrawdownHalt(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        # Use max_drawdown_pct=0.10; halt fires when equity < peak * 0.90
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), max_drawdown_pct=0.10)

    async def test_halt_triggers_when_equity_drops_below_threshold(self):
        # peak will be set to 1000 by the first call, then 899 < 900 -> halt
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(self.ex._halted)

    async def test_halt_does_not_trigger_at_exact_threshold(self):
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        # 900 is NOT strictly less than 900 -> no halt
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 900.0, trade_date=self.today)
        self.assertFalse(self.ex._halted)

    async def test_buy_that_triggers_halt_is_itself_blocked(self):
        # Peak will be set to 1000 on the first call
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        # This buy call uses equity=899, which triggers the halt — and is then blocked
        result = await self.ex.execute_target_exposure("AAPL", 0.2, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(self.ex._halted)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")
        # Cash should be untouched
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 1000.0)

    async def test_halt_blocks_subsequent_buys(self):
        self.ex._halted = True
        result = await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")

    async def test_halt_allows_closing_positions(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=50, avg_price=10.0)
        self.ex.broker.portfolio.cash = 500.0
        self.ex._halted = True
        result = await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)

    async def test_needs_liquidation_flag_set_on_first_halt(self):
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(self.ex._needs_liquidation)

    async def test_needs_liquidation_not_re_set_after_first_halt(self):
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 899.0, trade_date=self.today)
        self.ex._needs_liquidation = False  # Simulate: sweep has run and cleared the flag
        await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 800.0, trade_date=self.today)
        self.assertFalse(self.ex._needs_liquidation)

    async def test_check_stop_losses_liquidates_all_positions_on_halt(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        self.ex.broker.portfolio.positions["MSFT"] = Position(shares=5, avg_price=20.0)
        self.ex.broker.portfolio.cash = 500.0
        self.ex._halted = True
        self.ex._needs_liquidation = True

        prices = {"AAPL": 9.0, "MSFT": 18.0}
        results = await self.ex.check_stop_losses(prices)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.trigger == "drawdown_halt" for r in results))
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 0.0)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["MSFT"].shares, 0.0)

    async def test_liquidation_sweep_only_runs_once(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        self.ex._halted = True
        self.ex._needs_liquidation = True
        prices = {"AAPL": 9.0}

        results_first = await self.ex.check_stop_losses(prices)
        results_second = await self.ex.check_stop_losses(prices)

        self.assertEqual(len(results_first), 1)
        self.assertEqual(len(results_second), 0)

    def test_reset_halt_clears_state(self):
        self.ex._halted = True
        self.ex._peak_equity = 500.0
        self.ex._needs_liquidation = True
        self.ex.reset_halt()
        self.assertFalse(self.ex._halted)
        self.assertIsNone(self.ex._peak_equity)
        self.assertFalse(self.ex._needs_liquidation)


class TestStopLoss(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=900.0), stop_loss_pct=0.05)
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)

    async def test_stop_loss_triggers_below_threshold(self):
        # 9.4 < 10.0 * (1 - 0.05) = 9.5 -> trigger
        results = await self.ex.check_stop_losses({"AAPL": 9.4})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertTrue(results[0].filled)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 0.0)

    async def test_stop_loss_does_not_trigger_above_threshold(self):
        # 9.6 > 9.5 -> no trigger
        results = await self.ex.check_stop_losses({"AAPL": 9.6})
        self.assertEqual(len(results), 0)

    async def test_stop_loss_sell_adds_proceeds_to_cash(self):
        # proceeds = 10 shares * $9.4 = $94; cash = $900 + $94 = $994
        await self.ex.check_stop_losses({"AAPL": 9.4})
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 994.0)

    async def test_stop_loss_result_has_correct_trigger_and_reason(self):
        results = await self.ex.check_stop_losses({"AAPL": 9.4})
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertEqual(results[0].reason, "ok")

    async def test_symbol_missing_from_prices_is_skipped(self):
        results = await self.ex.check_stop_losses({"MSFT": 50.0})
        self.assertEqual(len(results), 0)

    async def test_stop_loss_blocked_by_pdt_preserves_both_fields(self):
        # Set up: position bought today, pdt blocks the close
        ex = OrderExecutor(
            PaperBroker(starting_cash=900.0),
            stop_loss_pct=0.05,
            allow_intraday=False,
        )
        today = date.today()
        ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        ex._record_buy("AAPL", today)  # Simulate: bought today
        # check_stop_losses calls execute_target_exposure which uses date.today()
        # Since our test runs today, the PDT block should fire
        results = await ex.check_stop_losses({"AAPL": 9.4})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trigger, "stop_loss")     # why it was initiated
        self.assertEqual(results[0].reason, "pdt_blocked")    # why it didn't execute


class TestPDT(unittest.IsolatedAsyncioTestCase):
    """
    PDT rule: when allow_intraday=False, block same-day round trips in both
    directions. buy->sell AND sell->buy on the same day are both blocked.
    """

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), allow_intraday=False)
        self.today = date(2026, 2, 15)
        self.yesterday = date(2026, 2, 14)

    async def _buy(self, symbol, trade_date, exposure=0.2, price=10.0, equity=1000.0):
        return await self.ex.execute_target_exposure(
            symbol, exposure, price, equity, trade_date=trade_date
        )

    async def _sell(self, symbol, trade_date, price=10.0):
        return await self.ex.execute_target_exposure(
            symbol, 0.0, price, 1000.0, trade_date=trade_date
        )

    async def test_buy_then_sell_same_day_blocked(self):
        await self._buy("AAPL", self.today)
        result = await self._sell("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_buy_yesterday_sell_today_allowed(self):
        await self._buy("AAPL", self.yesterday)
        result = await self._sell("AAPL", self.today)
        self.assertTrue(result.filled)

    async def test_sell_then_buy_same_day_blocked(self):
        # Position held from before today (no intraday buy recorded)
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        await self._sell("AAPL", self.today)
        result = await self._buy("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_allow_intraday_bypasses_pdt(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0), allow_intraday=True)
        await ex.execute_target_exposure("AAPL", 0.2, 10.0, 1000.0, trade_date=self.today)
        result = await ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)

    async def test_multiple_buys_each_counted_separately(self):
        # Two buys today -> counter = 2; both block any sell attempt
        await self._buy("AAPL", self.today, exposure=0.2)
        await self._buy("AAPL", self.today, exposure=0.4, equity=800.0)
        result = await self._sell("AAPL", self.today)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_different_symbols_are_independent(self):
        await self._buy("AAPL", self.today)
        self.ex.broker.portfolio.positions["MSFT"] = Position(shares=10, avg_price=10.0)
        result = await self._sell("MSFT", self.today)
        self.assertTrue(result.filled)

    async def test_force_close_records_sell_preventing_same_day_rebuy(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        await self.ex._force_close("AAPL", 10.0, self.today)
        result = await self._buy("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_force_close_still_executes_when_bought_today(self):
        # force_close bypasses PDT intentionally; result should still be filled
        await self._buy("AAPL", self.today)
        result = await self.ex._force_close("AAPL", 10.0, self.today)
        self.assertTrue(result.filled)


class TestStalePrediction(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), stale_prediction_hours=2.0)

    async def test_none_prediction_ts_is_always_allowed(self):
        result = await self.ex.execute_target_exposure(
            "AAPL", 0.2, 10.0, 1000.0, prediction_ts=None, trade_date=self.today
        )
        self.assertTrue(result.filled)

    async def test_very_old_prediction_is_blocked(self):
        old_ts = datetime(2020, 1, 1, 0, 0, 0)
        result = await self.ex.execute_target_exposure(
            "AAPL", 0.2, 10.0, 1000.0, prediction_ts=old_ts, trade_date=self.today
        )
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "stale_prediction")

    async def test_fresh_prediction_is_allowed(self):
        from datetime import timezone
        fresh_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        result = await self.ex.execute_target_exposure(
            "AAPL", 0.2, 10.0, 1000.0, prediction_ts=fresh_ts, trade_date=self.today
        )
        self.assertTrue(result.filled)


class TestLogging(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), allow_intraday=False)
        self.today = date(2026, 2, 15)

    async def test_executed_buy_logged_at_info(self):
        with self.assertLogs("trading.executor", level="INFO") as cm:
            await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(any("Trade executed" in m and "BUY" in m for m in cm.output))

    async def test_executed_sell_logged_at_info(self):
        yesterday = date(2026, 2, 14)
        await self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=yesterday)
        with self.assertLogs("trading.executor", level="INFO") as cm:
            await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(any("Trade executed" in m and "SELL" in m for m in cm.output))

    async def test_pdt_block_logged_at_warning(self):
        await self.ex.execute_target_exposure("AAPL", 0.2, 10.0, 1000.0, trade_date=self.today)
        with self.assertLogs("trading.executor", level="WARNING") as cm:
            await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(any("PDT block" in m for m in cm.output))

    async def test_halt_logged_at_warning(self):
        with self.assertLogs("trading.executor", level="WARNING") as cm:
            await self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
            await self.ex.execute_target_exposure("AAPL", 0.2, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(any("TRADING HALTED" in m for m in cm.output))

    async def test_stale_prediction_logged_at_warning(self):
        old_ts = datetime(2020, 1, 1)
        with self.assertLogs("trading.executor", level="WARNING") as cm:
            await self.ex.execute_target_exposure(
                "AAPL", 0.2, 10.0, 1000.0, prediction_ts=old_ts, trade_date=self.today
            )
        self.assertTrue(any("Stale prediction" in m for m in cm.output))

    async def test_stop_loss_trigger_logged_at_warning(self):
        ex = OrderExecutor(PaperBroker(starting_cash=900.0), stop_loss_pct=0.05)
        ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        with self.assertLogs("trading.executor", level="WARNING") as cm:
            await ex.check_stop_losses({"AAPL": 9.4})
        self.assertTrue(any("Stop-loss trigger" in m for m in cm.output))

    async def test_drawdown_halt_logged_at_warning(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0), max_drawdown_pct=0.10)
        ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        ex._halted = True
        ex._needs_liquidation = True
        with self.assertLogs("trading.executor", level="WARNING") as cm:
            await ex.check_stop_losses({"AAPL": 9.0})
        self.assertTrue(any("Drawdown liquidation" in m for m in cm.output))


if __name__ == "__main__":
    unittest.main()
