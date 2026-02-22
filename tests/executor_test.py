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


class TestPaperOrderExecutorBuySell(unittest.TestCase):

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0))
        self.today = date(2026, 2, 15)

    def test_buy_increases_shares(self):
        # equity=1000, target=0.5, price=10 → delta=500 → 50 shares
        result = self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(result.shares_delta, 50.0)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 50.0)

    def test_buy_reduces_cash(self):
        self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 500.0)

    def test_buy_sets_avg_price(self):
        self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].avg_price, 10.0)

    def test_sell_reduces_shares_and_returns_cash(self):
        yesterday = date(2026, 2, 14)
        self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=yesterday)
        result = self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 0.0)
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 1000.0)

    def test_no_change_when_already_at_target(self):
        self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        result = self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertEqual(result.reason, "no_change")

    def test_flat_fee_deducted_from_cash_and_recorded(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, flat_fee=5.0))
        ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        # delta_value=500, fee=5 → cash = 1000 - 500 - 5 = 495
        self.assertAlmostEqual(ex.broker.portfolio.cash, 495.0)
        self.assertAlmostEqual(ex.broker.portfolio.fees_paid, 5.0)

    def test_percent_fee_deducted(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, percent_fee=0.01))
        ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        # delta_value=500, fee=5 → cash = 1000 - 500 - 5 = 495
        self.assertAlmostEqual(ex.broker.portfolio.cash, 495.0)
        self.assertAlmostEqual(ex.broker.portfolio.fees_paid, 5.0)

    def test_avg_price_updates_on_second_buy(self):
        yesterday = date(2026, 2, 14)
        # Buy 50 @ $10 → cash=$500, equity=$1500 @ $20
        # Then target 100% @ $20 → delta=25 shares → avg = (500 + 500) / 75 ≈ 13.33
        self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=yesterday)
        self.ex.execute_target_exposure("AAPL", 1.0, 20.0, 1500.0, trade_date=self.today)
        avg = self.ex.broker.portfolio.positions["AAPL"].avg_price
        self.assertAlmostEqual(avg, (50 * 10 + 25 * 20) / 75, places=5)

    def test_get_equity_returns_cash_plus_positions(self):
        self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        # 50 shares * $12 + $500 cash = $1100
        equity = self.ex.get_equity({"AAPL": 12.0})
        self.assertAlmostEqual(equity, 1100.0)


class TestCashCap(unittest.TestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)

    def test_buy_capped_to_available_cash_no_fees(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0))
        # Requesting 3× exposure, but only $1000 available at $10/share = 100 shares max
        ex.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(ex.broker.portfolio.cash, 0.0, places=6)
        self.assertAlmostEqual(ex.broker.portfolio.positions["AAPL"].shares, 100.0, places=6)

    def test_buy_capped_with_percent_fee(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, percent_fee=0.01))
        # affordable = 1000 / 1.01 ≈ 990.099; shares = 990.099 / 10 ≈ 99.009
        ex.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(ex.broker.portfolio.cash, 0.0, places=4)
        expected_shares = (1000.0 / 1.01) / 10.0
        self.assertAlmostEqual(ex.broker.portfolio.positions["AAPL"].shares, expected_shares, places=4)

    def test_buy_capped_with_flat_fee(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0, flat_fee=10.0))
        # affordable = (1000 - 10) / 1.0 = 990; shares = 990 / 10 = 99
        ex.execute_target_exposure("AAPL", 3.0, 10.0, 1000.0, trade_date=self.today)
        self.assertAlmostEqual(ex.broker.portfolio.cash, 0.0, places=4)
        self.assertAlmostEqual(ex.broker.portfolio.positions["AAPL"].shares, 99.0, places=4)


class TestDrawdownHalt(unittest.TestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        # Use max_drawdown_pct=0.10; halt fires when equity < peak * 0.90
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), max_drawdown_pct=0.10)

    def test_halt_triggers_when_equity_drops_below_threshold(self):
        # peak will be set to 1000 by the first call, then 899 < 900 → halt
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(self.ex._halted)

    def test_halt_does_not_trigger_at_exact_threshold(self):
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        # 900 is NOT strictly less than 900 → no halt
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 900.0, trade_date=self.today)
        self.assertFalse(self.ex._halted)

    def test_buy_that_triggers_halt_is_itself_blocked(self):
        # Peak will be set to 1000 on the first call
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        # This buy call uses equity=899, which triggers the halt — and is then blocked
        result = self.ex.execute_target_exposure("AAPL", 0.2, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(self.ex._halted)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")
        # Cash should be untouched
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 1000.0)

    def test_halt_blocks_subsequent_buys(self):
        self.ex._halted = True
        result = self.ex.execute_target_exposure("AAPL", 0.5, 10.0, 1000.0, trade_date=self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")

    def test_halt_allows_closing_positions(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=50, avg_price=10.0)
        self.ex.broker.portfolio.cash = 500.0
        self.ex._halted = True
        result = self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)

    def test_needs_liquidation_flag_set_on_first_halt(self):
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 899.0, trade_date=self.today)
        self.assertTrue(self.ex._needs_liquidation)

    def test_needs_liquidation_not_re_set_after_first_halt(self):
        # _needs_liquidation is only set when transitioning from not-halted to halted.
        # Once halted and the sweep has cleared _needs_liquidation, further equity drops
        # should not re-trigger it — otherwise the liquidation sweep would fire repeatedly
        # on an already-empty portfolio.
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 899.0, trade_date=self.today)
        self.ex._needs_liquidation = False  # Simulate: sweep has run and cleared the flag
        self.ex.execute_target_exposure("AAPL", 0.0, 10.0, 800.0, trade_date=self.today)
        self.assertFalse(self.ex._needs_liquidation)

    def test_check_stop_losses_liquidates_all_positions_on_halt(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        self.ex.broker.portfolio.positions["MSFT"] = Position(shares=5, avg_price=20.0)
        self.ex.broker.portfolio.cash = 500.0
        self.ex._halted = True
        self.ex._needs_liquidation = True

        prices = {"AAPL": 9.0, "MSFT": 18.0}
        results = self.ex.check_stop_losses(prices)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.trigger == "drawdown_halt" for r in results))
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 0.0)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["MSFT"].shares, 0.0)

    def test_liquidation_sweep_only_runs_once(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        self.ex._halted = True
        self.ex._needs_liquidation = True
        prices = {"AAPL": 9.0}

        results_first = self.ex.check_stop_losses(prices)
        results_second = self.ex.check_stop_losses(prices)

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


class TestStopLoss(unittest.TestCase):

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=900.0), stop_loss_pct=0.05)
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)

    def test_stop_loss_triggers_below_threshold(self):
        # 9.4 < 10.0 * (1 - 0.05) = 9.5 → trigger
        results = self.ex.check_stop_losses({"AAPL": 9.4})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertTrue(results[0].filled)
        self.assertAlmostEqual(self.ex.broker.portfolio.positions["AAPL"].shares, 0.0)

    def test_stop_loss_does_not_trigger_above_threshold(self):
        # 9.6 > 9.5 → no trigger
        results = self.ex.check_stop_losses({"AAPL": 9.6})
        self.assertEqual(len(results), 0)

    def test_stop_loss_sell_adds_proceeds_to_cash(self):
        # proceeds = 10 shares * $9.4 = $94; cash = $900 + $94 = $994
        self.ex.check_stop_losses({"AAPL": 9.4})
        self.assertAlmostEqual(self.ex.broker.portfolio.cash, 994.0)

    def test_stop_loss_result_has_correct_trigger_and_reason(self):
        results = self.ex.check_stop_losses({"AAPL": 9.4})
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertEqual(results[0].reason, "ok")

    def test_symbol_missing_from_prices_is_skipped(self):
        results = self.ex.check_stop_losses({"MSFT": 50.0})
        self.assertEqual(len(results), 0)

    def test_stop_loss_blocked_by_pdt_preserves_both_fields(self):
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
        results = ex.check_stop_losses({"AAPL": 9.4})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trigger, "stop_loss")     # why it was initiated
        self.assertEqual(results[0].reason, "pdt_blocked")    # why it didn't execute


class TestPDT(unittest.TestCase):
    """
    PDT rule: when allow_intraday=False, block same-day round trips in both
    directions. buy→sell AND sell→buy on the same day are both blocked.
    """

    def setUp(self):
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), allow_intraday=False)
        self.today = date(2026, 2, 15)
        self.yesterday = date(2026, 2, 14)

    def _buy(self, symbol, trade_date, exposure=0.2, price=10.0, equity=1000.0):
        return self.ex.execute_target_exposure(
            symbol, exposure, price, equity, trade_date=trade_date
        )

    def _sell(self, symbol, trade_date, price=10.0):
        return self.ex.execute_target_exposure(
            symbol, 0.0, price, 1000.0, trade_date=trade_date
        )

    def test_buy_then_sell_same_day_blocked(self):
        self._buy("AAPL", self.today)
        result = self._sell("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    def test_buy_yesterday_sell_today_allowed(self):
        self._buy("AAPL", self.yesterday)
        result = self._sell("AAPL", self.today)
        self.assertTrue(result.filled)

    def test_sell_then_buy_same_day_blocked(self):
        # Position held from before today (no intraday buy recorded)
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        self._sell("AAPL", self.today)
        result = self._buy("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    def test_allow_intraday_bypasses_pdt(self):
        ex = OrderExecutor(PaperBroker(starting_cash=1000.0), allow_intraday=True)
        ex.execute_target_exposure("AAPL", 0.2, 10.0, 1000.0, trade_date=self.today)
        result = ex.execute_target_exposure("AAPL", 0.0, 10.0, 1000.0, trade_date=self.today)
        self.assertTrue(result.filled)

    def test_multiple_buys_each_counted_separately(self):
        # Two buys today → counter = 2; both block any sell attempt
        self._buy("AAPL", self.today, exposure=0.2)
        self._buy("AAPL", self.today, exposure=0.4, equity=800.0)
        result = self._sell("AAPL", self.today)
        self.assertEqual(result.reason, "pdt_blocked")

    def test_different_symbols_are_independent(self):
        self._buy("AAPL", self.today)
        self.ex.broker.portfolio.positions["MSFT"] = Position(shares=10, avg_price=10.0)
        result = self._sell("MSFT", self.today)
        self.assertTrue(result.filled)

    def test_force_close_records_sell_preventing_same_day_rebuy(self):
        self.ex.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        self.ex._force_close("AAPL", 10.0, self.today)
        result = self._buy("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    def test_force_close_still_executes_when_bought_today(self):
        # force_close bypasses PDT intentionally; result should still be filled
        self._buy("AAPL", self.today)
        result = self.ex._force_close("AAPL", 10.0, self.today)
        self.assertTrue(result.filled)


class TestStalePrediction(unittest.TestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        self.ex = OrderExecutor(PaperBroker(starting_cash=1000.0), stale_prediction_hours=2.0)

    def test_none_prediction_ts_is_always_allowed(self):
        result = self.ex.execute_target_exposure(
            "AAPL", 0.2, 10.0, 1000.0, prediction_ts=None, trade_date=self.today
        )
        self.assertTrue(result.filled)

    def test_very_old_prediction_is_blocked(self):
        old_ts = datetime(2020, 1, 1, 0, 0, 0)
        result = self.ex.execute_target_exposure(
            "AAPL", 0.2, 10.0, 1000.0, prediction_ts=old_ts, trade_date=self.today
        )
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "stale_prediction")

    def test_fresh_prediction_is_allowed(self):
        from datetime import timezone
        fresh_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        result = self.ex.execute_target_exposure(
            "AAPL", 0.2, 10.0, 1000.0, prediction_ts=fresh_ts, trade_date=self.today
        )
        self.assertTrue(result.filled)


if __name__ == "__main__":
    unittest.main()
