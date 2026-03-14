"""Unit tests for src/trading/trading_engine.py safety logic."""

import unittest
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

from src.trading.brokers.paper_broker import PaperBroker
from src.trading.brokers.base_broker import TradeResult
from src.trading.portfolio import Position
from src.trading.trading_engine import TradingEngine, SafetySignal


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_engine(**kwargs) -> TradingEngine:
    """Return a TradingEngine backed by PaperBroker with default safety params."""
    defaults = {"broker": PaperBroker(starting_cash=1000.0), "paper_trading": False}
    defaults.update(kwargs)
    return TradingEngine(**defaults)


# ---------------------------------------------------------------------------
# Drawdown halt
# ---------------------------------------------------------------------------


class TestDrawdownHalt(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        self.engine = _make_engine(max_drawdown_pct=0.10)

    def test_halt_triggers_when_equity_drops_below_threshold(self):
        self.engine._update_drawdown(1000.0)
        self.engine._update_drawdown(899.0)
        self.assertTrue(self.engine._halted)

    def test_halt_does_not_trigger_at_exact_threshold(self):
        self.engine._update_drawdown(1000.0)
        self.engine._update_drawdown(900.0)  # 900 is NOT strictly less than 900
        self.assertFalse(self.engine._halted)

    async def test_buy_blocked_when_halted(self):
        self.engine._halted = True
        result = await self.engine._execute_and_record("AAPL", 10.0, 10.0, self.today, None)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")

    async def test_sell_allowed_when_halted(self):
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=50, avg_price=10.0
        )
        self.engine._halted = True
        result = await self.engine._execute_and_record("AAPL", -50.0, 10.0, self.today, None)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)

    def test_needs_liquidation_flag_set_on_first_halt(self):
        self.engine._update_drawdown(1000.0)
        self.engine._update_drawdown(899.0)
        self.assertTrue(self.engine._needs_liquidation)

    def test_needs_liquidation_not_re_set_after_first_halt(self):
        self.engine._update_drawdown(1000.0)
        self.engine._update_drawdown(899.0)
        self.engine._needs_liquidation = False  # Simulate: sweep has run
        self.engine._update_drawdown(800.0)
        self.assertFalse(self.engine._needs_liquidation)

    async def test_liquidation_sweep_closes_all_positions(self):
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        self.engine.executor.broker.portfolio.positions["MSFT"] = Position(
            shares=5, avg_price=20.0
        )
        self.engine.executor.broker.portfolio.cash = 500.0
        prices = {"AAPL": 9.0, "MSFT": 18.0}

        signal = SafetySignal(
            is_halted=True,
            needs_liquidation=True,
            stop_loss_symbols=[],
            positions=dict(self.engine.executor.broker.portfolio.positions),
        )
        results = await self.engine._execute_liquidation_sweep(signal.positions, prices)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.trigger == "drawdown_halt" for r in results))
        self.assertAlmostEqual(
            self.engine.executor.broker.portfolio.positions["AAPL"].shares, 0.0
        )
        self.assertAlmostEqual(
            self.engine.executor.broker.portfolio.positions["MSFT"].shares, 0.0
        )

    async def test_liquidation_sweep_only_runs_once(self):
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        self.engine._needs_liquidation = True
        positions = {"AAPL": Position(shares=10, avg_price=10.0)}
        prices = {"AAPL": 9.0}

        results_first = await self.engine._execute_liquidation_sweep(positions, prices)
        # _needs_liquidation is cleared; a second call should find no open positions
        results_second = await self.engine._execute_liquidation_sweep({}, prices)

        self.assertEqual(len(results_first), 1)
        self.assertEqual(len(results_second), 0)

    def test_reset_halt_clears_state(self):
        self.engine._halted = True
        self.engine._peak_equity = 500.0
        self.engine._needs_liquidation = True
        self.engine.reset_halt()
        self.assertFalse(self.engine._halted)
        self.assertIsNone(self.engine._peak_equity)
        self.assertFalse(self.engine._needs_liquidation)

    async def test_halt_logged_at_warning(self):
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            self.engine._update_drawdown(1000.0)
            self.engine._update_drawdown(899.0)
        self.assertTrue(any("TRADING HALTED" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Stop-loss
# ---------------------------------------------------------------------------


class TestStopLoss(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = _make_engine(stop_loss_pct=0.05)
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        self.engine.executor.broker.portfolio.cash = 900.0

    async def _run_checks(self, prices):
        positions = await self.engine.executor.broker.get_positions()
        symbols = self.engine._find_stop_loss_triggers(positions, prices)
        return await self.engine._execute_stop_loss_closes(symbols, positions, prices)

    async def test_stop_loss_triggers_below_threshold(self):
        # 9.4 < 10.0 * (1 - 0.05) = 9.5 -> trigger
        results = await self._run_checks({"AAPL": 9.4})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertTrue(results[0].filled)
        self.assertAlmostEqual(
            self.engine.executor.broker.portfolio.positions["AAPL"].shares, 0.0
        )

    async def test_stop_loss_does_not_trigger_above_threshold(self):
        # 9.6 > 9.5 -> no trigger
        results = await self._run_checks({"AAPL": 9.6})
        self.assertEqual(len(results), 0)

    async def test_stop_loss_sell_adds_proceeds_to_cash(self):
        # proceeds = 10 shares * $9.4 = $94; cash = $900 + $94 = $994
        await self._run_checks({"AAPL": 9.4})
        self.assertAlmostEqual(self.engine.executor.broker.portfolio.cash, 994.0)

    async def test_stop_loss_result_has_correct_trigger_and_reason(self):
        results = await self._run_checks({"AAPL": 9.4})
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertEqual(results[0].reason, "ok")

    async def test_symbol_missing_from_prices_is_skipped(self):
        results = await self._run_checks({"MSFT": 50.0})
        self.assertEqual(len(results), 0)

    async def test_stop_loss_logged_at_warning(self):
        positions = await self.engine.executor.broker.get_positions()
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self.engine._force_close("AAPL", positions["AAPL"], 9.4, date.today())
        self.assertTrue(any("Stop-loss trigger" in m for m in cm.output))


# ---------------------------------------------------------------------------
# PDT (Pattern Day Trader) rule
# ---------------------------------------------------------------------------


class TestPDT(unittest.IsolatedAsyncioTestCase):
    """
    PDT rule: when allow_intraday=False, block same-day round-trips in both
    directions. buy->sell AND sell->buy on the same day are both blocked.
    """

    def setUp(self):
        self.engine = _make_engine(allow_intraday=False)
        self.today = date(2026, 2, 15)
        self.yesterday = date(2026, 2, 14)

    async def _buy(self, symbol, trade_date, shares_delta=20.0, price=10.0):
        return await self.engine._execute_and_record(
            symbol, shares_delta, price, trade_date, None
        )

    async def _sell(self, symbol, trade_date, shares_delta=-20.0, price=10.0):
        return await self.engine._execute_and_record(
            symbol, shares_delta, price, trade_date, None
        )

    async def test_buy_then_sell_same_day_blocked(self):
        await self._buy("AAPL", self.today)
        result = await self._sell("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_buy_yesterday_sell_today_allowed(self):
        self.engine._record_trade("AAPL", self.yesterday, "BUY")
        result = await self._sell("AAPL", self.today)
        self.assertTrue(result.filled)

    async def test_sell_then_buy_same_day_blocked(self):
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        await self._sell("AAPL", self.today, shares_delta=-10.0)
        result = await self._buy("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_allow_intraday_bypasses_pdt(self):
        engine = _make_engine(allow_intraday=True)
        await engine._execute_and_record("AAPL", 20.0, 10.0, self.today, None)
        result = await engine._execute_and_record("AAPL", -20.0, 10.0, self.today, None)
        self.assertTrue(result.filled)

    async def test_multiple_buys_each_counted_separately(self):
        await self._buy("AAPL", self.today)
        await self._buy("AAPL", self.today, shares_delta=10.0)
        result = await self._sell("AAPL", self.today)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_different_symbols_are_independent(self):
        await self._buy("AAPL", self.today)
        self.engine.executor.broker.portfolio.positions["MSFT"] = Position(
            shares=10, avg_price=10.0
        )
        result = await self._sell("MSFT", self.today, shares_delta=-10.0)
        self.assertTrue(result.filled)

    async def test_force_close_records_sell_preventing_same_day_rebuy(self):
        pos = Position(shares=10, avg_price=10.0)
        self.engine.executor.broker.portfolio.positions["AAPL"] = pos
        await self.engine._force_close("AAPL", pos, 10.0, self.today)
        result = await self._buy("AAPL", self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_force_close_executes_regardless_of_pdt(self):
        # _force_close bypasses PDT — stop-loss protection takes priority
        await self._buy("AAPL", self.today)
        pos = Position(shares=10, avg_price=10.0)
        self.engine.executor.broker.portfolio.positions["AAPL"] = pos
        result = await self.engine._force_close("AAPL", pos, 10.0, self.today)
        self.assertTrue(result.filled)

    async def test_pdt_block_logged_at_warning(self):
        await self._buy("AAPL", self.today)
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self._sell("AAPL", self.today)
        self.assertTrue(any("PDT block" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Stale prediction
# ---------------------------------------------------------------------------


class TestStalePrediction(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        self.engine = _make_engine(stale_prediction_hours=2.0)

    async def test_none_prediction_ts_is_always_allowed(self):
        result = await self.engine._execute_and_record(
            "AAPL", 20.0, 10.0, self.today, None
        )
        self.assertTrue(result.filled)

    async def test_very_old_prediction_is_blocked(self):
        old_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = await self.engine._execute_and_record(
            "AAPL", 20.0, 10.0, self.today, old_ts
        )
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "stale_prediction")

    async def test_fresh_prediction_is_allowed(self):
        fresh_ts = datetime.now(timezone.utc)
        result = await self.engine._execute_and_record(
            "AAPL", 20.0, 10.0, self.today, fresh_ts
        )
        self.assertTrue(result.filled)

    async def test_stale_prediction_only_blocks_buys(self):
        # A sell should always go through regardless of prediction staleness
        old_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        result = await self.engine._execute_and_record(
            "AAPL", -10.0, 10.0, self.today, old_ts
        )
        self.assertTrue(result.filled)

    async def test_stale_prediction_logged_at_warning(self):
        old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self.engine._execute_and_record("AAPL", 20.0, 10.0, self.today, old_ts)
        self.assertTrue(any("Stale prediction" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Manual trade
# ---------------------------------------------------------------------------


class TestManualTrade(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = _make_engine()

    async def test_manual_trade_executes_successfully(self):
        result = await self.engine.execute_manual_trade("AAPL", 5.0, 100.0)
        self.assertTrue(result.filled)
        self.assertAlmostEqual(
            self.engine.executor.broker.portfolio.positions["AAPL"].shares, 5.0
        )

    async def test_manual_trade_blocked_when_halted(self):
        self.engine._halted = True
        result = await self.engine.execute_manual_trade("AAPL", 5.0, 100.0)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")
        self.assertEqual(result.trigger, "manual")

    async def test_manual_trade_blocked_by_pdt(self):
        engine = _make_engine(allow_intraday=False)
        today = date.today()
        engine._record_trade("AAPL", today, "BUY")
        result = await engine.execute_manual_trade("AAPL", -5.0, 100.0)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")
        self.assertEqual(result.trigger, "manual")

    async def test_manual_trade_records_for_pdt(self):
        engine = _make_engine(allow_intraday=False)
        today = date.today()
        await engine.execute_manual_trade("AAPL", 5.0, 100.0)
        # Same-day sell should now be blocked
        result = await engine.execute_manual_trade("AAPL", -5.0, 100.0)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_manual_trade_halt_logged_at_warning(self):
        self.engine._halted = True
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self.engine.execute_manual_trade("AAPL", 5.0, 100.0)
        self.assertTrue(any("halted" in m for m in cm.output))


# ---------------------------------------------------------------------------
# perform_safety_checks
# ---------------------------------------------------------------------------


class TestPerformSafetyChecks(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = _make_engine(max_drawdown_pct=0.10, stop_loss_pct=0.05)

    async def test_returns_no_triggers_when_all_clear(self):
        signal = await self.engine.perform_safety_checks({"AAPL": 10.0})
        self.assertFalse(signal.is_halted)
        self.assertFalse(signal.needs_liquidation)
        self.assertEqual(signal.stop_loss_symbols, [])

    async def test_updates_halt_flag_on_drawdown(self):
        self.engine._peak_equity = 1000.0
        # Portfolio has no positions; cash = 1000 initially, but we set it lower
        self.engine.executor.broker.portfolio.cash = 899.0
        signal = await self.engine.perform_safety_checks({})
        self.assertTrue(signal.is_halted)
        self.assertTrue(signal.needs_liquidation)

    async def test_identifies_stop_loss_symbols(self):
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        # 9.4 < 10 * 0.95 = 9.5 -> triggers
        signal = await self.engine.perform_safety_checks({"AAPL": 9.4})
        self.assertIn("AAPL", signal.stop_loss_symbols)

    async def test_does_not_execute_trades(self):
        # Even with a stop-loss detected, perform_safety_checks must not close any positions
        self.engine.executor.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        await self.engine.perform_safety_checks({"AAPL": 9.4})
        # Position should be unchanged
        self.assertAlmostEqual(
            self.engine.executor.broker.portfolio.positions["AAPL"].shares, 10.0
        )


if __name__ == "__main__":
    unittest.main()
