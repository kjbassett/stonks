"""Unit tests for src/trading/trading_engine.py safety logic."""

import unittest
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd

from src.trading.brokers.paper_broker import PaperBroker
from src.trading.brokers.base_broker import TradeResult
from src.trading.portfolio import Position
from src.trading.trading_engine import TradingEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_price_df(*rows):
    """Build a minimal price DataFrame: rows are (symbol, timestamp, close)."""
    return pd.DataFrame(rows, columns=["symbol", "timestamp", "close"])


def _make_broker(price_df=None, starting_cash: float = 1_000.0) -> PaperBroker:
    if price_df is None:
        price_df = pd.DataFrame(columns=["symbol", "timestamp", "close"])
    return PaperBroker(price_df, starting_cash=starting_cash)


def _make_engine(**kwargs) -> TradingEngine:
    """Return a TradingEngine backed by PaperBroker with default safety params."""
    defaults = {"broker": _make_broker()}
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
        """Stop-loss check: equity below peak * (1 - threshold) sets halt."""
        positions = {"AAPL": Position(shares=10, avg_price=100.0)}
        # Peak 1000, equity 899 => 899/1000 < 0.90 => halt
        self.engine._peak_equity = 1_000.0
        triggered = self.engine._find_stop_loss_triggers(positions, {"AAPL": 89.9}, 899.0)
        self.assertTrue(self.engine._halted)
        self.assertEqual(triggered, ["AAPL"])

    def test_halt_does_not_trigger_at_exact_threshold(self):
        """equity == peak * (1 - threshold) should NOT trigger halt."""
        self.engine._peak_equity = 1_000.0
        triggered = self.engine._find_stop_loss_triggers({}, {}, 900.0)
        self.assertFalse(self.engine._halted)
        self.assertEqual(triggered, [])

    def test_needs_liquidation_flag_set_on_first_halt(self):
        self.engine._peak_equity = 1_000.0
        self.engine._find_stop_loss_triggers({}, {}, 899.0)
        self.assertTrue(self.engine._needs_liquidation)

    def test_needs_liquidation_not_re_set_after_first_halt(self):
        self.engine._peak_equity = 1_000.0
        self.engine._halted = True  # already halted
        self.engine._needs_liquidation = False  # sweep already ran
        # Should not re-set _needs_liquidation
        self.engine._find_stop_loss_triggers({}, {}, 800.0)
        self.assertFalse(self.engine._needs_liquidation)

    async def test_buy_blocked_when_halted(self):
        self.engine._halted = True
        result = await self.engine.execute_trade("AAPL", 10.0, 10.0, self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "halted")

    async def test_sell_allowed_when_halted(self):
        self.engine.broker.portfolio.positions["AAPL"] = Position(
            shares=50, avg_price=10.0
        )
        self.engine._halted = True
        result = await self.engine.execute_trade("AAPL", -50.0, 10.0, self.today)
        self.assertTrue(result.filled)
        self.assertLess(result.shares_delta, 0)

    async def test_liquidation_sweep_closes_all_positions(self):
        self.engine.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        self.engine.broker.portfolio.positions["MSFT"] = Position(
            shares=5, avg_price=20.0
        )
        self.engine.broker.portfolio.cash = 500.0
        positions = dict(self.engine.broker.portfolio.positions)
        prices = {"AAPL": 9.0, "MSFT": 18.0}

        results = await self.engine._execute_liquidation_sweep(positions, prices)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.trigger == "drawdown_halt" for r in results))
        self.assertAlmostEqual(
            self.engine.broker.portfolio.positions["AAPL"].shares, 0.0
        )
        self.assertAlmostEqual(
            self.engine.broker.portfolio.positions["MSFT"].shares, 0.0
        )

    def test_reset_halt_clears_state(self):
        self.engine._halted = True
        self.engine._peak_equity = 500.0
        self.engine._needs_liquidation = True
        self.engine.reset_halt()
        self.assertFalse(self.engine._halted)
        self.assertIsNone(self.engine._peak_equity)
        self.assertFalse(self.engine._needs_liquidation)

    def test_halt_logged_at_warning(self):
        self.engine._peak_equity = 1_000.0
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            self.engine._find_stop_loss_triggers({}, {}, 899.0)
        self.assertTrue(any("TRADING HALTED" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Stop-loss
# ---------------------------------------------------------------------------


class TestStopLoss(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = _make_engine(stop_loss_pct=0.05)
        self.engine.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        self.engine.broker.portfolio.cash = 900.0

    def _run_trigger_check(self, prices):
        positions = {"AAPL": Position(shares=10, avg_price=10.0)}
        return self.engine._find_stop_loss_triggers(positions, prices, equity=1_000.0)

    async def test_stop_loss_triggers_below_threshold(self):
        # 9.4 < 10.0 * (1 - 0.05) = 9.5 → trigger
        triggered = self._run_trigger_check({"AAPL": 9.4})
        trade_date = date(2026, 2, 15)
        results = await self.engine._execute_stop_loss_closes(
            triggered, {"AAPL": 9.4}, trade_date
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertTrue(results[0].filled)
        self.assertAlmostEqual(
            self.engine.broker.portfolio.positions["AAPL"].shares, 0.0
        )

    def test_stop_loss_does_not_trigger_above_threshold(self):
        # 9.6 > 9.5 → no trigger
        triggered = self._run_trigger_check({"AAPL": 9.6})
        self.assertEqual(triggered, [])

    async def test_stop_loss_sell_adds_proceeds_to_cash(self):
        # proceeds = 10 shares * $9.4 = $94; cash = $900 + $94 = $994
        triggered = self._run_trigger_check({"AAPL": 9.4})
        await self.engine._execute_stop_loss_closes(
            triggered, {"AAPL": 9.4}, date(2026, 2, 15)
        )
        self.assertAlmostEqual(self.engine.broker.portfolio.cash, 994.0)

    async def test_stop_loss_result_has_correct_trigger_and_reason(self):
        triggered = self._run_trigger_check({"AAPL": 9.4})
        results = await self.engine._execute_stop_loss_closes(
            triggered, {"AAPL": 9.4}, date(2026, 2, 15)
        )
        self.assertEqual(results[0].trigger, "stop_loss")
        self.assertEqual(results[0].reason, "ok")

    def test_symbol_missing_from_prices_is_skipped(self):
        triggered = self._run_trigger_check({"MSFT": 50.0})
        self.assertEqual(triggered, [])

    def test_floating_point_residue_shares_not_retriggered(self):
        """Residue like 1e-12 left over from a prior sell must not re-trigger."""
        positions = {"AAPL": Position(shares=1e-10, avg_price=10.0)}
        triggered = self.engine._find_stop_loss_triggers(
            positions, {"AAPL": 9.4}, equity=1_000.0
        )
        self.assertEqual(triggered, [])

    async def test_stop_loss_logged_at_warning(self):
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self.engine._execute_stop_loss_closes(
                ["AAPL"], {"AAPL": 9.4}, date(2026, 2, 15)
            )
        self.assertTrue(any("Stop-loss trigger" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Stop-loss vs. PDT block interaction
# ---------------------------------------------------------------------------


class TestStopLossPdtDedup(unittest.IsolatedAsyncioTestCase):
    """A stop-loss close that's blocked by the PDT rule must not repeatedly
    attempt execution or repeatedly warn on every tick."""

    async def test_pdt_blocked_stop_loss_skips_execution(self):
        engine = _make_engine(allow_intraday=False, stop_loss_pct=0.05)
        today = date(2026, 2, 15)
        engine._record_trade("AAPL", today, "BUY")  # blocks a same-day SELL
        engine.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        engine.execute_target_exposure = AsyncMock()

        await engine._execute_stop_loss_closes(["AAPL"], {"AAPL": 9.4}, today)

        engine.execute_target_exposure.assert_not_called()

    async def test_pdt_blocked_stop_loss_warns_once_not_per_tick(self):
        engine = _make_engine(allow_intraday=False, stop_loss_pct=0.05)
        today = date(2026, 2, 15)
        engine._record_trade("AAPL", today, "BUY")
        engine.broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=10.0)
        engine.execute_target_exposure = AsyncMock()

        with self.assertLogs("trading.engine", level="WARNING") as cm:
            for _ in range(5):  # simulate 5 ticks all re-finding the same trigger
                await engine._execute_stop_loss_closes(["AAPL"], {"AAPL": 9.4}, today)

        block_warnings = [m for m in cm.output if "blocked by PDT" in m]
        self.assertEqual(len(block_warnings), 1)

    async def test_non_blocked_stop_loss_still_executes(self):
        """Symbols that are NOT PDT-blocked must still be closed as before."""
        engine = _make_engine(allow_intraday=False, stop_loss_pct=0.05)
        today = date(2026, 2, 15)
        # No opposite-direction trade recorded for MSFT today, so it's not blocked.
        engine.broker.portfolio.positions["MSFT"] = Position(shares=5, avg_price=50.0)
        engine.execute_target_exposure = AsyncMock(return_value=None)

        await engine._execute_stop_loss_closes(["MSFT"], {"MSFT": 47.0}, today)

        engine.execute_target_exposure.assert_called_once()


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

    async def test_buy_then_sell_same_day_blocked(self):
        await self.engine.execute_trade("AAPL", 20.0, 10.0, self.today)
        result = await self.engine.execute_trade("AAPL", -20.0, 10.0, self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_buy_yesterday_sell_today_allowed(self):
        self.engine._record_trade("AAPL", self.yesterday, "BUY")
        result = await self.engine.execute_trade("AAPL", -20.0, 10.0, self.today)
        self.assertTrue(result.filled)

    async def test_sell_then_buy_same_day_blocked(self):
        self.engine.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        await self.engine.execute_trade("AAPL", -10.0, 10.0, self.today)
        result = await self.engine.execute_trade("AAPL", 20.0, 10.0, self.today)
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_allow_intraday_bypasses_pdt(self):
        engine = _make_engine(allow_intraday=True)
        await engine.execute_trade("AAPL", 20.0, 10.0, self.today)
        result = await engine.execute_trade("AAPL", -20.0, 10.0, self.today)
        self.assertTrue(result.filled)

    async def test_multiple_buys_each_counted_separately(self):
        await self.engine.execute_trade("AAPL", 20.0, 10.0, self.today)
        await self.engine.execute_trade("AAPL", 10.0, 10.0, self.today)
        result = await self.engine.execute_trade("AAPL", -30.0, 10.0, self.today)
        self.assertEqual(result.reason, "pdt_blocked")

    async def test_different_symbols_are_independent(self):
        await self.engine.execute_trade("AAPL", 20.0, 10.0, self.today)
        self.engine.broker.portfolio.positions["MSFT"] = Position(
            shares=10, avg_price=10.0
        )
        result = await self.engine.execute_trade("MSFT", -10.0, 10.0, self.today)
        self.assertTrue(result.filled)

    async def test_pdt_block_logged_at_warning(self):
        await self.engine.execute_trade("AAPL", 20.0, 10.0, self.today)
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self.engine.execute_trade("AAPL", -20.0, 10.0, self.today)
        self.assertTrue(any("PDT block" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Stale prediction
# ---------------------------------------------------------------------------


class TestStalePrediction(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.today = date(2026, 2, 15)
        self.engine = _make_engine(stale_prediction_hours=2.0)

    async def test_none_prediction_ts_is_always_allowed(self):
        result = await self.engine.execute_trade(
            "AAPL", 20.0, 10.0, self.today, None
        )
        self.assertTrue(result.filled)

    async def test_very_old_prediction_is_blocked(self):
        old_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = await self.engine.execute_trade(
            "AAPL", 20.0, 10.0, self.today, old_ts
        )
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "stale_prediction")

    async def test_fresh_prediction_is_allowed(self):
        fresh_ts = datetime.now(timezone.utc)
        result = await self.engine.execute_trade(
            "AAPL", 20.0, 10.0, self.today, fresh_ts
        )
        self.assertTrue(result.filled)

    async def test_stale_prediction_only_blocks_buys(self):
        old_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.engine.broker.portfolio.positions["AAPL"] = Position(
            shares=10, avg_price=10.0
        )
        result = await self.engine.execute_trade(
            "AAPL", -10.0, 10.0, self.today, old_ts
        )
        self.assertTrue(result.filled)

    async def test_stale_prediction_logged_at_warning(self):
        old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
        with self.assertLogs("trading.engine", level="WARNING") as cm:
            await self.engine.execute_trade("AAPL", 20.0, 10.0, self.today, old_ts)
        self.assertTrue(any("Stale prediction" in m for m in cm.output))


# ---------------------------------------------------------------------------
# Backtest — stop-loss triggers on raw price ticks between predictions
# ---------------------------------------------------------------------------


class TestBacktest(unittest.IsolatedAsyncioTestCase):
    """
    Verify that the backtest loop iterates raw price timestamps so stop-losses
    trigger at the correct tick, even when no prediction exists for that symbol.
    """

    def _make_raw(self, rows):
        return pd.DataFrame(rows, columns=["symbol", "timestamp", "close"])

    def _make_predictions(self, rows):
        return pd.DataFrame(
            rows, columns=["symbol", "timestamp", "close", "prediction", "variance"]
        )

    def _make_policy_mock(self):
        """A policy that allocates 100% to the first symbol every tick."""
        from unittest.mock import MagicMock
        import numpy as np
        policy = MagicMock()
        # apply returns an array with 1.0 for each row (will be re-normalised downstream)
        policy.apply = MagicMock(
            side_effect=lambda df: pd.Series([1.0] * len(df), index=df.index)
        )
        policy.adapt = MagicMock()
        return policy

    async def test_stop_loss_triggers_between_prediction_timestamps(self):
        """
        Predictions exist at t=1 and t=3. The price drops below stop-loss at t=2
        (raw tick only). The position should be closed at t=2, not later.
        """
        raw = self._make_raw([
            ("AAPL", 1, 100.0),
            ("AAPL", 2, 80.0),   # <-- price crash: 80 < 100 * 0.95 = 95
            ("AAPL", 3, 80.0),
        ])
        predictions = self._make_predictions([
            ("AAPL", 1, 100.0, 0.05, 0.001),
            ("AAPL", 3, 80.0, 0.05, 0.001),
        ])

        broker = PaperBroker(raw, starting_cash=10_000.0)
        # Prime the broker with an existing AAPL position bought at $100
        broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=100.0)
        broker.portfolio.cash = 9_000.0

        policy = self._make_policy_mock()
        engine = TradingEngine(
            broker,
            policy=policy,
            df=predictions,
            stop_loss_pct=0.05,
            rebalance_interval_hours=0.0,
        )

        await engine.backtest()

        # Position should have been force-closed by stop-loss at t=2
        aapl_shares = broker.portfolio.positions["AAPL"].shares
        self.assertAlmostEqual(aapl_shares, 0.0, places=4)

    async def test_no_stop_loss_when_price_stays_above_threshold(self):
        raw = self._make_raw([
            ("AAPL", 1, 100.0),
            ("AAPL", 2, 96.0),   # 96 > 95 → no trigger
            ("AAPL", 3, 97.0),
        ])
        predictions = self._make_predictions([
            ("AAPL", 1, 100.0, 0.05, 0.001),
        ])

        broker = PaperBroker(raw, starting_cash=10_000.0)
        broker.portfolio.positions["AAPL"] = Position(shares=10, avg_price=100.0)
        broker.portfolio.cash = 9_000.0

        policy = self._make_policy_mock()
        engine = TradingEngine(
            broker,
            policy=policy,
            df=predictions,
            stop_loss_pct=0.05,
            rebalance_interval_hours=0.0,
        )

        await engine.backtest()

        # Position should not have been closed
        aapl_shares = broker.portfolio.positions["AAPL"].shares
        self.assertGreater(aapl_shares, 0.0)

    async def test_backtest_raises_without_df(self):
        broker = _make_broker()
        engine = TradingEngine(broker)
        with self.assertRaises(ValueError):
            await engine.backtest()

    async def test_backtest_raises_without_policy(self):
        raw = self._make_raw([("AAPL", 1, 100.0)])
        predictions = self._make_predictions([("AAPL", 1, 100.0, 0.05, 0.001)])
        broker = _make_broker(raw)
        engine = TradingEngine(broker, df=predictions)
        with self.assertRaises(ValueError):
            await engine.backtest()


# ---------------------------------------------------------------------------
# Trade log
# ---------------------------------------------------------------------------


class TestTradeLog(unittest.IsolatedAsyncioTestCase):

    async def test_trade_log_contains_only_filled_trades(self):
        """PDT-blocked and other unfilled trades must not appear in _trade_log."""
        engine = _make_engine(allow_intraday=False)
        today = date(2026, 2, 15)
        engine._last_tick_ts = 1_000.0

        # BUY fills → should be logged
        await engine.execute_trade("AAPL", 5.0, 100.0, today)
        # SELL is PDT-blocked (same day as the buy) → must NOT be logged
        blocked = await engine.execute_trade("AAPL", -5.0, 100.0, today)

        self.assertEqual(blocked.reason, "pdt_blocked")
        self.assertEqual(len(engine._trade_log), 1)
        self.assertEqual(engine._trade_log[0]["symbol"], "AAPL")
        self.assertEqual(engine._trade_log[0]["direction"], "BUY")

    async def test_trade_log_entry_has_required_fields(self):
        """Each trade log entry must contain ts, date, symbol, direction, shares, price, value, trigger."""
        engine = _make_engine()
        engine._last_tick_ts = 2_000.0
        today = date(2026, 2, 15)

        await engine.execute_trade("MSFT", 2.0, 50.0, today, trigger="rebalance")

        entry = engine._trade_log[0]
        for field in ("ts", "date", "symbol", "direction", "shares", "price", "value", "trigger"):
            self.assertIn(field, entry)
        self.assertAlmostEqual(entry["ts"], 2_000.0)
        self.assertEqual(entry["date"], "2026-02-15")
        self.assertEqual(entry["symbol"], "MSFT")
        self.assertAlmostEqual(entry["shares"], 2.0)
        self.assertAlmostEqual(entry["value"], 100.0)
        self.assertEqual(entry["trigger"], "rebalance")


# ---------------------------------------------------------------------------
# PDT rebalance pre-screening
# ---------------------------------------------------------------------------


class TestPDTRebalancePrescreening(unittest.IsolatedAsyncioTestCase):

    async def test_pdt_rebalance_prescreening_skips_silently(self):
        """After a BUY is recorded, a second rebalance attempt to SELL should be
        silently skipped without calling execute_target_exposure."""
        engine = _make_engine(allow_intraday=False)
        today = date(2026, 2, 15)

        # Record a BUY so that selling AAPL today would be a PDT round-trip
        engine._record_trade("AAPL", today, "BUY")
        engine.broker.portfolio.positions["AAPL"] = Position(shares=5, avg_price=100.0)

        # df_ts wants 0% allocation → would be a sell
        df_ts = pd.DataFrame([{"symbol": "AAPL", "close": 100.0, "portfolio_weight": 0.0}])
        engine.execute_target_exposure = AsyncMock()

        await engine._run_rebalance(df_ts, equity=1_000.0, trade_date=today)

        engine.execute_target_exposure.assert_not_called()

    async def test_non_blocked_symbol_still_executes(self):
        """Symbols that are NOT PDT-blocked must still be traded during rebalance."""
        engine = _make_engine(allow_intraday=False)
        today = date(2026, 2, 15)

        # No trades recorded for MSFT, so it is not PDT-blocked
        df_ts = pd.DataFrame([{"symbol": "MSFT", "close": 50.0, "portfolio_weight": 0.5}])
        engine.execute_target_exposure = AsyncMock(
            return_value=TradeResult("MSFT", 10.0, 50.0, True)
        )

        await engine._run_rebalance(df_ts, equity=1_000.0, trade_date=today)

        engine.execute_target_exposure.assert_called_once()


# ---------------------------------------------------------------------------
# Rebalance summary logging
# ---------------------------------------------------------------------------


class TestRebalanceSummaryLogging(unittest.IsolatedAsyncioTestCase):
    """_run_rebalance must always log a one-line summary so a silent rebalance
    (no trades) is distinguishable from one that never ran."""

    async def test_logs_summary_when_a_trade_executes(self):
        engine = _make_engine()
        today = date(2026, 2, 15)
        # 20% of $1000 equity @ $10/share = 20 shares, well within the $1000 cash.
        df_ts = pd.DataFrame([
            {"symbol": "AAPL", "close": 10.0, "portfolio_weight": 0.2},
            {"symbol": "MSFT", "close": 10.0, "portfolio_weight": 0.0},
        ])

        with self.assertLogs("trading.engine", level="INFO") as cm:
            await engine._run_rebalance(df_ts, equity=1_000.0, trade_date=today)

        summary = [m for m in cm.output if "Rebalance:" in m]
        self.assertEqual(len(summary), 1)
        self.assertIn("2/2 symbol(s) evaluated", summary[0])
        self.assertIn("1 trade(s) executed", summary[0])

    async def test_logs_zero_trades_when_all_no_change(self):
        engine = _make_engine()
        today = date(2026, 2, 15)
        # No position held and 0% target -> shares_delta rounds to 0 -> no_change.
        df_ts = pd.DataFrame([{"symbol": "AAPL", "close": 10.0, "portfolio_weight": 0.0}])

        with self.assertLogs("trading.engine", level="INFO") as cm:
            await engine._run_rebalance(df_ts, equity=1_000.0, trade_date=today)

        summary = next(m for m in cm.output if "Rebalance:" in m)
        self.assertIn("1/1 symbol(s) evaluated", summary)
        self.assertIn("0 trade(s) executed", summary)

    async def test_summary_excludes_pdt_blocked_symbols_from_evaluated_count(self):
        engine = _make_engine(allow_intraday=False)
        today = date(2026, 2, 15)
        engine._record_trade("AAPL", today, "BUY")  # blocks a same-day SELL
        engine.broker.portfolio.positions["AAPL"] = Position(shares=5, avg_price=10.0)
        df_ts = pd.DataFrame([{"symbol": "AAPL", "close": 10.0, "portfolio_weight": 0.0}])

        with self.assertLogs("trading.engine", level="INFO") as cm:
            await engine._run_rebalance(df_ts, equity=1_000.0, trade_date=today)

        summary = next(m for m in cm.output if "Rebalance:" in m)
        self.assertIn("0/1 symbol(s) evaluated", summary)
        self.assertIn("0 trade(s) executed", summary)


if __name__ == "__main__":
    unittest.main()
