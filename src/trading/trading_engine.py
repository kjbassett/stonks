import asyncio
import logging
import pandas as pd
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Dict, List, NamedTuple, Optional

from src.trading.executor import OrderExecutor
from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.brokers.paper_broker import PaperBroker
from src.trading.portfolio import Position
from src.trading.strategy import StrategyPolicy, PredictionThresholdRule
from src.utils.project_utilities import config


_log = logging.getLogger("trading.engine")


class SafetySignal(NamedTuple):
    """
    Result of perform_safety_checks(). Describes what was detected; no trades are executed.

    step() reads this signal to decide whether to liquidate, close stop-losses,
    skip rebalancing, or proceed normally.
    """

    is_halted: bool
    needs_liquidation: bool
    stop_loss_symbols: List[str]
    positions: Dict[str, Position]


class TradingEngine:
    """
    Drives a trading policy and owns all safety guardrails.

    Safety hierarchy (all live here, not in OrderExecutor):
    - Tick-level: drawdown monitoring, stop-loss detection, halt liquidation
    - Trade-level: PDT enforcement, stale-prediction blocking, halt-buy blocking

    Parameters
    ----------
    policy : StrategyPolicy, optional
        Required for backtest(). Optional for manual trades via execute_manual_trade().
    df : pd.DataFrame, optional
        Historical predictions for backtesting. Required columns:
        symbol, timestamp (unix seconds), close, prediction, variance.
    paper_trading : bool
        If True (default), use PaperBroker from config. If False, pass a broker.
    broker : BaseBroker, optional
        Required when paper_trading=False.
    rebalance_interval_hours : float
        Minimum gap between rebalances. 0.0 = every timestamp.
    allow_intraday : bool
        If False, block same-day round-trips (PDT rule).
    max_drawdown_pct : float
        Halt trading when portfolio drops this fraction from peak equity.
    stop_loss_pct : float
        Auto-close positions that lose this fraction from average cost.
    stale_prediction_hours : float
        Block buys when the prediction is older than this many hours.
    """

    def __init__(
        self,
        policy: Optional[StrategyPolicy] = None,
        df: Optional[pd.DataFrame] = None,
        paper_trading: bool = True,
        broker: Optional[BaseBroker] = None,
        rebalance_interval_hours: float = 1.0,
        allow_intraday: bool = True,
        max_drawdown_pct: float = 0.10,
        stop_loss_pct: float = 0.05,
        stale_prediction_hours: float = 2.0,
    ) -> None:
        self.df = df.sort_values("timestamp") if df is not None else None
        self.rebalance_interval_hours = rebalance_interval_hours
        self.allow_intraday = allow_intraday
        self.max_drawdown_pct = max_drawdown_pct
        self.stop_loss_pct = stop_loss_pct
        self.stale_prediction_hours = stale_prediction_hours
        self.policy = policy
        self.last_prices: Dict[str, float] = {}
        self._last_rebalance_ts: Optional[float] = None
        self._last_equity: Optional[float] = None
        self._peak_equity: Optional[float] = None
        self._halted: bool = False
        self._needs_liquidation: bool = False
        self._intraday_buys: defaultdict = defaultdict(int)
        self._intraday_sells: defaultdict = defaultdict(int)
        self.executor = OrderExecutor(broker=self._init_broker(paper_trading, broker))

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_broker(
        self, paper_trading: bool, broker: Optional[BaseBroker]
    ) -> BaseBroker:
        """Return the configured broker: PaperBroker from config, or the provided one."""
        if paper_trading:
            cfg = config["trading_rules"]
            return PaperBroker(
                starting_cash=cfg["starting_cash"],
                flat_fee=cfg["flat_fee"],
                percent_fee=cfg["percent_fee"],
            )
        if broker is None:
            raise ValueError(
                "For live trading, pass an authenticated broker. "
                "Use: broker = await SchwabBroker.from_auth()"
            )
        return broker

    # ------------------------------------------------------------------
    # Safety state helpers
    # ------------------------------------------------------------------

    def _update_drawdown(self, equity: float) -> None:
        """Update peak equity and set halt/liquidation flags if threshold breached."""
        if self._peak_equity is None or equity > self._peak_equity:
            self._peak_equity = equity
        if not self._halted and equity < self._peak_equity * (1.0 - self.max_drawdown_pct):
            self._halted = True
            self._needs_liquidation = True
            _log.warning(
                f"TRADING HALTED: equity ${equity:,.2f} dropped "
                f">{self.max_drawdown_pct:.0%} from peak ${self._peak_equity:,.2f}."
            )

    def _pdt_allowed(self, symbol: str, trade_date: date, direction: str) -> bool:
        """Return False and warn if this trade would complete a same-day round-trip."""
        if self.allow_intraday:
            return True
        if direction == "BUY":
            opposite, action, label = self._intraday_sells, "buying", "sell(s)"
        else:
            opposite, action, label = self._intraday_buys, "selling", "buy(s)"
        count = opposite.get((symbol, trade_date), 0)
        if count > 0:
            _log.warning(
                f"PDT block: {action} {symbol} would complete a same-day round-trip "
                f"({count} same-day {label} on {trade_date}). "
                f"Set allow_intraday=True or ensure account >= $25k."
            )
            return False
        return True

    def _record_trade(self, symbol: str, trade_date: date, direction: str) -> None:
        """Record a completed trade for intraday PDT tracking."""
        key = (symbol, trade_date)
        if direction == "BUY":
            self._intraday_buys[key] += 1
        else:
            self._intraday_sells[key] += 1

    def _prediction_fresh(self, prediction_ts: Optional[datetime]) -> bool:
        """Return True if prediction_ts is within the staleness threshold, or if None."""
        if prediction_ts is None:
            return True
        age_h = (datetime.now(timezone.utc) - prediction_ts).total_seconds() / 3600.0
        return age_h <= self.stale_prediction_hours

    def _is_rebalance_due(self, ts: float) -> bool:
        """Return True if enough time has elapsed since the last rebalance."""
        if self._last_rebalance_ts is None:
            return True
        return (ts - self._last_rebalance_ts) >= self.rebalance_interval_hours * 3600

    def _find_stop_loss_triggers(
        self, positions: Dict[str, Position], prices: Dict[str, float]
    ) -> List[str]:
        """Return symbols whose price is below their stop-loss threshold."""
        triggered = []
        for sym, pos in positions.items():
            if sym not in prices or pos.shares <= 0 or pos.avg_price <= 0:
                continue
            if prices[sym] < pos.avg_price * (1.0 - self.stop_loss_pct):
                triggered.append(sym)
        return triggered

    def reset_halt(self) -> None:
        """Clear the drawdown halt so automated trading can resume."""
        self._halted = False
        self._peak_equity = None
        self._needs_liquidation = False

    async def restore_intraday_state(self) -> None:
        """Reload today's fills from broker to restore PDT tracking after a restart."""
        today = date.today()
        fills = await self.executor.broker.get_today_fills()
        for symbol, direction in fills:
            self._record_trade(symbol, today, direction)

    # ------------------------------------------------------------------
    # Tick-level safety: signal detection (no trades)
    # ------------------------------------------------------------------

    async def perform_safety_checks(self, prices: Dict[str, float]) -> SafetySignal:
        """
        Evaluate all tick-level safety conditions. Does not execute any trades.

        Fetches current equity and positions, updates drawdown state, and identifies
        any stop-loss triggers. Returns a SafetySignal that step() uses to decide
        whether to liquidate, close stop-losses, or proceed with normal rebalancing.
        """
        equity = await self.executor.get_equity(prices)
        self._update_drawdown(equity)
        positions = await self.executor.broker.get_positions()
        stop_loss_symbols = self._find_stop_loss_triggers(positions, prices)
        return SafetySignal(
            is_halted=self._halted,
            needs_liquidation=self._needs_liquidation,
            stop_loss_symbols=stop_loss_symbols,
            positions=positions,
        )

    # ------------------------------------------------------------------
    # Forced-close execution (called by step() based on SafetySignal)
    # ------------------------------------------------------------------

    async def _execute_liquidation_sweep(
        self, positions: Dict[str, Position], prices: Dict[str, float]
    ) -> List[TradeResult]:
        """Close all open positions as part of a drawdown-halt liquidation."""
        self._needs_liquidation = False
        symbols = [s for s in positions if s in prices and positions[s].shares > 0]
        if not symbols:
            return []
        _log.warning(f"Drawdown liquidation: closing {len(symbols)} position(s).")
        results = await asyncio.gather(
            *[self.executor.execute_close(s, prices[s], positions[s]) for s in symbols]
        )
        for r in results:
            r.trigger = "drawdown_halt"
        return list(results)

    async def _force_close(
        self, symbol: str, pos: Position, price: float, trade_date: date
    ) -> TradeResult:
        """Close a position due to stop-loss. Bypasses PDT — protection takes priority."""
        _log.warning(
            f"Stop-loss trigger: {symbol} current=${price:.2f} < "
            f"avg_cost=${pos.avg_price:.2f} * {1.0 - self.stop_loss_pct:.2f}. Closing."
        )
        result = await self.executor.execute_close(symbol, price, pos)
        result.trigger = "stop_loss"
        self._record_trade(symbol, trade_date, "SELL")
        return result

    async def _execute_stop_loss_closes(
        self,
        symbols: List[str],
        positions: Dict[str, Position],
        prices: Dict[str, float],
    ) -> List[TradeResult]:
        """Force-close all stop-loss-triggered positions using pre-fetched data."""
        trade_date = date.today()
        results = await asyncio.gather(
            *[self._force_close(sym, positions[sym], prices[sym], trade_date)
              for sym in symbols]
        )
        return list(results)

    # ------------------------------------------------------------------
    # Trade-level safety + execution
    # ------------------------------------------------------------------

    async def _execute_and_record(
        self,
        symbol: str,
        shares_delta: float,
        price: float,
        trade_date: date,
        prediction_ts: Optional[datetime],
    ) -> TradeResult:
        """Apply per-trade safety checks and execute. Records for PDT on fill."""
        direction = "BUY" if shares_delta > 0 else "SELL"
        if self._halted and direction == "BUY":
            return TradeResult(symbol, 0.0, price, False, "halted")
        if not self._pdt_allowed(symbol, trade_date, direction):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")
        if direction == "BUY" and not self._prediction_fresh(prediction_ts):
            _log.warning(f"Stale prediction for {symbol}: blocking buy.")
            return TradeResult(symbol, 0.0, price, False, "stale_prediction")
        result = await self.executor.execute_shares(symbol, shares_delta, price)
        if result.filled and result.reason != "no_change":
            self._record_trade(symbol, trade_date, direction)
        return result

    # ------------------------------------------------------------------
    # Manual trade
    # ------------------------------------------------------------------

    async def execute_manual_trade(
        self, symbol: str, shares_delta: float, price: float
    ) -> TradeResult:
        """Execute a single manual trade after drawdown and PDT safety checks."""
        trade_date = date.today()
        direction = "BUY" if shares_delta > 0 else "SELL"
        if self._halted:
            _log.warning(f"Manual trade blocked: trading is halted. symbol={symbol}")
            return TradeResult(symbol, 0.0, price, False, "halted", trigger="manual")
        if not self._pdt_allowed(symbol, trade_date, direction):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked", trigger="manual")
        result = await self.executor.execute_shares(symbol, shares_delta, price)
        if result.filled and result.reason != "no_change":
            self._record_trade(symbol, trade_date, direction)
        return result

    # ------------------------------------------------------------------
    # Rebalancing helpers
    # ------------------------------------------------------------------

    async def _run_rebalance(
        self,
        df_ts: pd.DataFrame,
        equity: float,
        pred_ts: Optional[datetime],
        trade_date: date,
    ) -> None:
        """Execute rebalancing trades for one timestamp: sells before buys."""
        positions = await self.executor.broker.get_positions()
        sell_coros, buy_coros = [], []
        for _, row in df_ts.iterrows():
            symbol = row["symbol"]
            price = row["close"]
            target_exposure = row["portfolio_weight"]
            pos = positions.get(symbol)
            current_value = pos.shares * price if pos else 0.0
            target_value = equity * target_exposure
            pos_shares = pos.shares if pos else 0.0
            shares_delta = (equity * target_exposure - pos_shares * price) / price
            coro = self._execute_and_record(symbol, shares_delta, price, trade_date, pred_ts)
            (sell_coros if target_value < current_value else buy_coros).append(coro)
        await asyncio.gather(*sell_coros)
        await asyncio.gather(*buy_coros)

    async def _run_adapt(self, df_ts: pd.DataFrame, did_rebalance: bool) -> None:
        """Update policy parameters based on the realized return since the last tick."""
        equity = await self.executor.get_equity(self.last_prices)
        if self._last_equity is not None:
            realized_return = (equity - self._last_equity) / self._last_equity
            utilization = float(df_ts["portfolio_weight"].sum()) if did_rebalance else 0.0
            self.policy.adapt(realized_return, utilization)
        self._last_equity = equity

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    async def step(self, df_ts: pd.DataFrame, adapt_policy: bool = True) -> None:
        """
        Process one snapshot of weighted recommendations.

        Calls perform_safety_checks() to detect problems, then acts on the returned
        SafetySignal: liquidates, closes stop-losses, or proceeds with rebalancing.

        Parameters
        ----------
        df_ts : pd.DataFrame
            Must contain: symbol, timestamp, close, portfolio_weight.
            Optionally: prediction_ts.
        adapt_policy : bool
            If True (default), call policy.adapt() after each tick.
        """
        for _, row in df_ts.iterrows():
            self.last_prices[row["symbol"]] = row["close"]

        safety = await self.perform_safety_checks(self.last_prices)

        if safety.needs_liquidation:
            await self._execute_liquidation_sweep(safety.positions, self.last_prices)
        elif safety.stop_loss_symbols:
            await self._execute_stop_loss_closes(
                safety.stop_loss_symbols, safety.positions, self.last_prices
            )

        ts = float(df_ts["timestamp"].max())
        should_rebalance = self._is_rebalance_due(ts)
        did_rebalance = False

        if should_rebalance and not safety.is_halted:
            equity = await self.executor.get_equity(self.last_prices)
            pred_ts = self._parse_prediction_ts(df_ts)
            trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            await self._run_rebalance(df_ts, equity, pred_ts, trade_date)
            did_rebalance = True

        if should_rebalance:
            self._last_rebalance_ts = ts

        if adapt_policy and self.policy is not None:
            await self._run_adapt(df_ts, did_rebalance)

    async def backtest(self, adapt_policy: bool = True) -> dict:
        """
        Run the policy over the full historical DataFrame passed at init.

        Returns
        -------
        dict with keys: total_equity, policy, executor, fees_paid
        """
        if self.df is None:
            raise ValueError(
                "df is required for backtest(). Pass df at init, or use step() for production."
            )
        if self.policy is None:
            raise ValueError("policy is required for backtest().")
        self._last_rebalance_ts = None
        self._last_equity = None
        await self.restore_intraday_state()
        for _, df_ts in self.df.groupby("timestamp", sort=True):
            df_ts = df_ts.copy()
            df_ts["portfolio_weight"] = self.policy.apply(df_ts)
            await self.step(df_ts, adapt_policy=adapt_policy)
        equity = (
            self._last_equity
            if self._last_equity is not None
            else await self.executor.get_equity(self.last_prices)
        )
        return {
            "total_equity": equity,
            "policy": self.policy,
            "executor": self.executor,
            "fees_paid": self.executor.get_fees_paid(),
        }

    @staticmethod
    def _parse_prediction_ts(df_ts: pd.DataFrame) -> Optional[datetime]:
        """Extract a timezone-aware prediction timestamp from the dataframe if present."""
        if "prediction_ts" not in df_ts.columns:
            return None
        val = df_ts["prediction_ts"].dropna()
        if val.empty:
            return None
        return datetime.fromtimestamp(float(val.iloc[0]), tz=timezone.utc)


async def train_trading_policy(
    predictions: pd.DataFrame,
    rebalance_interval_hours: float = 1.0,
    allow_intraday: bool = False,
    max_drawdown_pct: float = 0.10,
    stop_loss_pct: float = 0.05,
):
    """
    Train a trading policy via market simulation.

    Returns:
        policy: StrategyPolicy (stateful, trained)
        fitness: float (final equity / starting_cash)
    """
    required_cols = {"symbol", "timestamp", "close", "prediction", "variance"}
    missing = required_cols - set(predictions.columns)
    if missing:
        raise ValueError(f"Predictions missing required columns: {missing}")

    policy = StrategyPolicy(
        [
            PredictionThresholdRule(
                threshold=1,
                aggressiveness=1.0,
                learning_rate=0.01,
            )
        ]
    )

    engine = TradingEngine(
        policy=policy,
        df=predictions,
        paper_trading=True,
        rebalance_interval_hours=rebalance_interval_hours,
        allow_intraday=allow_intraday,
        max_drawdown_pct=max_drawdown_pct,
        stop_loss_pct=stop_loss_pct,
    )

    result = await engine.backtest()

    starting_cash = config["trading_rules"]["starting_cash"]
    returns = result["total_equity"] / starting_cash
    policy = result["policy"]
    return policy, returns


async def apply_trading_policy(
    predictions: pd.DataFrame,
    policy: StrategyPolicy,
):
    """
    Apply a trained trading policy to new data.

    Returns:
        recommendations: pd.DataFrame with portfolio_weight column added
    """
    required_cols = {"symbol", "timestamp", "prediction", "variance"}
    missing = required_cols - set(predictions.columns)
    if missing:
        raise ValueError(f"Predictions missing required columns: {missing}")
    predictions = predictions.copy()
    predictions["portfolio_weight"] = policy.apply(predictions)

    return predictions
