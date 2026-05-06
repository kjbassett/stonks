import asyncio
import logging
import pandas as pd
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.brokers.paper_broker import PaperBroker
from src.trading.portfolio import Position
from src.trading.strategy import StrategyPolicy, InformationRatioRule
from src.utils.project_utilities import config


_log = logging.getLogger("trading.engine")


class TradingEngine:
    """
    Drives a trading policy and owns all safety guardrails.

    Safety hierarchy:
    - Tick-level: stop-loss detection (runs on every raw price tick in backtest)
    - Trade-level: PDT enforcement, stale-prediction blocking, halt-buy blocking

    Parameters
    ----------
    broker : BaseBroker
        The broker to execute trades through.
    policy : StrategyPolicy, optional
        Required for backtest(). Optional for step()-based live trading.
    df : pd.DataFrame, optional
        Filtered prediction data for backtesting. Required columns:
        symbol, timestamp (unix seconds), close, prediction, variance.
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
        broker: BaseBroker,
        policy: Optional[StrategyPolicy] = None,
        df: Optional[pd.DataFrame] = None,
        rebalance_interval_hours: float = 1.0,
        allow_intraday: bool = True,
        max_drawdown_pct: float = 0.10,
        stop_loss_pct: float = 0.05,
        stale_prediction_hours: float = 2.0,
    ) -> None:
        self.broker = broker
        self.df = df.sort_values("timestamp") if df is not None else None
        self.rebalance_interval_hours = rebalance_interval_hours
        self.allow_intraday = allow_intraday
        self.max_drawdown_pct = max_drawdown_pct
        self.stop_loss_pct = stop_loss_pct
        self.stale_prediction_hours = stale_prediction_hours
        self.policy = policy
        self._last_rebalance_ts: Optional[float] = None
        self._last_equity: Optional[float] = None
        self._peak_equity: Optional[float] = None
        self._halted: bool = False
        self._needs_liquidation: bool = False
        self._intraday_buys: defaultdict = defaultdict(int)
        self._intraday_sells: defaultdict = defaultdict(int)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute_target_exposure(
        self,
        symbol: str,
        target_exposure: float,
        price: float,
        equity: float,
        trade_date: Optional[date] = None,
        trigger: Optional[str] = None,
    ) -> TradeResult:
        """
        Adjust symbol so its market value equals target_exposure * equity.
        Fetches the current position to compute the required delta, then calls execute_trade.
        """
        positions = await self.broker.get_positions()
        pos = positions.get(symbol, Position())
        shares_delta = (equity * target_exposure - pos.shares * price) / price
        return await self.execute_trade(
            symbol, shares_delta, price, trade_date, trigger=trigger,
        )

    async def execute_trade(
        self,
        symbol: str,
        shares_delta: float,
        price: float,
        trade_date: Optional[date] = None,
        prediction_ts: Optional[datetime] = None,
        trigger: Optional[str] = None,
    ) -> TradeResult:
        """
        Apply per-trade safety filters, then execute.
        trigger is stamped onto the TradeResult for audit purposes.
        """
        pdt_blocked_buys, pdt_blocked_sells = self._find_pdt_blocks(trade_date)
        reason = None
        direction = "BUY" if shares_delta > 0 else "SELL"
        if abs(shares_delta) < 1e-6:
            reason = "no_change"
        elif self._halted and direction == "BUY":
            _log.warning(f"Buy blocked: trading is halted. symbol={symbol}")
            reason = "halted"
        elif direction == "BUY" and symbol in pdt_blocked_buys:
            _log.warning(
                f"PDT block: buying {symbol} would complete a same-day round-trip. "
                f"Set allow_intraday=True or ensure account >= $25k."
            )
            reason = "pdt_blocked"
        elif direction == "SELL" and symbol in pdt_blocked_sells:
            _log.warning(
                f"PDT block: selling {symbol} would complete a same-day round-trip. "
                f"Set allow_intraday=True or ensure account >= $25k."
            )
            reason = "pdt_blocked"
        elif direction == "BUY" and not self._prediction_fresh(prediction_ts):
            _log.warning(f"Stale prediction for {symbol}: blocking buy.")
            reason = "stale_prediction"
        # if there is a reason to not trade
        if reason is not None:
            # return TradeResult with 0 shares, not filled
            result = TradeResult(symbol, 0.0, price, False, reason, trigger)
        else:
            result = await self.broker.fill_order(symbol, shares_delta, price)
            result.trigger = trigger

        if result.filled:
            _log.info(
                f"Trade executed: {symbol} {direction} {abs(result.shares_delta):.4f} shares "
                f"@ ${price:.2f} | reason={result.reason}"
            )
            self._record_trade(symbol, trade_date or date.today(), direction)
        elif not result.filled and result.reason != "no_change":
            _log.warning(f"Trade not filled: {symbol} {direction} | reason={result.reason}")
        return result

    # ------------------------------------------------------------------
    # Forced-close execution
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
        return list(await asyncio.gather(*[
            self.execute_target_exposure(
                s, 0.0, prices[s], 0.0, trigger="drawdown_halt"
            )
            for s in symbols
        ]))

    async def _execute_stop_loss_closes(
        self, symbols: List[str], prices: Dict[str, float], trade_date: date
    ) -> List[TradeResult]:
        """Force-close all stop-loss-triggered positions."""
        for sym in symbols:
            _log.warning(f"Stop-loss trigger: closing {sym} @ ${prices[sym]:.2f}.")
        return list(await asyncio.gather(*[
            self.execute_target_exposure(
                sym, 0.0, prices[sym], 0.0, trade_date, trigger="stop_loss"
            )
            for sym in symbols
        ]))

    # ------------------------------------------------------------------
    # Safety checks
    # ------------------------------------------------------------------

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

    def _find_pdt_blocks(self, trade_date: date) -> Tuple[List[str], List[str]]:
        """Return (blocked_buys, blocked_sells) based on today's intraday trade history."""
        if self.allow_intraday:
            return [], []
        blocked_buys = [
            sym for sym, d in self._intraday_sells
            if d == trade_date and self._intraday_sells[(sym, d)] > 0
        ]
        blocked_sells = [
            sym for sym, d in self._intraday_buys
            if d == trade_date and self._intraday_buys[(sym, d)] > 0
        ]
        return blocked_buys, blocked_sells

    def _find_stop_loss_triggers(
        self, positions: Dict[str, Position], prices: Dict[str, float], equity: float
    ) -> List[str]:
        """
        Return symbols whose price is below their stop-loss threshold.
        If the account drawdown exceeds max_drawdown_pct, halt trading and return
        all held symbols for liquidation.
        """
        if self._peak_equity is not None and equity < self._peak_equity * (1.0 - self.max_drawdown_pct):
            if not self._halted:
                self._halted = True
                self._needs_liquidation = True
                _log.warning(
                    f"TRADING HALTED: equity ${equity:,.2f} dropped "
                    f">{self.max_drawdown_pct:.0%} from peak ${self._peak_equity:,.2f}."
                )
            return [sym for sym, pos in positions.items() if pos.shares > 0]

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
        fills = await self.broker.get_today_fills()
        for symbol, direction in fills:
            self._record_trade(symbol, today, direction)

    # ------------------------------------------------------------------
    # Rebalancing helpers
    # ------------------------------------------------------------------

    async def _run_rebalance(
        self,
        df_ts: pd.DataFrame,
        equity: float,
        trade_date: date,
    ) -> None:
        """Execute rebalancing trades for one timestamp: sells before buys."""
        positions = await self.broker.get_positions()
        sell_coros, buy_coros = [], []
        for _, row in df_ts.iterrows():
            symbol, price, target_exposure = row["symbol"], row["close"], row["portfolio_weight"]
            pos = positions.get(symbol, Position())
            current_exposure = pos.market_value(price) / equity if equity > 0 else 0.0

            coro = self.execute_target_exposure(
                symbol, target_exposure, price, equity, trade_date=trade_date
            )
            (sell_coros if target_exposure < current_exposure else buy_coros).append(coro)
        await asyncio.gather(*sell_coros)
        await asyncio.gather(*buy_coros)

    async def _run_adapt(self, df_ts: pd.DataFrame) -> None:
        """Update policy parameters based on the realized return since the last tick."""
        equity = await self.broker.get_equity()
        if self._last_equity is not None:
            realized_return = (equity - self._last_equity) / self._last_equity
            utilization = float(df_ts["portfolio_weight"].sum())
            self.policy.adapt(realized_return, utilization)
        self._last_equity = equity

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    async def step(self, df_ts: pd.DataFrame, adapt_policy: bool = True) -> None:
        """
        Process one snapshot of weighted recommendations (live trading path).

        Checks all held positions for stop losses, then rebalances if due.

        Parameters
        ----------
        df_ts : pd.DataFrame
            Must contain: symbol, timestamp, close, portfolio_weight.
            Optionally: prediction_ts.
        adapt_policy : bool
            If True (default), call policy.adapt() after each tick.
        """
        ts = float(df_ts["timestamp"].max())
        trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()

        positions = await self.broker.get_positions()
        prices = {row["symbol"]: row["close"] for _, row in df_ts.iterrows()}
        equity = await self.broker.get_equity()

        if self._peak_equity is None or equity > self._peak_equity:
            self._peak_equity = equity

        stop_loss_symbols = self._find_stop_loss_triggers(positions, prices, equity)

        if self._halted:
            if self._needs_liquidation:
                await self._execute_liquidation_sweep(positions, prices)
            return

        await self._execute_stop_loss_closes(stop_loss_symbols, prices, trade_date)

        if not self._is_rebalance_due(ts):
            return

        equity = await self.broker.get_equity()
        df_ts = df_ts.copy()
        await self._run_rebalance(df_ts, equity, trade_date)
        if adapt_policy:
            await self._run_adapt(df_ts)
        self._last_rebalance_ts = ts

    async def backtest(self, adapt_policy: bool = True) -> dict:
        """
        Run the policy over the full historical DataFrame.

        The broker drives the tick loop via advance_time() — it steps through its
        own price history so TradingEngine has no knowledge of timestamps or prices.
        Stop-losses run on every tick; rebalancing only happens at timestamps
        present in self.df (the filtered predictions).

        Parameters
        ----------
        adapt_policy : bool
            If True (default), call policy.adapt() after each rebalance.

        Returns
        -------
        dict with keys: total_equity, policy, broker, fees_paid
        """
        if self.df is None:
            raise ValueError(
                "df is required for backtest(). Pass df at init, or use step() for live trading."
            )
        if self.policy is None:
            raise ValueError("policy is required for backtest().")

        self._last_rebalance_ts = None
        self._last_equity = None
        self._peak_equity = None
        self._halted = False
        self._needs_liquidation = False
        await self.restore_intraday_state()

        pred_timestamps = set(self.df["timestamp"].unique())

        while (ts := self.broker.advance_time()) is not None:
            trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()

            positions = await self.broker.get_positions()
            prices = await self.broker.get_prices_for_positions(positions)
            equity = await self.broker.get_equity()

            if self._peak_equity is None or equity > self._peak_equity:
                self._peak_equity = equity

            stop_loss_symbols = self._find_stop_loss_triggers(positions, prices, equity)

            if self._halted:
                await self._execute_liquidation_sweep(positions, prices)
                break

            if stop_loss_symbols:
                await self._execute_stop_loss_closes(stop_loss_symbols, prices, trade_date)

            if ts not in pred_timestamps or not self._is_rebalance_due(ts):
                continue

            df_ts = self.df[self.df["timestamp"] == ts].copy()
            df_ts["portfolio_weight"] = self.policy.apply(df_ts)
            equity = await self.broker.get_equity()
            await self._run_rebalance(df_ts, equity, trade_date)
            if adapt_policy:
                await self._run_adapt(df_ts)
            self._last_rebalance_ts = ts

        equity = await self.broker.get_equity()
        return {
            "total_equity": equity,
            "policy": self.policy,
            "broker": self.broker,
            "fees_paid": self.broker.get_fees_paid(),
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
    raw_price_data: pd.DataFrame,
    rebalance_interval_hours: float = 1.0,
    allow_intraday: bool = False,
    max_drawdown_pct: float = 0.10,
    stop_loss_pct: float = 0.05,
) -> Tuple[StrategyPolicy, float]:
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

    cfg = config["trading_rules"]
    broker = PaperBroker(
        raw_price_data,
        starting_cash=cfg["starting_cash"],
        flat_fee=cfg["flat_fee"],
        percent_fee=cfg["percent_fee"],
    )

    policy = StrategyPolicy(
        [
            InformationRatioRule(
                min_ratio=0,
                learning_rate=0,
            )
        ]
    )

    engine = TradingEngine(
        broker,
        policy=policy,
        df=predictions,
        rebalance_interval_hours=rebalance_interval_hours,
        allow_intraday=allow_intraday,
        max_drawdown_pct=max_drawdown_pct,
        stop_loss_pct=stop_loss_pct,
    )

    result = await engine.backtest()

    starting_cash = cfg["starting_cash"]
    returns = result["total_equity"] / starting_cash
    policy = result["policy"]
    return policy, returns


async def apply_trading_policy(
    predictions: pd.DataFrame,
    policy: StrategyPolicy,
) -> pd.DataFrame:
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
