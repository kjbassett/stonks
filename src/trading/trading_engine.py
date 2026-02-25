import asyncio
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

from src.trading.executor import OrderExecutor
from src.trading.brokers.paper_broker import PaperBroker
from src.trading.strategy import StrategyPolicy, PredictionThresholdRule


class TradingEngine:
    """
    Drives a trading policy over a predictions DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: symbol, timestamp (unix seconds), close,
        prediction, variance.
        Optionally: prediction_ts (unix seconds, for stale-prediction checks).
    executor : OrderExecutor
        Handles actual trade execution and safety checks. If None, an
        OrderExecutor backed by PaperBroker is created with default settings.
    rebalance_interval_hours : float
        How often to rebalance the portfolio. 0.0 = every timestamp.
        Between rebalances, prices are still updated and stop-losses checked.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        executor: Optional[OrderExecutor] = None,
        rebalance_interval_hours: float = 1.0,
        # Kept for backward compatibility when no executor is provided
        starting_cash: float = 100_000.0,
        flat_fee: float = 0.0,
        percent_fee: float = 0.0,
        rules=None,
    ):
        self.df = df.sort_values("timestamp")
        self.rebalance_interval_hours = rebalance_interval_hours
        self.last_prices = {}

        if executor is not None:
            self.executor = executor
        else:
            self.executor = OrderExecutor(
                broker=PaperBroker(
                    starting_cash=starting_cash,
                    flat_fee=flat_fee,
                    percent_fee=percent_fee,
                )
            )
        self.policy = StrategyPolicy(rules or [])

    async def run(self):
        await self.executor._restore_intraday_state()
        last_equity = None
        last_rebalance_ts = None

        for ts, df_ts in self.df.groupby("timestamp", sort=True):
            # Always update last known prices
            for _, row in df_ts.iterrows():
                self.last_prices[row["symbol"]] = row["close"]

            # Check stop-losses on every tick even when not rebalancing
            await self.executor.check_stop_losses(self.last_prices)

            # Decide whether to rebalance
            rebalance_interval_s = self.rebalance_interval_hours * 3600
            should_rebalance = (
                last_rebalance_ts is None
                or (ts - last_rebalance_ts) >= rebalance_interval_s
            )

            if should_rebalance:
                equity = await self.executor.get_equity(self.last_prices)
                weights = self.policy.apply(df_ts)
                df_ts = df_ts.copy()
                df_ts["portfolio_weight"] = weights

                pred_ts = self._parse_prediction_ts(df_ts)
                trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()

                # Split into sells and buys so sells execute first, freeing cash
                # for subsequent buys. asyncio.gather runs each group concurrently,
                # but the two awaits are sequential — all sells complete before any
                # buy starts.
                positions = await self.executor.broker.get_positions()
                sell_coros, buy_coros = [], []
                for _, row in df_ts.iterrows():
                    symbol = row["symbol"]
                    price = row["close"]
                    target_exposure = row["portfolio_weight"]
                    pos = positions.get(symbol)
                    current_value = pos.shares * price if pos else 0.0
                    target_value = equity * target_exposure

                    coro = self.executor.execute_target_exposure(
                        symbol=symbol,
                        target_exposure=target_exposure,
                        price=price,
                        current_equity=equity,
                        prediction_ts=pred_ts,
                        trade_date=trade_date,
                    )
                    if target_value < current_value:
                        sell_coros.append(coro)
                    else:
                        buy_coros.append(coro)

                # Sells complete fully before any buy begins
                await asyncio.gather(*sell_coros)
                await asyncio.gather(*buy_coros)

                last_rebalance_ts = ts

            # Adapt policy after each tick
            equity = await self.executor.get_equity(self.last_prices)
            if last_equity is not None:
                realized_return = (equity - last_equity) / last_equity
                utilization = float(df_ts["portfolio_weight"].sum()) if should_rebalance else 0.0
                self.policy.adapt(realized_return, utilization)

            last_equity = equity

        return {
            "total_equity": equity,
            "policy": self.policy,
            "executor": self.executor,
            "fees_paid": self.executor.get_fees_paid(),
        }

    @staticmethod
    def _parse_prediction_ts(df_ts: pd.DataFrame) -> Optional[datetime]:
        """Extract a prediction timestamp from the dataframe if present."""
        if "prediction_ts" not in df_ts.columns:
            return None
        val = df_ts["prediction_ts"].dropna()
        if val.empty:
            return None
        return datetime.fromtimestamp(float(val.iloc[0]), tz=timezone.utc).replace(tzinfo=None)


def train_trading_policy(
    predictions: pd.DataFrame,
    starting_cash: float = 100_000.0,
    flat_fee: float = 0.0,
    percent_fee: float = 0.0,
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

    rule = PredictionThresholdRule(
        threshold=0,
        aggressiveness=1.0,
        learning_rate=0.01,
    )

    executor = OrderExecutor(
        broker=PaperBroker(
            starting_cash=starting_cash,
            flat_fee=flat_fee,
            percent_fee=percent_fee,
        ),
        allow_intraday=allow_intraday,
        max_drawdown_pct=max_drawdown_pct,
        stop_loss_pct=stop_loss_pct,
    )

    engine = TradingEngine(
        df=predictions,
        executor=executor,
        rebalance_interval_hours=rebalance_interval_hours,
        rules=[rule],
    )

    result = asyncio.run(engine.run())

    returns = result["total_equity"] / starting_cash
    policy = result["policy"]
    return policy, returns


def apply_trading_policy(
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
