import pandas as pd

from src.simulation.portfolio import Portfolio, Position
from src.simulation.strategy import StrategyPolicy, PredictionThresholdRule


class MarketSimulator:
    def __init__(
        self,
        df,
        starting_cash=100_000.0,
        flat_fee=0.0,
        percent_fee=0.0,
        rules=None,
    ):
        self.df = df.sort_values("timestamp")
        self.portfolio = Portfolio(starting_cash)
        self.flat_fee = flat_fee
        self.percent_fee = percent_fee
        self.policy = StrategyPolicy(rules)
        self.last_prices = {}

    def _apply_trade(self, symbol, target_exposure, price):
        if target_exposure < 0:
            raise NotImplementedError("Shorting not supported")
        if symbol not in self.portfolio.positions:
            self.portfolio.positions[symbol] = Position()

        pos = self.portfolio.positions[symbol]
        equity = self.portfolio.total_equity(self.last_prices)
        target_value = equity * target_exposure
        current_value = pos.market_value(price)

        delta_value = target_value - current_value
        if abs(delta_value) < 1e-6:
            return

        shares_to_trade = delta_value / price
        cost = shares_to_trade * price
        fee = self.flat_fee + abs(cost) * self.percent_fee

        if cost + fee > self.portfolio.cash:
            # Cap to available cash
            cost = (self.portfolio.cash - self.flat_fee) / (1 + self.percent_fee)
            shares_to_trade = cost / price
            fee = self.flat_fee + cost * self.percent_fee

        # Update portfolio
        self.portfolio.cash -= cost + fee
        self.portfolio.fees_paid += fee
        # Update position
        new_total_shares = pos.shares + shares_to_trade
        if new_total_shares > 0:
            pos.avg_price = (
                pos.avg_price * pos.shares + price * shares_to_trade
            ) / new_total_shares
        pos.shares = new_total_shares

    def run(self):
        last_equity = None

        for ts, df_ts in self.df.groupby("timestamp", sort=True):
            # update prices
            for _, row in df_ts.iterrows():
                self.last_prices[row["symbol"]] = row["close"]

            # allocate exposures
            df_ts["portfolio weight"] = self.policy.apply(df_ts)

            # apply trades
            for _, row in df_ts.iterrows():
                # update latest price
                self._apply_trade(
                    row["symbol"],
                    row["portfolio weight"],
                    row["close"],
                )

            # compute realized return
            equity = self.portfolio.total_equity(self.last_prices)
            if last_equity is not None:
                realized_return = (equity - last_equity) / last_equity
                utilization = sum(df_ts["portfolio weight"])
                self.policy.adapt(realized_return, utilization)
                print(utilization, equity)

            last_equity = equity

        return {
            "total_equity": equity,
            "policy": self.policy,
            "portfolio": self.portfolio,
        }


def train_trading_policy(
    predictions: pd.DataFrame,
    starting_cash: float = 100_000.0,
    flat_fee: float = 0.0,
    percent_fee: float = 0.0,
):
    """
    Train a trading policy via market simulation.

    Returns:
        policy: StrategyPolicy (stateful, trained, frozen)
        fitness: float (final equity)
    """

    # --- Safety checks ---
    required_cols = {"symbol", "timestamp", "close", "prediction", "variance"}
    missing = required_cols - set(predictions.columns)
    if missing:
        raise ValueError(f"Predictions missing required columns: {missing}")

    # --- Create trading rules ---
    # TODO GA can choose rules and control initial values and params if desired.
    rule = PredictionThresholdRule(
        threshold=3,  # initial Sharpe-like cutoff
        aggressiveness=1.0,  # exposure scaling
        learning_rate=0.01,  # enables in-simulation tuning
    )

    policy = StrategyPolicy(rules=[rule])

    # --- Run simulation ---
    simulator = MarketSimulator(
        df=predictions,
        starting_cash=starting_cash,
        flat_fee=flat_fee,
        percent_fee=percent_fee,
        rules=policy.rules,
    )

    result = simulator.run()  # TODO run mode = train or apply or maybe freeze=True

    # --- Extract results ---
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
        recommendations: pd.DataFrame
    """

    required_cols = {"symbol", "timestamp", "prediction", "uncertainty"}
    missing = required_cols - set(predictions.columns)
    if missing:
        raise ValueError(f"Predictions missing required columns: {missing}")
    predictions["portfolio weight"] = policy.apply(predictions)
    return predictions
