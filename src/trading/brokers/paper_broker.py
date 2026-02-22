from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.portfolio import Portfolio, Position


from typing import Dict


class PaperBroker(BaseBroker):
    """
    Simulates order execution against a local Portfolio.
    Fills are assumed immediate at the provided price (no slippage model).
    """

    def __init__(
        self,
        starting_cash: float = 100_000.0,
        flat_fee: float = 0.0,
        percent_fee: float = 0.0,
    ):
        self.portfolio = Portfolio(starting_cash)
        self.flat_fee = flat_fee
        self.percent_fee = percent_fee

    def fill_order(
        self,
        symbol: str,
        shares_delta: float,
        price: float,
    ) -> TradeResult:
        if symbol not in self.portfolio.positions:
            self.portfolio.positions[symbol] = Position()
        pos = self.portfolio.positions[symbol]

        is_buying = shares_delta > 0
        fee = (self.flat_fee + abs(shares_delta * price) * self.percent_fee) if abs(shares_delta) > 1e-6 else 0.0

        # Cap buys to available cash
        if is_buying:
            cost = shares_delta * price + fee
            if cost > self.portfolio.cash:
                affordable = (self.portfolio.cash - self.flat_fee) / (1.0 + self.percent_fee)
                shares_delta = affordable / price
                fee = self.flat_fee + affordable * self.percent_fee

        if abs(shares_delta) < 1e-6:
            return TradeResult(symbol, 0.0, price, True, "no_change")

        cost = shares_delta * price
        self.portfolio.cash -= cost + fee
        self.portfolio.fees_paid += fee

        new_shares = pos.shares + shares_delta
        if new_shares > 1e-9:
            pos.avg_price = (pos.avg_price * pos.shares + price * shares_delta) / new_shares
        pos.shares = new_shares

        return TradeResult(symbol, shares_delta, price, True)

    def get_positions(self) -> Dict[str, Position]:
        return dict(self.portfolio.positions)

    def get_equity(self, prices: Dict[str, float]) -> float:
        return self.portfolio.total_equity(prices)

    def get_fees_paid(self) -> float:
        return self.portfolio.fees_paid