from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Position:
    shares: float = 0.0
    avg_price: float = 0.0

    def market_value(self, price: float) -> float:
        return self.shares * price


@dataclass
class Portfolio:
    cash: float
    positions: Dict[str, Position] = field(default_factory=dict)
    fees_paid: float = 0.0

    def total_equity(self, prices: Dict[str, float]) -> float:
        equity = self.cash
        for sym, pos in self.positions.items():
            if sym in prices:
                equity += pos.market_value(prices[sym])
        return equity
