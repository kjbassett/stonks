from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

from src.trading.portfolio import Position


@dataclass
class TradeResult:
    symbol: str
    shares_delta: float
    price: float
    filled: bool
    reason: str = "ok"              # outcome: what happened when execution was attempted
    trigger: Optional[str] = None   # why the trade was initiated (e.g. "stop_loss", "drawdown_halt")
    order_id: Optional[str] = None


class BaseBroker(ABC):
    """
    Abstracts fill mechanics for a specific broker or simulation engine.

    Implementations:
      - PaperBroker: simulates fills against an in-memory Portfolio
      - SchwabBroker: routes orders to the Charles Schwab REST API

    The broker handles HOW and WHERE orders are filled.
    All trading safety logic (drawdown halt, stop-loss, PDT) lives in TradingEngine.
    """

    @abstractmethod
    async def fill_order(
        self,
        symbol: str,
        shares_delta: float,
        price: float,
    ) -> TradeResult:
        """
        Execute a trade. shares_delta > 0 = buy, < 0 = sell.
        The broker may fill fewer shares than requested (e.g. due to cash cap).
        Returns a TradeResult with the actual outcome.
        """

    @abstractmethod
    async def get_positions(self) -> Dict[str, Position]:
        """Current holdings: {symbol -> Position(shares, avg_price)}."""

    @abstractmethod
    async def get_equity(self) -> float:
        """Current total portfolio value (cash + open positions at current prices)."""

    @abstractmethod
    def get_fees_paid(self) -> float:
        """Cumulative fees paid."""

    def advance_time(self) -> Optional[float]:
        """
        Advance to the next simulation timestamp.

        PaperBroker steps through its price history sequentially, updates its
        internal clock, and returns the new timestamp.
        Returns None when there are no more timestamps (backtest loop should stop).
        Live brokers return None — use step() for real-time trading instead.
        """
        return None

    async def get_prices_for_positions(
        self, positions: Dict[str, Position]
    ) -> Dict[str, float]:
        """
        Return a {symbol: price} dict for currently held positions.
        PaperBroker looks up prices from its historical index.
        Live brokers may override to query the market data API; default returns {}.
        """
        return {}

    async def get_today_fills(self) -> List[Tuple[str, str]]:
        """
        Return (symbol, 'BUY'|'SELL') pairs for all filled orders today.
        Used by TradingEngine to restore intraday PDT tracking after a restart.
        Default returns [] — override in brokers that support order history.
        """
        return []