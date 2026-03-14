import logging
from typing import Dict

from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.portfolio import Position


_log = logging.getLogger("trading.executor")


class OrderExecutor:
    """
    Thin execution wrapper over a BaseBroker.

    Handles target-exposure-to-shares math and order logging.
    All safety checks (drawdown, PDT, stale prediction, stop-loss) live in TradingEngine.
    """

    def __init__(self, broker: BaseBroker) -> None:
        self.broker = broker

    async def execute_shares(
        self, symbol: str, shares_delta: float, price: float
    ) -> TradeResult:
        """
        Fill an exact shares delta. Returns no_change if delta is negligible.
        No safety checks — caller is responsible for those.
        """
        if abs(shares_delta) < 1e-6:
            return TradeResult(symbol, 0.0, price, True, "no_change")
        result = await self.broker.fill_order(symbol, shares_delta, price)
        direction = "BUY" if result.shares_delta > 0 else ("SELL" if result.shares_delta < 0 else "NONE")
        if result.filled and result.reason != "no_change":
            _log.info(
                f"Trade executed: {symbol} {direction} {abs(result.shares_delta):.4f} shares "
                f"@ ${price:.2f} | reason={result.reason}"
            )
        elif not result.filled:
            _log.warning(f"Trade not filled: {symbol} {direction} | reason={result.reason}")
        return result

    async def execute_target_exposure(
        self,
        symbol: str,
        target_exposure: float,
        price: float,
        equity: float,
    ) -> TradeResult:
        """
        Adjust `symbol` so its market value equals `target_exposure * equity`.
        Fetches current position to compute the required delta.
        target_exposure=0.0 closes the position entirely.
        """
        positions = await self.broker.get_positions()
        pos = positions.get(symbol, Position())
        shares_delta = (equity * target_exposure - pos.shares * price) / price
        return await self.execute_shares(symbol, shares_delta, price)

    async def execute_close(
        self, symbol: str, price: float, pos: Position
    ) -> TradeResult:
        """Close a position entirely using a pre-fetched Position (no broker re-fetch)."""
        return await self.execute_shares(symbol, -pos.shares, price)

    async def get_equity(self, prices: Dict[str, float]) -> float:
        """Current total portfolio value (delegates to broker)."""
        return await self.broker.get_equity(prices)

    def get_fees_paid(self) -> float:
        """Cumulative fees paid (delegates to broker)."""
        return self.broker.get_fees_paid()
