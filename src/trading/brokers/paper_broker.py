import asyncio
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.portfolio import Portfolio, Position


class PaperBroker(BaseBroker):
    """
    Simulates order execution against a local Portfolio.

    Uses raw_price_data (a DataFrame with columns: symbol, timestamp, close) as
    the ground truth for prices. Call advance_time() to step the internal clock
    to the next timestamp in the price history — get_price() then returns the
    most recent close at or before that timestamp.

    Fills are assumed immediate at the provided price (no slippage model).
    An asyncio.Lock serialises concurrent fill_order calls so that portfolio
    cash and share counts are never mutated by two coroutines simultaneously.
    """

    def __init__(
        self,
        price_history: pd.DataFrame,
        starting_cash: float = 100_000.0,
        flat_fee: float = 0.0,
        percent_fee: float = 0.0,
    ):
        self.portfolio = Portfolio(starting_cash)
        self.flat_fee = flat_fee
        self.percent_fee = percent_fee
        self._lock = asyncio.Lock()
        self._current_ts: float = float("-inf")
        self._price_index: Dict[str, Tuple[np.ndarray, np.ndarray]] = (
            self._build_price_index(price_history)
        )
        self._all_timestamps: List[float] = self._extract_timestamps(price_history)
        self._ts_index: int = 0

    @staticmethod
    def _build_price_index(
        df: pd.DataFrame,
    ) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
        """
        Pre-process raw_price_data into per-symbol sorted arrays for O(log n) lookup.

        Returns:
            {symbol: (timestamps_array, closes_array)} both sorted ascending by timestamp.
        """
        index: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
        print(f"df: {df}")
        if df is None or df.empty:
            return index
        for symbol, group in df.groupby("symbol"):
            group = group.sort_values("timestamp")
            index[symbol] = (
                group["timestamp"].to_numpy(dtype=float),
                group["close"].to_numpy(dtype=float),
            )
        return index

    @staticmethod
    def _extract_timestamps(df: pd.DataFrame) -> List[float]:
        """Return the sorted unique timestamps across all symbols."""
        if df is None or df.empty:
            return []
        return sorted(df["timestamp"].unique().tolist())

    def advance_time(self) -> Optional[float]:
        """
        Advance to the next timestamp in the price history.

        Updates the internal clock so get_price() reflects the new tick.
        Returns the timestamp advanced to, or None when all ticks are exhausted.
        """
        if self._ts_index >= len(self._all_timestamps):
            return None
        self._current_ts = self._all_timestamps[self._ts_index]
        self._ts_index += 1
        return self._current_ts

    async def get_price(self, symbol: str) -> Optional[float]:
        """
        Return the most recent close price for symbol at or before _current_ts.
        Returns None if the symbol is unknown or has no data before _current_ts.
        """
        if symbol not in self._price_index:
            return None
        timestamps, closes = self._price_index[symbol]
        idx = int(np.searchsorted(timestamps, self._current_ts, side="right")) - 1
        if idx < 0:
            return None
        return float(closes[idx])

    async def get_prices_for_positions(
        self, positions: Dict[str, Position]
    ) -> Dict[str, float]:
        """
        Build a {symbol: price} dict for all held symbols that have price data
        available at the current simulation timestamp.
        """
        prices: Dict[str, float] = {}
        for symbol in positions:
            price = await self.get_price(symbol)
            if price is not None:
                prices[symbol] = price
        return prices

    async def fill_order(
        self,
        symbol: str,
        shares_delta: float,
        price: float,
    ) -> TradeResult:
        """Fill an order at the given price, capping buys to available cash."""
        async with self._lock:
            if symbol not in self.portfolio.positions:
                self.portfolio.positions[symbol] = Position()
            pos = self.portfolio.positions[symbol]

            is_buying = shares_delta > 0
            fee = (
                (self.flat_fee + abs(shares_delta * price) * self.percent_fee)
                if abs(shares_delta) > 1e-6
                else 0.0
            )

            if is_buying:
                cost = shares_delta * price + fee
                if cost > self.portfolio.cash:
                    affordable = (self.portfolio.cash - self.flat_fee) / (
                        1.0 + self.percent_fee
                    )
                    shares_delta = affordable / price
                    fee = self.flat_fee + affordable * self.percent_fee

            if abs(shares_delta) < 1e-6:
                return TradeResult(symbol, 0.0, price, True, "no_change")

            cost = shares_delta * price
            self.portfolio.cash -= cost + fee
            self.portfolio.fees_paid += fee

            new_shares = pos.shares + shares_delta
            if new_shares > 1e-9:
                pos.avg_price = (
                    pos.avg_price * pos.shares + price * shares_delta
                ) / new_shares
            pos.shares = new_shares

            return TradeResult(symbol, shares_delta, price, True)

    async def get_positions(self) -> Dict[str, Position]:
        """Return a copy of the current portfolio positions."""
        return dict(self.portfolio.positions)

    async def get_equity(self) -> float:
        """Return total portfolio value using prices at the current simulation timestamp."""
        prices = await self.get_prices_for_positions(self.portfolio.positions)
        return self.portfolio.total_equity(prices)

    def get_fees_paid(self) -> float:
        """Return cumulative fees paid."""
        return self.portfolio.fees_paid
