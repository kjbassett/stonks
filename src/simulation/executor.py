from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import logging

from src.simulation.portfolio import Portfolio, Position


@dataclass
class TradeResult:
    symbol: str
    shares_delta: float
    price: float
    filled: bool
    reason: str = "ok"              # outcome: what happened when execution was attempted
    trigger: Optional[str] = None   # why the trade was initiated (e.g. "stop_loss", "drawdown_halt")
    order_id: Optional[str] = None


class OrderExecutor(ABC):
    """
    Abstraction over order execution.

    Shared safety checks applied before every trade:
      - Max drawdown halt: liquidates all positions then blocks new buys
      - Per-position stop-loss: auto-closes positions below avg_price * (1 - stop_loss_pct)
      - PDT (Pattern Day Trader) rule: when allow_intraday=False, refuses same-day round-trips
        in both directions (buy→sell and sell→buy count equally as day trades)
      - Stale prediction guard: skips trades if the prediction is too old (live trading only;
        None prediction_ts is treated as unknown age and allowed through, which is the
        correct behaviour for backtesting)
    """

    def __init__(
        self,
        flat_fee: float = 0.0,
        percent_fee: float = 0.0,
        max_drawdown_pct: float = 0.10,
        stop_loss_pct: float = 0.05,
        allow_intraday: bool = True,
        stale_prediction_hours: float = 2.0,
    ):
        self.flat_fee = flat_fee
        self.percent_fee = percent_fee
        self.max_drawdown_pct = max_drawdown_pct
        self.stop_loss_pct = stop_loss_pct
        self.allow_intraday = allow_intraday
        self.stale_prediction_hours = stale_prediction_hours

        self._peak_equity: Optional[float] = None
        self._halted: bool = False
        self._needs_liquidation: bool = False

        # PDT tracking: (symbol, date) → count of same-day events not yet matched.
        # Buys and sells are tracked separately so both round-trip directions are caught:
        #   buy→sell same day: _intraday_buys  > 0 blocks the sell
        #   sell→buy same day: _intraday_sells > 0 blocks the buy
        self._intraday_buys: Dict[Tuple[str, date], int] = {}
        self._intraday_sells: Dict[Tuple[str, date], int] = {}

        self._log = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Safety checks
    # ------------------------------------------------------------------

    def _update_drawdown(self, current_equity: float) -> bool:
        """
        Update peak equity. If max drawdown is exceeded, set the halted flag
        and schedule a one-time liquidation sweep via check_stop_losses.
        Returns True if halted (new/increased buys should be blocked).
        """
        if self._peak_equity is None or current_equity > self._peak_equity:
            self._peak_equity = current_equity
        if current_equity < self._peak_equity * (1.0 - self.max_drawdown_pct):
            if not self._halted:
                self._halted = True
                self._needs_liquidation = True
                pct = 1.0 - current_equity / self._peak_equity
                self._log.warning(
                    f"TRADING HALTED: portfolio down {pct:.1%} from peak "
                    f"(${self._peak_equity:,.2f} → ${current_equity:,.2f}). "
                    f"Liquidating all positions."
                )
        return self._halted

    def _pdt_buy_allowed(self, symbol: str, trade_date: date) -> bool:
        """Block a buy if the same symbol was already sold today (sell→buy round-trip)."""
        if self.allow_intraday:
            return True
        count = self._intraday_sells.get((symbol, trade_date), 0)
        if count > 0:
            self._log.warning(
                f"PDT block: buying {symbol} would complete a same-day round-trip "
                f"({count} same-day sell(s) on {trade_date}). "
                f"Set allow_intraday=True or ensure account >= $25k."
            )
            return False
        return True

    def _pdt_sell_allowed(self, symbol: str, trade_date: date) -> bool:
        """Block a sell if the same symbol was already bought today (buy→sell round-trip)."""
        if self.allow_intraday:
            return True
        count = self._intraday_buys.get((symbol, trade_date), 0)
        if count > 0:
            self._log.warning(
                f"PDT block: selling {symbol} would complete a same-day round-trip "
                f"({count} unmatched same-day buy(s) on {trade_date}). "
                f"Set allow_intraday=True or ensure account >= $25k."
            )
            return False
        return True

    def _record_buy(self, symbol: str, trade_date: date) -> None:
        key = (symbol, trade_date)
        self._intraday_buys[key] = self._intraday_buys.get(key, 0) + 1

    def _record_sell(self, symbol: str, trade_date: date) -> None:
        key = (symbol, trade_date)
        self._intraday_sells[key] = self._intraday_sells.get(key, 0) + 1

    def _prediction_fresh(self, prediction_ts: Optional[datetime]) -> bool:
        """
        Returns True if the prediction is recent enough to act on.
        None means no timestamp was provided — allowed through since this is
        the normal case for backtesting.
        """
        if prediction_ts is None:
            return True
        age_h = (datetime.utcnow() - prediction_ts).total_seconds() / 3600.0
        if age_h > self.stale_prediction_hours:
            self._log.warning(
                f"Stale prediction: {age_h:.1f}h old (max {self.stale_prediction_hours}h). Skipping trade."
            )
            return False
        return True

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def execute_target_exposure(
        self,
        symbol: str,
        target_exposure: float,
        price: float,
        current_equity: float,
        prediction_ts: Optional[datetime] = None,
        trade_date: Optional[date] = None,
    ) -> TradeResult:
        """
        Adjust the position in `symbol` so its market value equals
        `target_exposure * current_equity`.

        target_exposure=0.0 means close the position entirely.
        shares_delta > 0 in result means shares were bought, < 0 means sold.

        When halted, closes (target_exposure=0) are still allowed so the
        liquidation sweep can proceed; new/increased buys are blocked.
        """

    @abstractmethod
    def check_stop_losses(self, prices: Dict[str, float]) -> List[TradeResult]:
        """
        Check all open positions for stop-loss triggers and close if needed.
        Also handles the one-time liquidation sweep when a drawdown halt fires.
        """

    @abstractmethod
    def get_equity(self, prices: Dict[str, float]) -> float:
        """Current total portfolio value (cash + positions)."""

    @abstractmethod
    def get_fees_paid(self) -> float:
        """Cumulative fees/commissions paid."""

    def reset_halt(self) -> None:
        """Clear a drawdown halt after manual review."""
        self._halted = False
        self._needs_liquidation = False
        self._peak_equity = None
        self._log.info("Trading halt cleared.")


class PaperOrderExecutor(OrderExecutor):
    """
    Simulates order execution against a local Portfolio.
    Fills are assumed immediate at the provided price (no slippage model).
    """

    def __init__(self, starting_cash: float = 100_000.0, **kwargs):
        super().__init__(**kwargs)
        self.portfolio = Portfolio(starting_cash)

    def execute_target_exposure(
        self,
        symbol: str,
        target_exposure: float,
        price: float,
        current_equity: float,
        prediction_ts: Optional[datetime] = None,
        trade_date: Optional[date] = None,
    ) -> TradeResult:
        if trade_date is None:
            trade_date = date.today()

        if symbol not in self.portfolio.positions:
            self.portfolio.positions[symbol] = Position()
        pos = self.portfolio.positions[symbol]

        current_value = pos.market_value(price)
        target_value = current_equity * target_exposure
        delta_value = target_value - current_value

        if abs(delta_value) < 1e-6:
            return TradeResult(symbol, 0.0, price, True, "no_change")

        is_buying = delta_value > 0
        is_selling = delta_value < 0 and pos.shares > 0

        # When halted, block new/increased buys but allow closes
        if self._halted and is_buying:
            return TradeResult(symbol, 0.0, price, False, "halted")

        if not self._prediction_fresh(prediction_ts):
            return TradeResult(symbol, 0.0, price, False, "stale_prediction")

        if is_buying and not self._pdt_buy_allowed(symbol, trade_date):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")
        if is_selling and not self._pdt_sell_allowed(symbol, trade_date):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")

        shares_delta = delta_value / price
        fee = (self.flat_fee + abs(delta_value) * self.percent_fee) if abs(delta_value) > 1e-6 else 0.0

        # Cap buys to available cash
        if is_buying and delta_value + fee > self.portfolio.cash:
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

        if shares_delta > 0:
            self._record_buy(symbol, trade_date)
        elif is_selling:
            self._record_sell(symbol, trade_date)

        return TradeResult(symbol, shares_delta, price, True)

    def check_stop_losses(self, prices: Dict[str, float]) -> List[TradeResult]:
        results = []

        # One-time liquidation sweep when drawdown halt first fires
        if self._needs_liquidation:
            self._needs_liquidation = False
            for symbol, pos in list(self.portfolio.positions.items()):
                if pos.shares > 0 and symbol in prices:
                    result = self._force_close(symbol, prices[symbol], date.today())
                    result.trigger = "drawdown_halt"
                    self._log.warning(f"Drawdown liquidation: closing {symbol}")
                    results.append(result)
            return results

        # Regular per-position stop-loss check
        for symbol, pos in list(self.portfolio.positions.items()):
            if pos.shares <= 0 or symbol not in prices:
                continue
            price = prices[symbol]
            if pos.avg_price > 0 and price < pos.avg_price * (1.0 - self.stop_loss_pct):
                equity = self.get_equity(prices)
                result = self.execute_target_exposure(symbol, 0.0, price, equity)
                result.trigger = "stop_loss"
                self._log.warning(
                    f"Stop-loss triggered: {symbol} @ ${price:.2f} "
                    f"(avg ${pos.avg_price:.2f}, loss {1.0 - price / pos.avg_price:.1%}) "
                    f"→ outcome: {result.reason}"
                )
                results.append(result)
        return results

    def _force_close(self, symbol: str, price: float, trade_date: date) -> TradeResult:
        """
        Close a position unconditionally, bypassing halt and PDT checks.
        Used only for drawdown liquidation. Records the sell for PDT tracking
        so that re-buying the same symbol today is subsequently blocked.
        If this causes a PDT violation (position was bought today), a warning is logged.
        """
        pos = self.portfolio.positions.get(symbol)
        if pos is None or pos.shares <= 0:
            return TradeResult(symbol, 0.0, price, True, "no_change")

        # Warn if this force-close would constitute a PDT violation
        if not self.allow_intraday:
            buys_today = self._intraday_buys.get((symbol, trade_date), 0)
            if buys_today > 0:
                self._log.warning(
                    f"PDT notice: force-closing {symbol} which was bought today ({trade_date}). "
                    f"This constitutes a day trade. Capital protection takes priority."
                )

        shares_delta = -pos.shares
        proceeds = pos.shares * price
        fee = self.flat_fee + proceeds * self.percent_fee

        self.portfolio.cash += proceeds - fee
        self.portfolio.fees_paid += fee
        pos.shares = 0.0

        self._record_sell(symbol, trade_date)

        return TradeResult(symbol, shares_delta, price, True)

    def get_equity(self, prices: Dict[str, float]) -> float:
        return self.portfolio.total_equity(prices)

    def get_fees_paid(self) -> float:
        return self.portfolio.fees_paid
