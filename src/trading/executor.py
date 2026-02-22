
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Tuple
import logging

from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.portfolio import Position


class OrderExecutor:
    """
    Enforces all trading safety rules and delegates fill mechanics to a BrokerInterface.

    Safety checks applied before every trade:
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
        broker: BaseBroker,
        max_drawdown_pct: float = 0.10,
        stop_loss_pct: float = 0.05,
        allow_intraday: bool = True,
        stale_prediction_hours: float = 2.0,
    ):
        self.broker = broker
        self.max_drawdown_pct = max_drawdown_pct
        self.stop_loss_pct = stop_loss_pct
        self.allow_intraday = allow_intraday
        self.stale_prediction_hours = stale_prediction_hours

        self._peak_equity: Optional[float] = None
        self._halted: bool = False
        self._needs_liquidation: bool = False

        # PDT tracking: (symbol, date) → count of same-day events.
        # Buys and sells tracked separately so both round-trip directions are caught:
        #   buy→sell same day: _intraday_buys > 0 blocks the sell
        #   sell→buy same day: _intraday_sells > 0 blocks the buy
        self._intraday_buys: Dict[Tuple[str, date], int] = {}
        self._intraday_sells: Dict[Tuple[str, date], int] = {}

        self._log = logging.getLogger(self.__class__.__name__)
        self._restore_intraday_state()

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
        age_h = (datetime.now(timezone.utc).replace(tzinfo=None) - prediction_ts).total_seconds() / 3600.0
        if age_h > self.stale_prediction_hours:
            self._log.warning(
                f"Stale prediction: {age_h:.1f}h old (max {self.stale_prediction_hours}h). Skipping trade."
            )
            return False
        return True

    def _restore_intraday_state(self) -> None:
        """
        Restore PDT tracking from broker order history.
        Called on __init__ so that state survives a process restart mid-day.
        Brokers that don't support history return [] from get_today_fills().
        """
        today = date.today()
        try:
            fills = self.broker.get_today_fills()
        except Exception as e:
            self._log.warning(f"Could not restore intraday state from broker history: {e}")
            return

        buys, sells = 0, 0
        for symbol, instruction in fills:
            if instruction == "BUY":
                self._record_buy(symbol, today)
                buys += 1
            elif instruction == "SELL":
                self._record_sell(symbol, today)
                sells += 1

        if buys or sells:
            self._log.info(
                f"Restored intraday state from broker history: "
                f"{buys} buy(s), {sells} sell(s) for {today}"
            )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

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
        if trade_date is None:
            trade_date = date.today()

        self._update_drawdown(current_equity)

        positions = self.broker.get_positions()
        pos = positions.get(symbol, Position())
        current_value = pos.shares * price
        target_value = current_equity * target_exposure
        delta_value = target_value - current_value

        if abs(delta_value) < 1e-6:
            return TradeResult(symbol, 0.0, price, True, "no_change")

        is_buying = delta_value > 0
        is_selling = delta_value < 0 and pos.shares > 0

        if self._halted and is_buying:
            return TradeResult(symbol, 0.0, price, False, "halted")

        if not self._prediction_fresh(prediction_ts):
            return TradeResult(symbol, 0.0, price, False, "stale_prediction")

        if is_buying and not self._pdt_buy_allowed(symbol, trade_date):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")
        if is_selling and not self._pdt_sell_allowed(symbol, trade_date):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")

        shares_delta = delta_value / price
        result = self.broker.fill_order(symbol, shares_delta, price)

        if result.shares_delta > 0:
            self._record_buy(symbol, trade_date)
        elif result.shares_delta < 0:
            self._record_sell(symbol, trade_date)

        return result

    def check_stop_losses(self, prices: Dict[str, float]) -> List[TradeResult]:
        """
        Check all open positions for stop-loss triggers and close if needed.
        Also handles the one-time liquidation sweep when a drawdown halt fires.
        """
        results = []
        try:
            positions = self.broker.get_positions()
        except Exception as e:
            self._log.error(f"Failed to fetch positions for stop-loss check: {e}")
            return results

        # One-time liquidation sweep when drawdown halt first fires
        if self._needs_liquidation:
            self._needs_liquidation = False
            for symbol, pos in positions.items():
                if pos.shares > 0 and symbol in prices:
                    result = self._force_close(symbol, prices[symbol], date.today())
                    result.trigger = "drawdown_halt"
                    self._log.warning(f"Drawdown liquidation: closing {symbol}")
                    results.append(result)
            return results

        # Regular per-position stop-loss check
        equity = None  # computed lazily to avoid KeyError when not all symbols are in prices
        for symbol, pos in positions.items():
            if pos.shares <= 0 or symbol not in prices:
                continue
            price = prices[symbol]
            if pos.avg_price > 0 and price < pos.avg_price * (1.0 - self.stop_loss_pct):
                if equity is None:
                    equity = self.broker.get_equity(prices)
                result = self.execute_target_exposure(symbol, 0.0, price, equity)
                result.trigger = "stop_loss"
                self._log.warning(
                    f"Stop-loss triggered: {symbol} @ ${price:.2f} "
                    f"(avg ${pos.avg_price:.2f}, loss {1.0 - price / pos.avg_price:.1%}) "
                    f"→ outcome: {result.reason}"
                )
                results.append(result)
        return results

    def get_equity(self, prices: Dict[str, float]) -> float:
        """Current total portfolio value (delegates to broker)."""
        return self.broker.get_equity(prices)

    def get_fees_paid(self) -> float:
        """Cumulative fees paid (delegates to broker)."""
        return self.broker.get_fees_paid()

    def reset_halt(self) -> None:
        """Clear a drawdown halt after manual review."""
        self._halted = False
        self._needs_liquidation = False
        self._peak_equity = None
        self._log.info("Trading halt cleared.")

    def _force_close(self, symbol: str, price: float, trade_date: date) -> TradeResult:
        """
        Close a position unconditionally, bypassing halt and PDT checks.
        Used only for drawdown liquidation. Records the sell for PDT tracking
        so that re-buying the same symbol today is subsequently blocked.
        If this causes a PDT violation (position was bought today), a warning is logged.
        """
        positions = self.broker.get_positions()
        pos = positions.get(symbol)
        if pos is None or pos.shares <= 0:
            return TradeResult(symbol, 0.0, price, True, "no_change")

        if not self.allow_intraday:
            buys_today = self._intraday_buys.get((symbol, trade_date), 0)
            if buys_today > 0:
                self._log.warning(
                    f"PDT notice: force-closing {symbol} which was bought today ({trade_date}). "
                    f"This constitutes a day trade. Capital protection takes priority."
                )

        result = self.broker.fill_order(symbol, -pos.shares, price)
        self._record_sell(symbol, trade_date)
        return result
