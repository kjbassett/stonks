"""
Live order execution through the Charles Schwab Trader API.

Use dry_run=True (default) to log what would be traded without placing real orders.
Flip to dry_run=False only after validating the integration against paper results.

Safety features (inherited from OrderExecutor + live-specific):
  - Max drawdown halt
  - Per-position stop-loss
  - PDT rule (allow_intraday=False when account < $25k)
  - Stale prediction guard
  - Order fill confirmation with configurable timeout
"""

import logging
import time
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from src.brokers.schwab_client import SchwabClient
from src.simulation.executor import OrderExecutor, TradeResult


# Order statuses that indicate a terminal failure
_FAILED_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}
_FILLED_STATUS = "FILLED"


class SchwabOrderExecutor(OrderExecutor):
    """
    Routes trades to Charles Schwab via the Trader API.

    Unlike PaperOrderExecutor, portfolio state is owned by Schwab — this class
    queries the API for current positions and balances rather than maintaining
    a local copy.

    Parameters
    ----------
    client : SchwabClient
        Authenticated API client. Call client.update_token() after token refresh.
    account_number : str
        The Schwab account number to trade in.
    dry_run : bool
        If True, log what would be traded but don't submit real orders.
        Default True. Set False only when ready for live trading.
    order_confirm_timeout : float
        Seconds to poll for fill confirmation before giving up.
    **kwargs
        Passed to OrderExecutor (flat_fee, percent_fee, max_drawdown_pct,
        stop_loss_pct, allow_intraday, stale_prediction_hours).
    """

    def __init__(
        self,
        client: SchwabClient,
        account_number: str,
        dry_run: bool = True,
        order_confirm_timeout: float = 15.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.client = client
        self.account_number = account_number
        self.dry_run = dry_run
        self.order_confirm_timeout = order_confirm_timeout
        self._log = logging.getLogger("SchwabOrderExecutor")

        if dry_run:
            self._log.info(
                "SchwabOrderExecutor running in DRY RUN mode — no real orders will be placed."
            )

        self._restore_intraday_state()

    # ------------------------------------------------------------------
    # OrderExecutor interface
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
        if trade_date is None:
            trade_date = datetime.utcnow().date()

        self._update_drawdown(current_equity)

        current_shares = self._get_shares(symbol)
        current_value = current_shares * price
        target_value = current_equity * target_exposure
        delta_value = target_value - current_value
        shares_delta = delta_value / price

        is_buying = shares_delta > 0
        is_selling = shares_delta < 0 and current_shares > 0

        # When halted, block new/increased buys but allow closes
        if self._halted and is_buying:
            return TradeResult(symbol, 0.0, price, False, "halted")

        if not self._prediction_fresh(prediction_ts):
            return TradeResult(symbol, 0.0, price, False, "stale_prediction")

        if is_buying and not self._pdt_buy_allowed(symbol, trade_date):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")
        if is_selling and not self._pdt_sell_allowed(symbol, trade_date):
            return TradeResult(symbol, 0.0, price, False, "pdt_blocked")

        # Schwab requires whole shares
        shares_to_trade = int(abs(shares_delta))
        if shares_to_trade < 1:
            return TradeResult(symbol, 0.0, price, True, "no_change")

        instruction = "BUY" if shares_delta > 0 else "SELL"
        order = SchwabClient.build_market_order(symbol, shares_to_trade, instruction)
        signed = shares_to_trade if instruction == "BUY" else -shares_to_trade

        if self.dry_run:
            self._log.info(
                f"[DRY RUN] {instruction} {shares_to_trade} × {symbol} @ ~${price:.2f} "
                f"(target exposure {target_exposure:.1%})"
            )
            if instruction == "BUY":
                self._record_buy(symbol, trade_date)
            else:
                self._record_sell(symbol, trade_date)
            return TradeResult(symbol, float(signed), price, True, "dry_run")

        order_id = self.client.place_order(self.account_number, order)
        self._log.info(
            f"Placed {instruction} {shares_to_trade} × {symbol} | order_id={order_id}"
        )

        filled = self._wait_for_fill(order_id)
        if not filled:
            self._log.warning(
                f"Order {order_id} not confirmed within {self.order_confirm_timeout}s"
            )

        if filled:
            if instruction == "BUY":
                self._record_buy(symbol, trade_date)
            else:
                self._record_sell(symbol, trade_date)

        return TradeResult(
            symbol, float(signed), price, filled,
            "filled" if filled else "unconfirmed", order_id=order_id,
        )

    def check_stop_losses(self, prices: Dict[str, float]) -> List[TradeResult]:
        """Query live positions and close any that breach the stop-loss threshold.
        Also handles the one-time liquidation sweep when a drawdown halt fires."""
        results = []
        try:
            positions = self.client.get_positions(self.account_number)
            equity = self.get_equity(prices)
        except Exception as e:
            self._log.error(f"Failed to fetch positions for stop-loss check: {e}")
            return results

        # One-time liquidation sweep when drawdown halt first fires
        if self._needs_liquidation:
            self._needs_liquidation = False
            for pos in positions:
                symbol = pos["instrument"]["symbol"]
                if symbol not in prices or pos.get("longQuantity", 0.0) <= 0:
                    continue
                result = self.execute_target_exposure(symbol, 0.0, prices[symbol], equity)
                result.trigger = "drawdown_halt"
                self._log.warning(f"Drawdown liquidation: closing {symbol}")
                results.append(result)
            return results

        # Regular per-position stop-loss check
        for pos in positions:
            symbol = pos["instrument"]["symbol"]
            if symbol not in prices:
                continue
            price = prices[symbol]
            avg_price = pos.get("averagePrice", 0.0)
            long_qty = pos.get("longQuantity", 0.0)

            if long_qty > 0 and avg_price > 0 and price < avg_price * (1.0 - self.stop_loss_pct):
                result = self.execute_target_exposure(symbol, 0.0, price, equity)
                result.trigger = "stop_loss"
                self._log.warning(
                    f"Stop-loss triggered: {symbol} @ ${price:.2f} "
                    f"(avg ${avg_price:.2f}, loss {1.0 - price / avg_price:.1%}) "
                    f"→ outcome: {result.reason}"
                )
                results.append(result)
        return results

    def get_equity(self, prices: Dict[str, float]) -> float:
        """Query Schwab for current account liquidation value."""
        return self.client.get_account_equity(self.account_number)

    def get_fees_paid(self) -> float:
        # Schwab doesn't expose cumulative commissions via API.
        # Zero is returned; override this if you add local fee tracking.
        return 0.0

    # ------------------------------------------------------------------
    # Internal helpers

    def _restore_intraday_state(self) -> None:
        """
        Populate _intraday_buys and _intraday_sells from today's filled orders.
        Called on __init__ so that PDT tracking survives a process restart mid-day.
        """
        today = datetime.now(timezone.utc).date()
        # ISO-8601 range covering the full UTC trading day
        from_ts = f"{today.isoformat()}T00:00:00.000Z"
        to_ts   = f"{today.isoformat()}T23:59:59.999Z"

        try:
            orders = self.client.get_orders(
                self.account_number,
                from_entered_time=from_ts,
                to_entered_time=to_ts,
                status="FILLED",
            )
        except Exception as e:
            self._log.warning(f"Could not restore intraday state from order history: {e}")
            return

        buys, sells = 0, 0
        for order in orders:
            for leg in order.get("orderLegCollection", []):
                symbol = leg.get("instrument", {}).get("symbol")
                instruction = leg.get("instruction", "").upper()
                if not symbol or instruction not in ("BUY", "SELL"):
                    continue
                if instruction == "BUY":
                    self._record_buy(symbol, today)
                    buys += 1
                else:
                    self._record_sell(symbol, today)
                    sells += 1

        if buys or sells:
            self._log.info(
                f"Restored intraday state from order history: "
                f"{buys} buy(s), {sells} sell(s) for {today}"
            )
    # ------------------------------------------------------------------

    def _get_shares(self, symbol: str) -> float:
        positions = self.client.get_positions(self.account_number)
        for p in positions:
            if p["instrument"]["symbol"].upper() == symbol.upper():
                return float(p.get("longQuantity", 0.0))
        return 0.0

    def _wait_for_fill(self, order_id: Optional[str]) -> bool:
        if order_id is None:
            return False
        deadline = time.monotonic() + self.order_confirm_timeout
        while time.monotonic() < deadline:
            try:
                order = self.client.get_order(self.account_number, order_id)
                status = order.get("status", "")
                if status == _FILLED_STATUS:
                    return True
                if status in _FAILED_STATUSES:
                    self._log.error(f"Order {order_id} ended with status '{status}'")
                    return False
            except Exception as e:
                self._log.warning(f"Error polling order {order_id}: {e}")
            time.sleep(0.5)
        return False

