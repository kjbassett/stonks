"""
Live order execution through the Charles Schwab Trader API.

Use dry_run=True (default) to log what would be traded without placing real orders.
Flip to dry_run=False only after validating the integration against paper results.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from src.trading.brokers.schwab_client import SchwabClient
from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.portfolio import Position


# Order statuses that indicate a terminal failure
_FAILED_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}
_FILLED_STATUS = "FILLED"


class SchwabBroker(BaseBroker):
    """
    Implements BrokerInterface for the Charles Schwab Trader API.

    Unlike PaperBroker, portfolio state is owned by Schwab — this class
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
    """

    def __init__(
        self,
        client: SchwabClient,
        account_number: str,
        dry_run: bool = True,
        order_confirm_timeout: float = 15.0,
    ):
        self.client = client
        self.account_number = account_number
        self.dry_run = dry_run
        self.order_confirm_timeout = order_confirm_timeout
        self._log = logging.getLogger("SchwabBroker")

        if dry_run:
            self._log.info(
                "SchwabBroker running in DRY RUN mode — no real orders will be placed."
            )

    # ------------------------------------------------------------------
    # BrokerInterface
    # ------------------------------------------------------------------

    def fill_order(
        self,
        symbol: str,
        shares_delta: float,
        price: float,
    ) -> TradeResult:
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
                f"(target exposure via OrderExecutor)"
            )
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

        return TradeResult(
            symbol, float(signed), price, filled,
            "filled" if filled else "unconfirmed", order_id=order_id,
        )

    def get_positions(self) -> Dict[str, Position]:
        positions = self.client.get_positions(self.account_number)
        result = {}
        for p in positions:
            symbol = p["instrument"]["symbol"].upper()
            result[symbol] = Position(
                shares=float(p.get("longQuantity", 0.0)),
                avg_price=float(p.get("averagePrice", 0.0)),
            )
        return result

    def get_equity(self, _: Dict[str, float]) -> float:
        """Query Schwab for current account liquidation value."""
        return self.client.get_account_equity(self.account_number)

    def get_fees_paid(self) -> float:
        # Schwab doesn't expose cumulative commissions via API.
        # Zero is returned; override this if you add local fee tracking.
        return 0.0

    def get_today_fills(self) -> List[Tuple[str, str]]:
        """
        Query today's filled orders from the API.
        Returns (symbol, 'BUY'|'SELL') pairs for PDT state restoration in OrderExecutor.
        Raises on network errors — caught by OrderExecutor._restore_intraday_state.
        """
        today = datetime.now(timezone.utc).date()
        from_ts = f"{today.isoformat()}T00:00:00.000Z"
        to_ts   = f"{today.isoformat()}T23:59:59.999Z"

        orders = self.client.get_orders(
            self.account_number,
            from_entered_time=from_ts,
            to_entered_time=to_ts,
            status="FILLED",
        )

        fills = []
        for order in orders:
            for leg in order.get("orderLegCollection", []):
                symbol = leg.get("instrument", {}).get("symbol")
                instruction = leg.get("instruction", "").upper()
                if symbol and instruction in ("BUY", "SELL"):
                    fills.append((symbol, instruction))
        return fills

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
