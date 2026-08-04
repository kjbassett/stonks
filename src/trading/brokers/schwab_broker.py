"""
Live order execution through the Charles Schwab Trader API.

Use dry_run=True (default) to log what would be traded without placing real orders.
Flip to dry_run=False only after validating the integration against paper results.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from src.trading.brokers.schwab_auth import SchwabAuth
from src.trading.brokers.schwab_client import SchwabClient
from src.trading.brokers.base_broker import BaseBroker, TradeResult
from src.trading.portfolio import Position
from src.utils.project_utilities import config


# Order statuses that indicate a terminal failure
_FAILED_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}
_FILLED_STATUS = "FILLED"

_log = logging.getLogger("trading.schwab_broker")


async def _discover_account_number(client: SchwabClient) -> str:
    """
    Fetch the hash value Schwab uses in API URLs for the first linked account.

    Schwab's Trader API requires the *hashValue* (not the plain account number)
    in all URL paths. This is discovered via the /accounts/accountNumbers endpoint
    and saved to config.json for future runs.

    Raises:
        ValueError: If no accounts are returned or the response is unexpected.
    """
    entries = await client.get_account_numbers()
    if not entries:
        raise ValueError("No Schwab accounts found for this token.")
    try:
        hash_value = str(entries[0]["hashValue"])
    except (KeyError, IndexError) as e:
        raise ValueError(f"Unexpected /accounts/accountNumbers response: {e}") from e
    _log.info(f"Auto-discovered Schwab account hash: {hash_value}")
    _persist_account_number(hash_value)
    return hash_value


def _persist_account_number(account_number: str) -> None:
    """Write the discovered account number back to config.json."""
    try:
        with open("config.json", "r") as f:
            cfg = json.load(f)
        cfg["schwab"]["account_number"] = account_number
        with open("config.json", "w") as f:
            json.dump(cfg, f, indent=4)
        _log.info("Saved account number to config.json.")
    except OSError as e:
        _log.warning(f"Could not persist account number to config.json: {e}")


class SchwabBroker(BaseBroker):
    """
    Implements BaseBroker for the Charles Schwab Trader API.

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
        dry_run: bool = True,
        order_confirm_timeout: float = 15.0,
        client: Optional[SchwabClient] = None,
        account_number: Optional[str] = None,
    ):
        if client is None:
            raise ValueError(
                "A SchwabClient is required. Use SchwabBroker.from_auth() to "
                "obtain one with automatic token management."
            )
        cfg = config["schwab"]
        self.account_number = account_number or cfg["account_number"]
        self.client = client
        self.dry_run = dry_run
        self.order_confirm_timeout = order_confirm_timeout
        self._log = logging.getLogger("trading.schwab_broker")

        if dry_run:
            self._log.info(
                "SchwabBroker running in DRY RUN mode - no real orders will be placed."
            )

    @classmethod
    async def from_auth(
        cls,
        dry_run: bool = True,
        order_confirm_timeout: float = 15.0,
        account_number: Optional[str] = None,
    ) -> "SchwabBroker":
        """
        Create a SchwabBroker with tokens managed automatically by SchwabAuth.

        Loads saved tokens (refreshing if needed) from the token file configured
        in config.json. If account_number is not provided and not set in config,
        it is auto-discovered from the /accounts endpoint and saved to config.json.
        Run SchwabAuth().authorize() once before using this.
        """
        client = await SchwabAuth().get_client()
        resolved = account_number or config["schwab"].get("account_number") or ""
        if not resolved:
            resolved = await _discover_account_number(client)
        return cls(
            dry_run=dry_run,
            order_confirm_timeout=order_confirm_timeout,
            client=client,
            account_number=resolved,
        )

    # ------------------------------------------------------------------
    # BaseBroker
    # ------------------------------------------------------------------

    async def fill_order(
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
                f"[DRY RUN] {instruction} {shares_to_trade} x {symbol} @ ~${price:.2f}"
            )
            return TradeResult(symbol, float(signed), price, True, "dry_run")

        order_id = await self.client.place_order(self.account_number, order)
        self._log.info(
            f"Placed {instruction} {shares_to_trade} x {symbol} | order_id={order_id}"
        )

        fill_status = await self._wait_for_fill(order_id)
        filled = fill_status == _FILLED_STATUS

        if fill_status in _FAILED_STATUSES:
            self._log.error(
                f"Order {order_id} ended with terminal status '{fill_status}'"
            )
            reason = fill_status.lower()
        elif fill_status is None:
            self._log.warning(
                f"Order {order_id} not confirmed within {self.order_confirm_timeout}s. "
                f"Attempting to cancel."
            )
            await self._cancel_with_retry(order_id)
            reason = "timeout"
        else:
            reason = "filled"

        return TradeResult(
            symbol, float(signed), price, filled,
            reason, order_id=order_id,
        )

    async def get_positions(self) -> Dict[str, Position]:
        positions = await self.client.get_positions(self.account_number)
        result = {}
        for p in positions:
            symbol = p["instrument"]["symbol"].upper()
            result[symbol] = Position(
                shares=float(p.get("longQuantity", 0.0)),
                avg_price=float(p.get("averagePrice", 0.0)),
            )
        return result

    async def get_equity(self) -> float:
        """Query Schwab for current account liquidation value."""
        return await self.client.get_account_equity(self.account_number)

    def get_fees_paid(self) -> float:
        # Schwab doesn't expose cumulative commissions via API.
        # Zero is returned; override this if you add local fee tracking.
        return 0.0

    async def get_today_fills(self) -> List[Tuple[str, str]]:
        """
        Query today's filled orders from the API.
        Returns (symbol, 'BUY'|'SELL') pairs for PDT state restoration in OrderExecutor.
        Raises on network errors — caught by OrderExecutor._restore_intraday_state.
        """
        today = datetime.now(timezone.utc).date()
        from_ts = f"{today.isoformat()}T00:00:00.000Z"
        to_ts   = f"{today.isoformat()}T23:59:59.999Z"

        orders = await self.client.get_orders(
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

    async def _cancel_with_retry(self, order_id: str) -> None:
        """
        Send a cancel request for an order, retrying once on failure.

        Logs each attempt and its outcome. Does not raise — a failed cancel
        is logged as an error and callers should treat the order state as unknown.
        """
        for attempt in range(1, 3):
            try:
                await self.client.cancel_order(self.account_number, order_id)
                self._log.info(
                    f"Cancel request sent for order {order_id} (attempt {attempt}/2)"
                )
                return
            except Exception as e:
                self._log.error(
                    f"Cancel attempt {attempt}/2 failed for order {order_id}: {e}",
                    exc_info=True,
                )
        self._log.error(
            f"All cancel attempts exhausted for order {order_id}. "
            f"Order may still be active on Schwab — verify manually."
        )

    async def _wait_for_fill(self, order_id: Optional[str]) -> Optional[str]:
        """
        Poll until the order reaches a terminal state or the timeout expires.

        Returns the final Schwab status string (e.g. "FILLED", "REJECTED"),
        or None if the poll deadline was reached without a terminal status.
        """
        if order_id is None:
            return None
        deadline = time.monotonic() + self.order_confirm_timeout
        while time.monotonic() < deadline:
            try:
                order = await self.client.get_order(self.account_number, order_id)
                self._log.debug("Order status poll: %s", order)
                status = order.get("status", "")
                if status == _FILLED_STATUS or status in _FAILED_STATUSES:
                    return status
            except Exception as e:
                self._log.warning(
                    f"Error polling order {order_id}: {e}", exc_info=True
                )
            await asyncio.sleep(0.5)
        return None
