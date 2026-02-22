"""
Schwab Trader API client.

Base URLs:
  Market data:  https://api.schwabapi.com/marketdata/v1
  Trading:      https://api.schwabapi.com/trader/v1

Authentication: OAuth2 Bearer token (obtain via Schwab developer portal OAuth flow).
Pass the access_token to SchwabClient. Token refresh is the caller's responsibility.

Rate limit: ~35,000 requests per 10 minutes per the developer portal.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

_log = logging.getLogger(__name__)

MARKET_BASE = "https://api.schwabapi.com/marketdata/v1"
TRADER_BASE = "https://api.schwabapi.com/trader/v1"


class SchwabAPIError(Exception):
    """Raised when Schwab returns a non-2xx response."""

    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
        super().__init__(f"Schwab API error {status_code}: {body}")


class SchwabClient:
    """
    Thin wrapper around the Schwab REST API.

    All methods raise SchwabAPIError on non-2xx responses.
    """

    def __init__(self, access_token: str):
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def update_token(self, access_token: str) -> None:
        """Replace the Bearer token (call after an OAuth token refresh)."""
        self._session.headers["Authorization"] = f"Bearer {access_token}"

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_quotes(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Fetch real-time NBBO quotes for one or more symbols.

        Returns a dict keyed by symbol, each value containing:
          quote.lastPrice, quote.bidPrice, quote.askPrice, quote.closePrice, etc.
        """
        resp = self._get(
            f"{MARKET_BASE}/quotes",
            params={"symbols": ",".join(symbols), "fields": "quote,reference"},
        )
        return resp

    # ------------------------------------------------------------------
    # Account information
    # ------------------------------------------------------------------

    def get_accounts(self) -> List[Dict[str, Any]]:
        """Return a list of linked accounts with basic info."""
        return self._get(f"{TRADER_BASE}/accounts")

    def get_account(self, account_number: str, fields: str = "positions") -> Dict[str, Any]:
        """
        Fetch account details.

        fields: comma-separated subset of "positions" (balances always included).
        Key response fields under securitiesAccount:
          currentBalances.liquidationValue  — total account value
          currentBalances.cashAvailableForTrading
          currentBalances.dayTradingBuyingPower
          positions[].instrument.symbol
          positions[].longQuantity
          positions[].averagePrice
          positions[].marketValue
        """
        return self._get(
            f"{TRADER_BASE}/accounts/{account_number}",
            params={"fields": fields},
        )

    def get_account_equity(self, account_number: str) -> float:
        """Convenience: return current liquidation value of the account."""
        data = self.get_account(account_number, fields="")
        return data["securitiesAccount"]["currentBalances"]["liquidationValue"]

    def get_positions(self, account_number: str) -> List[Dict[str, Any]]:
        """Return list of open positions for the account."""
        data = self.get_account(account_number, fields="positions")
        return data["securitiesAccount"].get("positions", [])

    def get_cash_available(self, account_number: str) -> float:
        """Return cash available for trading."""
        data = self.get_account(account_number, fields="")
        return data["securitiesAccount"]["currentBalances"]["cashAvailableForTrading"]

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def place_order(self, account_number: str, order: Dict[str, Any]) -> Optional[str]:
        """
        Submit an order. Returns the order ID extracted from the Location header,
        or None if the header is absent.

        Order statuses: AWAITING_PARENT_ORDER, PENDING_ACTIVATION, QUEUED,
                        WORKING, REJECTED, PENDING_CANCEL, CANCELED, PENDING_REPLACE,
                        REPLACED, FILLED, EXPIRED, NEW, AWAITING_CONDITION,
                        AWAITING_STOP_CONDITION, AWAITING_MANUAL_REVIEW, ACCEPTED,
                        AWAITING_UR_OUT, PENDING_ACKNOWLEDGEMENT, PENDING_RECALL,
                        UNKNOWN
        """
        resp = self._raw_post(f"{TRADER_BASE}/accounts/{account_number}/orders", json=order)
        location = resp.headers.get("Location", "")
        return location.rstrip("/").split("/")[-1] if location else None

    def get_order(self, account_number: str, order_id: str) -> Dict[str, Any]:
        """Fetch the current state of an order."""
        return self._get(f"{TRADER_BASE}/accounts/{account_number}/orders/{order_id}")

    def cancel_order(self, account_number: str, order_id: str) -> None:
        """Cancel an open order."""
        self._delete(f"{TRADER_BASE}/accounts/{account_number}/orders/{order_id}")

    def get_orders(
        self,
        account_number: str,
        from_entered_time: Optional[str] = None,
        to_entered_time: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all orders for an account, optionally filtered.

        Timestamps: ISO-8601 strings, e.g. "2026-01-01T00:00:00.000Z"
        Status: e.g. "FILLED", "WORKING", "CANCELED"
        """
        params: Dict[str, str] = {}
        if from_entered_time:
            params["fromEnteredTime"] = from_entered_time
        if to_entered_time:
            params["toEnteredTime"] = to_entered_time
        if status:
            params["status"] = status
        return self._get(f"{TRADER_BASE}/accounts/{account_number}/orders", params=params)

    # ------------------------------------------------------------------
    # Order builders
    # ------------------------------------------------------------------

    @staticmethod
    def build_market_order(symbol: str, quantity: float, instruction: str) -> Dict[str, Any]:
        """
        Build a simple equity market order payload.

        instruction: "BUY" or "SELL"
        quantity: number of shares (will be rounded to nearest whole share)
        """
        return {
            "session": "NORMAL",
            "duration": "DAY",
            "orderType": "MARKET",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "orderLegType": "EQUITY",
                    "instruction": instruction.upper(),
                    "quantity": round(abs(quantity)),
                    "instrument": {
                        "symbol": symbol.upper(),
                        "assetType": "EQUITY",
                    },
                }
            ],
        }

    @staticmethod
    def build_limit_order(
        symbol: str, quantity: float, instruction: str, limit_price: float
    ) -> Dict[str, Any]:
        """
        Build a DAY limit order. Useful for controlling fill price.

        instruction: "BUY" or "SELL"
        """
        return {
            "session": "NORMAL",
            "duration": "DAY",
            "orderType": "LIMIT",
            "price": round(limit_price, 2),
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "orderLegType": "EQUITY",
                    "instruction": instruction.upper(),
                    "quantity": round(abs(quantity)),
                    "instrument": {
                        "symbol": symbol.upper(),
                        "assetType": "EQUITY",
                    },
                }
            ],
        }

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: Optional[Dict] = None) -> Any:
        resp = self._session.get(url, params=params)
        self._raise_for_status(resp)
        return resp.json()

    def _raw_post(self, url: str, json: Any) -> requests.Response:
        resp = self._session.post(url, json=json)
        self._raise_for_status(resp)
        return resp

    def _delete(self, url: str) -> None:
        resp = self._session.delete(url)
        self._raise_for_status(resp)

    @staticmethod
    def _raise_for_status(resp: requests.Response) -> None:
        if not resp.ok:
            raise SchwabAPIError(resp.status_code, resp.text[:500])
