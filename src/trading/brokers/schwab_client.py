"""
Schwab Trader API client.

Base URLs:
  Market data:  https://api.schwabapi.com/marketdata/v1
  Trading:      https://api.schwabapi.com/trader/v1

Authentication: OAuth2 Bearer token (obtain via Schwab developer portal OAuth flow).
Pass the access_token to SchwabClient. Token refresh is the caller's responsibility.

Rate limit: ~35,000 requests per 10 minutes per the developer portal.
"""

from typing import Any, Dict, List, Optional

import httpx

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
    Thin async wrapper around the Schwab REST API.

    All methods raise SchwabAPIError on non-2xx responses.
    Call aclose() (or use as an async context manager) to release the
    underlying HTTP session when done.
    """

    def __init__(self, access_token: str):
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def update_token(self, access_token: str) -> None:
        """Replace the Bearer token (call after an OAuth token refresh)."""
        self._client.headers["Authorization"] = f"Bearer {access_token}"

    async def aclose(self) -> None:
        """Close the underlying HTTP session."""
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    async def get_quotes(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Fetch real-time NBBO quotes for one or more symbols.

        Returns a dict keyed by symbol, each value containing:
          quote.lastPrice, quote.bidPrice, quote.askPrice, quote.closePrice, etc.
        """
        return await self._get(
            f"{MARKET_BASE}/quotes",
            params={"symbols": ",".join(symbols), "fields": "quote,reference"},
        )

    # ------------------------------------------------------------------
    # Account information
    # ------------------------------------------------------------------

    async def get_accounts(self) -> List[Dict[str, Any]]:
        """Return a list of linked accounts with basic info."""
        return await self._get(f"{TRADER_BASE}/accounts")

    async def get_account(self, account_number: str, fields: str = "positions") -> Dict[str, Any]:
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
        return await self._get(
            f"{TRADER_BASE}/accounts/{account_number}",
            params={"fields": fields},
        )

    async def get_account_equity(self, account_number: str) -> float:
        """Convenience: return current liquidation value of the account."""
        data = await self.get_account(account_number, fields="")
        return data["securitiesAccount"]["currentBalances"]["liquidationValue"]

    async def get_positions(self, account_number: str) -> List[Dict[str, Any]]:
        """Return list of open positions for the account."""
        data = await self.get_account(account_number, fields="positions")
        return data["securitiesAccount"].get("positions", [])

    async def get_cash_available(self, account_number: str) -> float:
        """Return cash available for trading."""
        data = await self.get_account(account_number, fields="")
        return data["securitiesAccount"]["currentBalances"]["cashAvailableForTrading"]

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def place_order(self, account_number: str, order: Dict[str, Any]) -> Optional[str]:
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
        resp = await self._raw_post(f"{TRADER_BASE}/accounts/{account_number}/orders", json=order)
        location = resp.headers.get("Location", "")
        return location.rstrip("/").split("/")[-1] if location else None

    async def get_order(self, account_number: str, order_id: str) -> Dict[str, Any]:
        """Fetch the current state of an order."""
        return await self._get(f"{TRADER_BASE}/accounts/{account_number}/orders/{order_id}")

    async def cancel_order(self, account_number: str, order_id: str) -> None:
        """Cancel an open order."""
        await self._delete(f"{TRADER_BASE}/accounts/{account_number}/orders/{order_id}")

    async def get_orders(
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
        return await self._get(f"{TRADER_BASE}/accounts/{account_number}/orders", params=params)

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

    async def _get(self, url: str, params: Optional[Dict] = None) -> Any:
        resp = await self._client.get(url, params=params)
        self._raise_for_status(resp)
        return resp.json()

    async def _raw_post(self, url: str, json: Any) -> httpx.Response:
        resp = await self._client.post(url, json=json)
        self._raise_for_status(resp)
        return resp

    async def _delete(self, url: str) -> None:
        resp = await self._client.delete(url)
        self._raise_for_status(resp)

    @staticmethod
    def _raise_for_status(resp: httpx.Response) -> None:
        if not resp.is_success:
            raise SchwabAPIError(resp.status_code, resp.text[:500])
