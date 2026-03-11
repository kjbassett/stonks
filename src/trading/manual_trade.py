"""
Manual single-trade plugins for live Schwab API testing.

These plugins bypass the full TradingEngine and call SchwabBroker directly,
allowing you to verify Schwab connectivity and order flow before enabling
automated trading. Trades only execute during NYSE market hours.

Usage (web UI or direct call)::

    await test_buy("AAPL", 1)           # buy 1 share at market price
    await test_sell("AAPL", 1, "180.0") # sell 1 share, price logged as $180
"""

import logging
from typing import Optional

from webrock.decorator import plugin

from src.trading.brokers.base_broker import TradeResult
from src.trading.brokers.schwab_broker import SchwabBroker
from src.trading.brokers.schwab_client import SchwabClient
from src.utils.market_calendar import is_currently_open

_log = logging.getLogger("trading.manual")

_MIN_TIMEOUT: float = 5.0
_MAX_TIMEOUT: float = 600.0


@plugin()
async def test_buy(
    symbol: str,
    quantity: int,
    price: str = "auto",
    timeout: float = 60.0,
) -> None:
    """
    Place a single live BUY order on Schwab for manual testing.

    Args:
        symbol: Ticker symbol (e.g. "AAPL"). Case-insensitive, letters only.
        quantity: Number of whole shares to buy (>= 1).
        price: Price per share for logging. Use "auto" to fetch current market price.
        timeout: Seconds to wait for fill before cancelling the order (5–600).
    """
    _validate_inputs(symbol, quantity, price, timeout)
    await _execute_manual_trade(symbol.strip().upper(), quantity, price, timeout)


@plugin()
async def test_sell(
    symbol: str,
    quantity: int,
    price: str = "auto",
    timeout: float = 60.0,
) -> None:
    """
    Place a single live SELL order on Schwab for manual testing.

    Args:
        symbol: Ticker symbol (e.g. "AAPL"). Case-insensitive, letters only.
        quantity: Number of whole shares to sell (>= 1).
        price: Price per share for logging. Use "auto" to fetch current market price.
        timeout: Seconds to wait for fill before cancelling the order (5–600).
    """
    _validate_inputs(symbol, quantity, price, timeout)
    await _execute_manual_trade(symbol.strip().upper(), -quantity, price, timeout)


async def _execute_manual_trade(
    symbol: str, shares_delta: int, price: str, timeout: float
) -> None:
    """Validate market hours, connect to Schwab, resolve price, and submit the order."""
    if not is_currently_open():
        raise ValueError(
            "NYSE is currently closed. Manual trades are only allowed during market hours."
        )

    direction = "BUY" if shares_delta > 0 else "SELL"
    broker = await SchwabBroker.from_auth(dry_run=False, order_confirm_timeout=timeout)

    cash = await _verify_connection_and_get_balance(broker)
    resolved_price = await _resolve_price(broker.client, symbol, price)

    _log.info(
        f"Manual {direction} starting: {abs(shares_delta)} x {symbol} "
        f"@ ~${resolved_price:.2f} | timeout={timeout}s | cash_available=${cash:,.2f}"
    )

    result = await broker.fill_order(symbol, float(shares_delta), resolved_price)
    _log_trade_result(result, direction)


async def _verify_connection_and_get_balance(broker: SchwabBroker) -> float:
    """
    Fetch available cash to confirm Schwab connectivity.

    Raises the underlying exception if the API call fails so the caller
    can surface a clear connectivity error before any order is placed.
    """
    try:
        cash = await broker.client.get_cash_available(broker.account_number)
        _log.info(f"Schwab connection OK. Cash available for trading: ${cash:,.2f}")
        return cash
    except Exception as e:
        _log.error(f"Schwab connectivity check failed: {e}", exc_info=True)
        raise


async def _resolve_price(client: SchwabClient, symbol: str, price: str) -> float:
    """Return price as a float, fetching the last trade price from Schwab if 'auto'."""
    if price != "auto":
        return float(price)
    quotes = await client.get_quotes([symbol])
    last_price = quotes.get(symbol, {}).get("quote", {}).get("lastPrice")
    if last_price is None:
        raise ValueError(f"Could not fetch market price for {symbol!r} from Schwab.")
    _log.info(f"Auto-resolved price for {symbol}: ${float(last_price):.2f}")
    return float(last_price)


def _validate_inputs(symbol: str, quantity: int, price: str, timeout: float) -> None:
    """Raise ValueError for any invalid plugin input."""
    if not symbol or not symbol.strip().isalpha():
        raise ValueError(
            f"Invalid symbol {symbol!r}: must contain letters only, no spaces or digits."
        )
    if quantity < 1:
        raise ValueError(f"quantity must be >= 1, got {quantity}.")
    if price != "auto":
        try:
            parsed = float(price)
            if parsed <= 0:
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError(
                f"price must be 'auto' or a positive number, got {price!r}."
            )
    if not (_MIN_TIMEOUT <= timeout <= _MAX_TIMEOUT):
        raise ValueError(
            f"timeout must be between {_MIN_TIMEOUT} and {_MAX_TIMEOUT} seconds, "
            f"got {timeout}."
        )


def _log_trade_result(result: TradeResult, direction: str) -> None:
    """Log the final outcome of a manual trade at the appropriate level."""
    if result.filled:
        _log.info(
            f"Manual {direction} FILLED: {abs(result.shares_delta):.0f} x {result.symbol} "
            f"@ ~${result.price:.2f} | reason={result.reason} | order_id={result.order_id}"
        )
    else:
        _log.warning(
            f"Manual {direction} NOT FILLED: {result.symbol} "
            f"| reason={result.reason} | order_id={result.order_id}"
        )
