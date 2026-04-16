"""
Read-only account inspection plugins for the Schwab brokerage account.

Exposed via the web UI. No orders are placed — all calls use dry_run=True.
"""

import logging
from typing import Dict

from webrock.decorator import plugin

from src.trading.brokers.schwab_broker import SchwabBroker
from src.trading.portfolio import Position

_log = logging.getLogger("trading.account_info")


@plugin()
async def get_equity() -> float:
    """
    Fetch the current account liquidation value from Schwab.

    Returns:
        Current account equity in USD.
    """
    broker = await SchwabBroker.from_auth(dry_run=True)
    equity = await broker.get_equity()
    _log.info(f"Account equity: ${equity:,.2f}")
    return equity


@plugin()
async def get_positions() -> Dict[str, dict]:
    """
    Fetch all open positions currently held in the Schwab account.

    Returns:
        Dict mapping ticker symbol to ``{"shares": float, "avg_price": float}``.
    """
    broker = await SchwabBroker.from_auth(dry_run=True)
    positions = await broker.get_positions()
    result = _format_positions(positions)
    _log.info(f"Fetched {len(result)} position(s): {list(result)}")
    return result


def _format_positions(positions: Dict[str, Position]) -> Dict[str, dict]:
    """Convert Position dataclasses to JSON-serialisable dicts.

    Args:
        positions: Mapping of symbol -> Position from the broker.

    Returns:
        Same mapping with dataclasses replaced by plain dicts.
    """
    return {
        symbol: {"shares": pos.shares, "avg_price": pos.avg_price}
        for symbol, pos in positions.items()
    }
