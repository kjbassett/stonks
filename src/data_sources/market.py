import asyncio
import datetime
import logging

from massive import RESTClient
from src.data_access.dao_manager import dao_manager
from src.data_sources.missing_data import fill_gaps
from src.utils.project_utilities import config, make_rest_client
from webrock.decorator import plugin
from webrock.pause import wait_if_paused

_log = logging.getLogger("data_sources.market")

FILL_MISSING_PLUGIN_ID = "src.data_sources.market.fill_missing"


async def _get_data(client: RESTClient, symbol: str, start: int, end: int):
    """Fetch minute aggregate bars for a symbol over a time range.

    Args:
        client: Massive REST client.
        symbol: Ticker symbol.
        start: Unix timestamp (seconds) for range start.
        end: Unix timestamp (seconds) for range end.

    Returns:
        List of Agg objects.
    """
    start_dt = datetime.datetime.fromtimestamp(start, tz=datetime.timezone.utc)
    end_dt = datetime.datetime.fromtimestamp(end, tz=datetime.timezone.utc)
    return await asyncio.to_thread(
        lambda: list(
            client.list_aggs(symbol, 1, "minute", start_dt, end_dt,
                             adjusted=True, limit=50000)
        )
    )


async def save_data(company_id: int, data: list) -> int:
    """Persist a list of Agg bars to TradingData.

    Args:
        company_id: Database ID of the company.
        data: List of Agg objects from the Massive API.

    Returns:
        Number of rows inserted.
    """
    td = dao_manager.get_dao("TradingData")
    try:
        rows = [
            {
                "company_id": company_id,
                "open": d.open,
                "high": d.high,
                "low": d.low,
                "close": d.close,
                "vw_average": d.vwap,
                "volume": d.volume,
                "timestamp": d.timestamp // 1000,
            }
            for d in data
        ]
    except (AttributeError, TypeError) as e:
        _log.error("Market data error: %s", e)
        return 1
    n = await td.insert(rows)
    _log.info("%d rows inserted into TradingData", n)
    return n


@plugin()
async def fill_missing(companies: str = ""):
    """Fill gaps in TradingData by fetching missing minute bars from Massive.

    Args:
        companies: Comma-separated ticker symbols, ``"watchlist"``, or empty for all.
    """
    td = dao_manager.get_dao("TradingData")
    client = make_rest_client(32)
    await fill_gaps(
        client,
        "TradingData",
        td.get_timestamps_by_company,
        _get_data,
        save_data,
        companies,
        min_gap_size=1800,  # 30 minutes
        max_gap_size=86400 * 30,  # 30 days
        adjust_for_market_hours=True,
        pause_check=lambda: wait_if_paused(FILL_MISSING_PLUGIN_ID),
    )


@plugin()
async def query_api(symbol: str, start: int, end: int):
    """Fetch aggregate bars directly from the Massive API for inspection.

    Args:
        symbol: Ticker symbol, or ``"all"``/``"*"`` for no filter.
        start: Unix timestamp (seconds) for range start.
        end: Unix timestamp (seconds) for range end.

    Returns:
        List of Agg objects.
    """
    if symbol in ("all", "*"):
        symbol = ""
    client = make_rest_client()
    return await _get_data(client, symbol, start, end)
