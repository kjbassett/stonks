import asyncio
import logging
from datetime import datetime
from typing import List

import pandas as pd
from webrock.decorator import plugin
from webrock.pause import wait_if_paused

from src.utils.market_calendar import earliest_market_time

_log = logging.getLogger("aggregations.update")

WATCHLIST_KEYWORD = "watchlist"
UPDATE_HOURLY_PLUGIN_ID = "src.aggregations.update_missing_aggregations.update_hourly_aggregations"


@plugin()
async def update_hourly_aggregations(
    companies: str = "",
    hours_per_query: int = 1440,
    companies_per_query: int = 10,
    lookback_hours: int = 0,
) -> None:
    """Update missing hourly OHLCV aggregations for all (or selected) companies.

    Args:
        companies: Comma-separated ticker symbols, ``"watchlist"``, or empty for all.
        hours_per_query: Size of each time window in hours.
        companies_per_query: Number of companies to process per DB call.
        lookback_hours: Only aggregate this many hours back from now. 0 = all time.
    """
    # Deferred import: aggregations/ loads before data_access/ so dao_manager has
    # no DAOs yet at import time.
    from src.data_access.dao_manager import dao_manager

    cmp = dao_manager.get_dao("Company")
    tda = dao_manager.get_dao("TradingDataAggregation")
    window = 3600 * hours_per_query

    if companies == WATCHLIST_KEYWORD:
        from src.data_sources.watchlist import get_watchlist_and_held_symbols
        symbols = await get_watchlist_and_held_symbols()
        all_companies = await cmp.get()
        all_companies = all_companies[all_companies["symbol"].isin(symbols)]
    elif companies:
        all_companies = await cmp.get(symbol=companies)
    else:
        all_companies = await cmp.get()

    now = int(datetime.now().timestamp())
    earliest_timestamp = (
        max(earliest_market_time(), now - lookback_hours * 3600)
        if lookback_hours
        else earliest_market_time()
    )

    chunks = _split_chunks(all_companies, companies_per_query)
    n_chunks = len(chunks)
    for i, chunk in enumerate(chunks, 1):
        await wait_if_paused(UPDATE_HOURLY_PLUGIN_ID)
        await _process_chunk(tda, chunk, earliest_timestamp, window, i, n_chunks)

    _log.info("Hourly aggregations updated")


def _split_chunks(companies: pd.DataFrame, companies_per_query: int) -> List[pd.DataFrame]:
    """Split a companies DataFrame into a list of DataFrames of at most companies_per_query rows.

    Args:
        companies: Full companies DataFrame.
        companies_per_query: Maximum number of rows per chunk.

    Returns:
        List of DataFrame chunks.
    """
    return [
        companies.iloc[i : i + companies_per_query]
        for i in range(0, len(companies), companies_per_query)
    ]


async def _process_chunk(
    tda,
    chunk: pd.DataFrame,
    earliest_timestamp: int,
    window: int,
    chunk_index: int,
    n_chunks: int,
) -> None:
    """Aggregate all time windows for a chunk of companies concurrently.

    Args:
        tda: TradingDataAggregation DAO.
        chunk: Subset of the companies DataFrame to process.
        earliest_timestamp: Earliest unix timestamp to aggregate from.
        window: Time window size in seconds.
        chunk_index: 1-based index of this chunk.
        n_chunks: Total number of chunks.
    """
    company_ids: List[int] = list(chunk["id"])
    symbols: List[str] = list(chunk["symbol"])
    current_iter_start = int(datetime.now().timestamp())
    _log.info("[%d/%d] Processing chunk %s", chunk_index, n_chunks, symbols)

    tasks = [
        asyncio.create_task(
            tda.update_missing_hourly_aggregations(company_ids, t, t + window)
        )
        for t in range(earliest_timestamp, current_iter_start, window)
    ]
    await asyncio.gather(*tasks)

    elapsed = datetime.now().timestamp() - current_iter_start
    _log.info("[%d/%d] Finished chunk %s in %.1fs", chunk_index, n_chunks, symbols, elapsed)
