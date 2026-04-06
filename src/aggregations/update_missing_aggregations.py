import asyncio
from datetime import datetime
from typing import List

import pandas as pd
from webrock.decorator import plugin

from src.utils.market_calendar import earliest_market_time


@plugin()
async def update_hourly_aggregations(
    companies: str = None,
    hours_per_query: int = 1440,
    companies_per_query: int = 10,
) -> None:
    """Update missing hourly OHLCV aggregations for all (or selected) companies.

    Args:
        companies: Comma-separated ticker symbols to process. Omit to process all.
        hours_per_query: Size of each time window in hours.
        companies_per_query: Number of companies to process per DB call.
    """
    # TODO src/aggregations folder appears before the data_access folder, so dao_manager had no daos until I put dao manager here.
    from src.data_access.dao_manager import dao_manager

    cmp = dao_manager.get_dao("Company")
    tda = dao_manager.get_dao("TradingDataAggregation")
    window = 3600 * hours_per_query
    earliest_timestamp = earliest_market_time()

    all_companies = (
        await cmp.get(symbol=companies) if companies else await cmp.get()
    )
    chunks = _split_chunks(all_companies, companies_per_query)

    for chunk in chunks:
        await _process_chunk(tda, chunk, earliest_timestamp, window)

    print("Missing hourly aggregations updated.")


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
) -> None:
    """Aggregate all time windows for a chunk of companies concurrently.

    Args:
        tda: TradingDataAggregation DAO.
        chunk: Subset of the companies DataFrame to process.
        earliest_timestamp: Earliest unix timestamp to aggregate from.
        window: Time window size in seconds.
    """
    company_ids: List[int] = list(chunk["id"])
    symbols: List[str] = list(chunk["symbol"])
    current_iter_start = int(datetime.now().timestamp())
    print(f"Processing chunk {symbols}")

    tasks = [
        asyncio.create_task(
            tda.update_missing_hourly_aggregations(company_ids, t, t + window)
        )
        for t in range(earliest_timestamp, current_iter_start, window)
    ]
    await asyncio.gather(*tasks)

    elapsed = datetime.now().timestamp() - current_iter_start
    print(f"Finished chunk {symbols}. Time taken: {elapsed:.1f}s")
