import datetime
import logging
from typing import List

from massive import RESTClient
from src.data_access.dao_manager import dao_manager
from src.utils.market_calendar import earliest_market_time
from src.utils.project_utilities import call_limiter, config, make_rest_client
from webrock.decorator import plugin

_log = logging.getLogger("data_sources.corporate_actions")


async def _get_recent_splits(client: RESTClient, since_date: str) -> list:
    """Fetch all stock splits on or after ``since_date`` from Massive.

    Args:
        client: Massive REST client.
        since_date: ISO date string ``"YYYY-MM-DD"``; lower bound on execution_date.

    Returns:
        List of Split objects.
    """
    import asyncio
    return await asyncio.to_thread(
        lambda: list(client.list_splits(execution_date_gte=since_date))
    )


async def _is_split_processed(ticker: str, execution_date: str) -> bool:
    """Return True if this split has already been handled.

    Args:
        ticker: Stock ticker symbol.
        execution_date: ISO date string of the split.

    Returns:
        True when a matching row exists in StockSplit.
    """
    rows = await dao_manager.get_dao("StockSplit").get(
        ticker=ticker, execution_date=execution_date
    )
    return len(rows) > 0


async def _get_incremental_since_date() -> str:
    """Return the lower bound for an incremental split sync.

    Resumes from the latest execution_date already recorded in StockSplit
    (which accumulates every split ever seen, cleared or skipped — see
    sync_split_adjustments), falling back to earliest_market_time() when the
    table is empty (first-ever run).

    Returns:
        ISO date string ``"YYYY-MM-DD"``.
    """
    result = await dao_manager.db.execute_query(
        "SELECT MAX(execution_date) AS max_date FROM StockSplit",
        return_type="DataFrame",
    )
    max_date = result.iloc[0]["max_date"] if not result.empty else None
    if not max_date:
        return datetime.date.fromtimestamp(earliest_market_time()).isoformat()
    return max_date


async def _clear_company_price_data(company_id: int) -> None:
    """Delete all price data for a company so it can be re-fetched.

    Clears TradingData, TradingDataAggregation, and TradingDataAttemptedQueries
    so that fill_missing will re-fetch the full adjusted history.

    Args:
        company_id: Database ID of the company to clear.
    """
    db = dao_manager.db
    for table in ("TradingData", "TradingDataAggregation", "TradingDataAttemptedQueries"):
        await db.execute_query(
            f"DELETE FROM {table} WHERE company_id = ?",
            (company_id,),
            query_type="DELETE",
        )


@plugin()
async def sync_split_adjustments(since_days: int = 0) -> str:
    """Clear stale price data for companies that had a stock split recently.

    Fetches splits from Massive, skips any already recorded in StockSplit, deletes
    all TradingData / TradingDataAggregation / TradingDataAttemptedQueries for each
    affected tracked company, and records the split so subsequent runs are no-ops.
    Run fill_missing after this to repopulate with corrected adjusted prices.

    Args:
        since_days: Relative day offset (e.g. ``-730`` = last 730 days) for a manual
            historical reprocessing run. Defaults to 0, which incrementally resumes
            from the latest split already recorded (see _get_incremental_since_date).

    Returns:
        Summary string listing cleared companies and how many were skipped.
    """
    if since_days:
        since_date = (
            datetime.date.today() + datetime.timedelta(days=since_days)
        ).isoformat()
    else:
        since_date = await _get_incremental_since_date()

    cleared: List[str] = []
    skipped = 0

    async with call_limiter:
        client = make_rest_client(1)
        splits = await _get_recent_splits(client, since_date)

    for split in splits:
        ticker = split.ticker or ""
        execution_date = split.execution_date or ""

        if await _is_split_processed(ticker, execution_date):
            skipped += 1
            continue

        split_row = {
            "ticker": ticker,
            "execution_date": execution_date,
            "split_from": split.split_from or 0,
            "split_to": split.split_to or 0,
        }

        companies = await dao_manager.get_dao("Company").get(symbol=ticker)
        if len(companies) == 0:
            await dao_manager.get_dao("StockSplit").insert(split_row)
            skipped += 1
            continue

        company_id = int(companies.iloc[0]["id"])
        await _clear_company_price_data(company_id)
        await dao_manager.get_dao("StockSplit").insert(split_row)
        cleared.append(ticker)
        _log.info("Cleared price data for %s (split %s)", ticker, execution_date)

    cleared_str = ", ".join(cleared) if cleared else "none"
    return (
        f"Cleared data for {len(cleared)} companies: [{cleared_str}]. "
        f"Skipped {skipped} (already processed or untracked). "
        f"Run fill_missing to repopulate."
    )
