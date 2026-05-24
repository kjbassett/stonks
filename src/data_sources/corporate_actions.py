import datetime
from typing import List

from polygon.reference_apis.reference_api import AsyncReferenceClient
from src.data_access.dao_manager import dao_manager
from src.utils.market_calendar import earliest_market_time
from src.utils.project_utilities import call_limiter, config
from webrock.decorator import plugin


async def _get_recent_splits(client: AsyncReferenceClient, since_date: str) -> list:
    """Fetch all stock splits on or after ``since_date`` from Polygon.

    Args:
        client: Authenticated Polygon async reference client.
        since_date: ISO date string ``"YYYY-MM-DD"``; lower bound on execution_date.

    Returns:
        List of split result dicts with keys ticker, execution_date, split_from, split_to.
    """
    return await client.get_stock_splits(
        execution_date_gte=since_date, all_pages=True
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
async def sync_split_adjustments(since_date: str = "") -> str:
    """Clear stale price data for companies that had a stock split since ``since_date``.

    Fetches splits from Polygon, skips any already recorded in StockSplit, deletes
    all TradingData / TradingDataAggregation / TradingDataAttemptedQueries for each
    affected tracked company, and records the split so subsequent runs are no-ops.
    Run fill_missing after this to repopulate with corrected adjusted prices.

    Args:
        since_date: ISO date ``"YYYY-MM-DD"`` lower bound. Defaults to the earliest
            stored data date so the full history window is covered.

    Returns:
        Summary string listing cleared companies and how many were skipped.
    """
    if not since_date:
        since_date = datetime.date.fromtimestamp(earliest_market_time()).isoformat()

    cleared: List[str] = []
    skipped = 0

    async with call_limiter:
        async with AsyncReferenceClient(config["polygon_io"], True) as client:
            splits = await _get_recent_splits(client, since_date)

    for split in splits:
        ticker = split.get("ticker", "")
        execution_date = split.get("execution_date", "")

        if await _is_split_processed(ticker, execution_date):
            skipped += 1
            continue

        split_row = {
            "ticker": ticker,
            "execution_date": execution_date,
            "split_from": split.get("split_from", 0),
            "split_to": split.get("split_to", 0),
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
        print(f"Cleared price data for {ticker} (split {execution_date})")

    cleared_str = ", ".join(cleared) if cleared else "none"
    return (
        f"Cleared data for {len(cleared)} companies: [{cleared_str}]. "
        f"Skipped {skipped} (already processed or untracked). "
        f"Run fill_missing to repopulate."
    )
