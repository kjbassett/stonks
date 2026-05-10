"""Plugin for screening all companies using Polygon ticker detail data."""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
from polygon.reference_apis.reference_api import AsyncReferenceClient

from src.data_access.dao_manager import dao_manager
from src.utils.project_utilities import config
from webrock.decorator import plugin

_cmp = dao_manager.get_dao("Company")
_OUTPUT_CSV = Path(__file__).resolve().parents[2] / "company_screen.csv"

# Lower than the shared call_limiter (32) — the reference API is slower than
# market data endpoints and times out under heavy concurrent load.
_REQUEST_SEMAPHORE = asyncio.Semaphore(5)


def _extract_row(symbol: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one Polygon ticker detail response into a screening row.

    Args:
        symbol: The ticker symbol.
        result: The 'results' dict from the Polygon ticker detail response.

    Returns:
        Dict with company screening fields.
    """
    return {
        "symbol": symbol,
        "name": result.get("name"),
        "type": result.get("type"),
        "primary_exchange": result.get("primary_exchange"),
        "sic_code": result.get("sic_code"),
        "sic_description": result.get("sic_description"),
        "description": result.get("description"),
        "total_employees": result.get("total_employees"),
        "market_cap": result.get("market_cap"),
        "list_date": result.get("list_date"),
        "locale": result.get("locale"),
        "found": True,
        "error": None,
    }


async def _fetch_row(client: AsyncReferenceClient, symbol: str) -> Dict[str, Any]:
    """Fetch Polygon ticker details for one symbol with rate limiting.

    Args:
        client: Authenticated Polygon reference API client.
        symbol: Ticker symbol to fetch.

    Returns:
        Flattened screening row; found=False if Polygon returned NOT_FOUND.
    """
    async with _REQUEST_SEMAPHORE:
        response = await client.get_ticker_details(symbol)
    if response.get("status") == "NOT_FOUND":
        return {"symbol": symbol, "found": False, "error": None}
    return _extract_row(symbol, response.get("results", {}))


def _row_from_exception(symbol: str, exc: Exception) -> Dict[str, Any]:
    """Build a placeholder row for a symbol whose fetch raised an exception.

    Args:
        symbol: The ticker symbol that failed.
        exc: The exception that was raised.

    Returns:
        Minimal row dict with found=False and the error message.
    """
    return {"symbol": symbol, "found": False, "error": type(exc).__name__}


async def _load_all_symbols() -> List[str]:
    """Load every symbol from the Company table regardless of TickerType.enabled.

    Returns:
        List of all ticker symbols in the database.
    """
    df = await _cmp.db.execute_query(
        "SELECT symbol FROM Company", return_type="DataFrame"
    )
    return df["symbol"].tolist()


@plugin()
async def screen_companies(sort_by: str = "type") -> str:
    """Fetch company details from Polygon for every company in the database.

    Retrieves fields that Polygon returns but the database does not store:
    exchange, SIC industry description, company description, employee count,
    market cap, and IPO date. Useful for identifying which companies or ticker
    types to disable.

    All companies are included regardless of TickerType.enabled so that
    disabled types remain visible for review. Rows with found=False are
    companies Polygon no longer recognises — also candidates for removal.
    Rows with a non-null error column encountered a transient fetch failure
    and can be re-screened individually.

    Results are written to company_screen.csv in the project root.

    Args:
        sort_by: Column to sort results by ascending. Useful values:
            ``type`` groups ETFs/warrants/rights together;
            ``primary_exchange`` surfaces OTC listings;
            ``total_employees`` puts shells and micro-entities first;
            ``market_cap`` puts the smallest companies first.
            Default: ``type``.

    Returns:
        Path to the written CSV file.
    """
    symbols = await _load_all_symbols()

    async with AsyncReferenceClient(config["polygon_io"], True) as client:
        tasks = [asyncio.create_task(_fetch_row(client, sym)) for sym in symbols]
        results: List[Union[Dict, Exception]] = await asyncio.gather(
            *tasks, return_exceptions=True
        )

    rows = [
        _row_from_exception(sym, res) if isinstance(res, Exception) else res
        for sym, res in zip(symbols, results)
    ]
    df = pd.DataFrame(rows)
    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=True, na_position="first")
    df.to_csv(_OUTPUT_CSV, index=False)
    return str(_OUTPUT_CSV)
