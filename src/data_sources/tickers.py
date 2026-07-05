import asyncio

import pandas as pd
from async_lru import alru_cache
from icecream import ic
from massive import RESTClient
from massive.exceptions import BadResponse
from src.data_access.dao_manager import dao_manager
from src.utils.project_utilities import call_limiter, make_rest_client
from webrock.decorator import plugin

_UPDATE_COLS = ["name", "industry_id", "ticker_type_id", "primary_exchange"]


def convert_result(result) -> dict:
    """Flatten a TickerDetails object into a Company row dict.

    Args:
        result: TickerDetails object from the Massive API.

    Returns:
        Dict suitable for upserting into the Company table.
    """
    sic_code = int(result.sic_code) if result.sic_code is not None else None
    return {
        "name": result.name,
        "symbol": result.ticker,
        "industry_id": sic_code,
        "ticker_type_id": result.type,
        "primary_exchange": result.primary_exchange,
    }


async def fetch_and_update(client: RESTClient, row):
    """Fetch ticker details for one company and upsert into Company table.

    Deletes the company row if Massive reports it as not found.

    Args:
        client: Massive REST client.
        row: Company row dict with at least ``id`` and ``symbol`` keys.
    """
    cmp = dao_manager.get_dao("Company")
    async with call_limiter:
        print("Starting " + row["symbol"])
        try:
            result = await asyncio.to_thread(client.get_ticker_details, row["symbol"])
        except BadResponse as e:
            if "NOT_FOUND" in str(e):
                await cmp.delete(row["id"])
            else:
                ic(e)
            print("Finished " + row["symbol"])
            return
        await cmp.insert(
            convert_result(result), on_conflict="UPDATE", update_cols=_UPDATE_COLS
        )
        print("Finished " + row["symbol"])


@plugin()
async def update_companies(symbols: str = ""):
    """Refresh Company rows with latest data from the Massive reference API.

    Only processes companies that have at least one null column.

    Args:
        symbols: Comma-separated ticker to limit the update, or empty for all.
    """
    cmp = dao_manager.get_dao("Company")
    if symbols:
        companies = await cmp.get(symbol=symbols)
    else:
        companies = await cmp.get()
    companies = companies[companies.isnull().sum(axis=1) > 0]
    client = make_rest_client(32)
    tasks = []
    for _, row in companies.iterrows():
        tasks.append(asyncio.create_task(fetch_and_update(client, row)))

    await asyncio.gather(*tasks)


@alru_cache(maxsize=500)
async def get_or_create_company(
    symbol: str = None,
    name: str = None,
    industry_id: str = None,
    ticker_type_id: str = None,
):
    """Return the database ID for a company, creating it if necessary.

    Args:
        symbol: Ticker symbol.
        name: Company name (used if symbol is not provided).
        industry_id: SIC code.
        ticker_type_id: Ticker type string.

    Returns:
        Integer company ID.

    Raises:
        ValueError: If neither symbol nor name is provided, or if the company
            cannot be found and no symbol is available to create one.
    """
    cpy = dao_manager.get_dao("Company")
    if not symbol and not name:
        raise ValueError("Please provide either a symbol or a name.")
    company = None
    if symbol:
        company = await cpy.get(symbol=symbol)
    elif name:
        company = await cpy.get(name=name)

    if (isinstance(company, pd.DataFrame) and company.empty) or company is None:
        if symbol:
            await cpy.insert(
                {
                    "symbol": symbol,
                    "name": name,
                    "industry_id": industry_id,
                    "ticker_type_id": ticker_type_id,
                }
            )
            company = await cpy.get(symbol=symbol)
        else:
            raise ValueError(
                "No company found, and no symbol provided to create new company."
            )

    return int(company.loc[0, "id"])
