import asyncio

import pandas as pd
from async_lru import alru_cache
from icecream import ic
from polygon.reference_apis.reference_api import AsyncReferenceClient
from src.data_access.dao_manager import dao_manager
from src.utils.project_utilities import config, call_limiter
from webrock.decorator import plugin

cmp = dao_manager.get_dao("Company")


def convert_result(result):
    result = result["results"]
    sic_code = int(result["sic_code"]) if "sic_code" in result else None
    result = {
        "name": result["name"],
        "symbol": result["ticker"],
        "industry_id": sic_code,
        "ticker_type_id": result["type"],
        "primary_exchange": result.get("primary_exchange"),
    }
    return result


async def handle_result(row, result):
    cpy = dao_manager.get_dao("Company")
    if result["status"] == "NOT_FOUND":
        # TODO this should be logged, and deleting from the database should be reviewed
        await cpy.delete(row["id"])
    elif result["status"] != "OK":
        ic(result)
    else:
        result = convert_result(result)
        await cpy.insert(result, on_conflict="UPDATE")


async def fetch_and_update(client, row):
    async with call_limiter:
        print("Starting " + row["symbol"])
        result = await client.get_ticker_details(row["symbol"])
        await handle_result(row, result)
        print("Finished " + row["symbol"])


@plugin()
async def update_companies(symbols: str = ""):
    if symbols:
        companies = await cmp.get(symbol=symbols)
    else:
        companies = await cmp.get()
    # filter out companies with no nans in any column
    companies = companies[companies.isnull().sum(axis=1) > 0]
    async with AsyncReferenceClient(config["polygon_io"], True) as client:
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
    cpy = dao_manager.get_dao("Company")
    if not symbol and not name:
        raise ValueError("Please provide either a symbol or a name.")
    company = None
    if symbol:
        company = await cpy.get(symbol=symbol)
    elif name:
        company = await cpy.get(name=name)

    # if not found, create a new company if symbol is provided
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
