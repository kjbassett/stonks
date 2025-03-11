import asyncio

from icecream import ic
from data_access.dao_manager import dao_manager
from polygon.reference_apis.reference_api import AsyncReferenceClient
from utils.project_utilities import get_key, call_limiter

from ..decorator import plugin


def convert_result(result):
    result = result["results"]
    sic_code = int(result["sic_code"]) if "sic_code" in result else None
    result = {
        "name": result["name"],
        "symbol": result["ticker"],
        "industry_id": sic_code,
    }
    return result


async def handle_result(row, result):
    cpy = dao_manager.get_dao("Company")
    if result["status"] == "NOT_FOUND":
        await cpy.delete(row["id"])
    elif result["status"] != "OK":
        ic(result)
    else:
        result = convert_result(result)
        await cpy.insert(result, on_conflict="UPDATE")



async def fetch_and_update(client, row):
    async with call_limiter:
        print('Starting ' + row['symbol'])
        result = await client.get_ticker_details(row['symbol'])
        await handle_result(row, result)
        print('Finished ' + row['symbol'])


@plugin()
async def update_companies(symbols: str = "all"):
    companies = await get_companies(symbols)
    # filter out companies with no nans in any column
    companies = companies[companies.isnull().sum(axis=1) > 0]
    async with AsyncReferenceClient(get_key("polygon_io"), True) as client:
        tasks = []
        for _, row in companies.iterrows():
            tasks.append(asyncio.create_task(fetch_and_update(client, row)))

        await asyncio.gather(*tasks)


@plugin()
async def get_companies(symbols: str = "all"):
    cpy = dao_manager.get_dao("Company")
    if not symbols or symbols == "all":
        return await cpy.get_all()
    elif isinstance(symbols, str):
        symbols = [c.strip() for c in symbols[0].split(",")]
        return await cpy.get(symbol=symbols)
