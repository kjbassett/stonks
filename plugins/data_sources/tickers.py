import asyncio

from data_access.dao_manager import dao_manager
from polygon.reference_apis.reference_api import AsyncReferenceClient
from utils.project_utilities import get_key

from ..decorator import plugin

cpy = dao_manager.get_dao("Company")


def convert_result(result):
    result = result["results"]
    result = {
        "name": result["name"],
        "symbol": result["ticker"],
        "industry_id": int(result["sic_code"]),
    }
    return result


async def handle_result(result):
    result = convert_result(result)
    await cpy.insert(result, on_conflict="UPDATE")


async def _get_ticker_details(companies):
    async with AsyncReferenceClient(get_key("polygon_io"), True) as client:
        companies = [c.strip() for c in companies[0].split(",")]
        tasks = []
        for company in companies:
            tasks.append(asyncio.create_task(client.get_ticker_details(company)))
        for task in tasks:
            result = await task
            await handle_result(result)


@plugin()
async def get_ticker_details(companies: str = "all", skip_existing=True):
    companies = [c.strip() for c in companies[0].split(",")]

    # if skip_existing is True, only get companies that aren't in current_companies dataframe
    if skip_existing:
        current_companies = await cpy.get(symbol=companies)
        # drop rows with any null values from current_companies dataframe
        current_companies = current_companies.dropna()
        current_companies = current_companies["symbol"].tolist()
        companies = [c for c in companies if c not in current_companies]

    await _get_ticker_details(companies)
