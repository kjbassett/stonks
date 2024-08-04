import asyncio

import pandas as pd
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
    return result


async def _get_ticker_details(companies: list):
    async with AsyncReferenceClient(get_key("polygon_io"), True) as client:
        tasks = []
        for company in companies:
            tasks.append(asyncio.create_task(client.get_ticker_details(company)))
        for task in tasks:
            result = await task
            await handle_result(result)


@plugin()
async def get_ticker_details(companies: str = "all"):
    if not companies or companies == "all":
        return await cpy.get_all()
    companies = [c.strip() for c in companies[0].split(",")]

    # only get companies that aren't already in db
    current_companies = await cpy.get(symbol=companies)
    # drop rows with any null values from current_companies dataframe
    current_companies = current_companies.dropna()
    companies = [c for c in companies if c not in current_companies["symbol"].tolist()]

    await _get_ticker_details(companies)
    companies = await cpy.get(symbol=companies)
    companies = pd.concat([current_companies, companies])
    return companies
