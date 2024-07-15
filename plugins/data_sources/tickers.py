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


@plugin(companies={"ui_element": "textbox", "default": "all"})
async def get_ticker_details(companies):
    async with AsyncReferenceClient(get_key("polygon_io"), True) as client:
        companies = [c.strip() for c in companies[0].split(",")]
        tasks = []
        for company in companies:
            tasks.append(asyncio.create_task(client.get_ticker_details(company)))
        for task in tasks:
            result = await task
            await handle_result(result)
