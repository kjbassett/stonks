import asyncio

import polygon
from data_access.Company import Company
from utils.project_utilities import get_key

from ..decorator import plugin


# Async function for WebSocket client
@plugin(companies={"ui_element": "textbox", "default": "all"})
async def main(db, companies: list | None = None):
    # incoming data handler
    async def process_and_store_data(data):
        cid = await Company(db).get_or_create_company(data["sym"])
        data = [
            {
                "company_id": cid,
                "open": d["o"],
                "high": d["h"],
                "low": d["l"],
                "close": d["c"],
                "vw_average": d["vw"],
                "volume": d["v"],
                "timestamp": d["s"] // 1000,
            }
            for d in data
        ]
        await db.insert("TradingData", data)

    api_key = get_key("polygon_io")
    stream_client = polygon.AsyncStreamClient(api_key, "stocks")

    try:
        await stream_client.subscribe_stock_minute_aggregates(
            companies, handler_function=process_and_store_data
        )
    except asyncio.CancelledError:
        pass
    finally:
        await stream_client.close_stream()
