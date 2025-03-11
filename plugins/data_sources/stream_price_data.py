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

# test function for all template input types
@plugin(
    date={"ui_element": "date"},
    slider={"ui_element": "slider", "default": 50, "min": 0, "max": 100},
    color={"ui_element": "color", "default": "#000000"},
    datetime_local={"ui_element": "datetime_local", "default": "2021-01-01T00:00"},
    email={"ui_element": "email"},
    file={"ui_element": "file"},
    month={"ui_element": "month"},
    password={"ui_element": "password"}
)
async def test_ui_elements(text: str, check: bool, integer: int, date: str, slider: int, color: str, datetime_local: str, email: str, file: str, month: str, password: str):
    print(f"text: {text}")
    print(f"check: {check}")
    print(f"integer: {integer}")
    print(f"date: {date}")
    print(f"slider: {slider}")
    print(f"color: {color}")
    print(f"datetime_local: {datetime_local}")
    print(f"email: {email}")
    print(f"file: {file}")
    print(f"month: {month}")
    print(f"password: {password}")