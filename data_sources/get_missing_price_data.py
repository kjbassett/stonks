import asyncio
import datetime

from icecream import ic
from polygon import StocksClient

from database_utilities import get_or_create_company
from project_utilities import get_key
from .helpers.missing_data import fill_gaps


async def load_data(db, company_id, min_timestamp=0):
    """Load saved data for a symbol if it exists, otherwise return None."""
    query = f"SELECT timestamp FROM TradingData WHERE company_id = ? AND timestamp > ? ORDER BY timestamp ASC;"
    data = await db(query, (company_id, min_timestamp), return_type="DataFrame")
    return data


async def get_data(client, symbol, start, end):
    start = datetime.datetime.fromtimestamp(start, tz=datetime.timezone.utc)
    end = datetime.datetime.fromtimestamp(end, tz=datetime.timezone.utc)
    aggs = await client.get_aggregate_bars(
        symbol, start, end, timespan="minute", full_range=True
    )
    ic(aggs[0])
    return aggs

    # This might need to run in the executor if there are a lot of aggs


async def save_data(db, data):
    print("SAVING DATA")
    data = [
        {
            "company_id": (await get_or_create_company(db, d["ticker"]))[0],
            "open": d["o"],
            "high": d["h"],
            "low": d["l"],
            "close": d["c"],
            "vw_average": d["vw"],
            "volume": d["v"],
            "timestamp": d["t"] // 1000,
        }
        for d in data
    ]
    n = await db.insert("TradingData", data)
    print(f"{n} rows inserted into TradingData")
    return n


async def main(db, companies=None):
    try:
        async with StocksClient(get_key("polygon_io"), True) as client:
            await fill_gaps(
                client,
                db,
                "TradingData",
                load_data,
                get_data,
                save_data,
                companies,
                min_gap_size=1800,
            )
    except asyncio.CancelledError:
        return
