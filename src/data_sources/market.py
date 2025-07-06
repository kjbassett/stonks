import asyncio
import datetime

from polygon import StocksClient
from webrock.decorator import plugin

from src.data_access.dao_manager import dao_manager
from src.data_sources.missing_data import fill_gaps
from src.utils.project_utilities import get_key

td = dao_manager.get_dao("TradingData")
cp = dao_manager.get_dao("Company")


async def _get_data(client: StocksClient, symbol: str, start: int, end: int):
    # Put everything into utc
    start = datetime.datetime.fromtimestamp(start, tz=datetime.timezone.utc)
    end = datetime.datetime.fromtimestamp(end, tz=datetime.timezone.utc)
    aggs = await client.get_aggregate_bars(
        symbol, start, end, timespan="minute", full_range=True
    )
    return aggs


async def save_data(company_id, data):
    try:
        data = [
            {
                "company_id": company_id,
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
    except KeyError as e:
        print("MARKET DATA ERROR")
        print(e)
        print(data)
        return 1
    n = await td.insert(data)
    print(f"{n} rows inserted into TradingData")
    return n


@plugin()
async def fill_missing(companies: str = ""):
    try:
        async with StocksClient(get_key("polygon_io"), True) as client:
            await fill_gaps(
                client,
                "TradingData",
                td.get_timestamps_by_company,
                _get_data,
                save_data,
                companies,
                min_gap_size=1800,  # 30 minutes
                max_gap_size=86400 * 30,  # 30 days
                adjust_for_market_hours=True,
            )
    except asyncio.CancelledError:
        return


@plugin()
async def query_api(symbol: str, start: int, end: int):
    if symbol in ("all", "*"):
        symbol = ""
    async with StocksClient(get_key("polygon_io"), True) as client:
        return await _get_data(client, symbol, start, end)
