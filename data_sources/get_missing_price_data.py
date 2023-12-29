import asyncio
import datetime

from polygon import StocksClient

from project_utilities import get_key
from .helpers.missing_data import fill_gaps


async def main(db, companies=None):
    try:
        if not companies:
            companies = await db("SELECT * FROM Companies;", return_type="DataFrame")

        async with StocksClient(get_key("polygon_io"), True) as api:

            async def get_data(symbol, start, end):
                start = datetime.datetime.fromtimestamp(start, tz=datetime.timezone.utc)
                end = datetime.datetime.fromtimestamp(end, tz=datetime.timezone.utc)
                aggs = await api.get_aggregate_bars(
                    symbol, start, end, timespan="minute", full_range=True
                )

                if not aggs:
                    return

                # This might need to run in the executor if there are a lot of aggs
                cid = companies[companies["symbol"] == symbol]["id"][0]
                return [
                    {
                        "company_id": cid,
                        "open": agg["o"],
                        "high": agg["h"],
                        "low": agg["l"],
                        "close": agg["c"],
                        "vw_average": agg["vw"],
                        "volume": agg["v"],
                        "timestamp": agg["t"] // 1000,
                    }
                    for agg in aggs
                ]

            await fill_gaps(db, "TradingData", get_data, companies, min_gap_size=1800)
    except asyncio.CancelledError:
        return
