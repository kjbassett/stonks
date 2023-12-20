from useful_funcs import get_key
import asyncio
from polygon import RESTClient
from helpers.missing_data import fill_gaps


async def main(db, companies=None):
    if not companies:
        companies = await db('SELECT * FROM Companies;', return_type='DataFrame')

    api = RESTClient(get_key('polygon_io'))

    def get_data(symbol, start, end):
        return [{
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": agg.volume,
            "timestamp": agg.timestamp // 1000
        } for agg in api.get_aggs(symbol, 1, 'minute', start * 1000, end * 1000, limit=5000)]
    try:
        await fill_gaps(db, 'TradingData', get_data, companies, min_gap_size=1800)
    except asyncio.CancelledError:
        return
