from polygon import RESTClient
from helpers.missing_data import fill_gaps
from datetime import datetime
from useful_funcs import get_key
import asyncio


async def main(db, companies=None):
    if not companies:
        companies = await db('SELECT * FROM Companies;', return_type='DataFrame')

    api = RESTClient(get_key('polygon_io'))

    def get_data(symbol, start, end):
        return [{
            "id": new.id,
            "source": new.publisher,
            "timestamp": int(datetime.fromisoformat(new.published_utc.rstrip("Z")).timestamp()),
            "title": new.title,
            "body": new.description,
        } for new in api.list_ticker_news(symbol,
                                          published_utc_gte=start * 1000,
                                          published_utc_lte=end * 1000,
                                          limit=1000)]

    try:
        await fill_gaps(db, 'TradingData', get_data, companies, min_gap_size=3600)
    except asyncio.CancelledError:
        return
