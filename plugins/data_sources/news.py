import asyncio
from datetime import datetime

from data_access.dao_manager import dao_manager
from polygon import ReferenceClient
from utils.project_utilities import get_key

from .helpers.missing_data import fill_gaps
from ..decorator import plugin

cmp = dao_manager.get_dao("Company")
news = dao_manager.get_dao("News")
nc_link = dao_manager.get_dao("NewsCompanyLink")


async def _get_data(client, symbol, start, end):
    return await client.get_ticker_news(
        symbol,
        published_utc_gte=start * 1000,
        published_utc_lte=end * 1000,
        all_pages=True,
        merge_all_pages=True,
    )


async def save_data(company_id, data):
    # transform data to match db table
    news_data = []
    n_c_link_data = []
    for d in data:
        news_data.append(
            {
                "id": d["id"],
                "source": d["publisher"]["name"],
                "timestamp": int(
                    datetime.fromisoformat(d["published_utc"].replace('Z', '+00:00')).timestamp()
                ),
                "title": d["title"],
                "body": d.get("description", ""),
            }
        )
        for symbol in d["tickers"]:
            n_c_link_data.append(
                {
                    "company_id": await cmp.get_or_create_company(symbol),
                    "news_id": d["id"],
                }
            )
    # insert data and return new rows in News table
    n = await news.insert(news_data)
    await nc_link.insert(n_c_link_data)
    if n > 0:
        print(f"{n} rows inserted into News")
    return n


@plugin()
async def fill_missing(companies: str = "all"):
    print(companies)
    try:
        async with ReferenceClient(get_key("polygon_io"), True) as client:
            await fill_gaps(
                client,
                "News",
                news.get_timestamps_by_company,
                _get_data,
                save_data,
                companies,
                min_gap_size=3600,
                max_gap_size=86400 * 30
            )
    except asyncio.CancelledError:
        return


@plugin()
async def query_api(symbol: str, start: int, end: int):
    if symbol in ("all", "*"):
        symbol = ""
    async with ReferenceClient(get_key("polygon_io"), True) as client:
        return await _get_data(client, symbol, start, end)

