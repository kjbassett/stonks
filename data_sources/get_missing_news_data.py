import asyncio
from datetime import datetime

from polygon import ReferenceClient

from database_utilities import get_or_create_company
from project_utilities import get_key
from .helpers.missing_data import fill_gaps


async def load_data(db, company_id, min_timestamp=0):
    """Load saved data for a symbol if it exists, otherwise return an empty dataframe."""

    query = f"""
    SELECT timestamp
    FROM News INNER JOIN NewsCompaniesLink
    ON News.id = NewsCompaniesLink.news_id
    WHERE NewsCompaniesLink.company_id =? AND News.timestamp >?;
    """

    data = await db(query, (company_id, min_timestamp), return_type="DataFrame")
    return data


async def get_data(client, symbol, start, end):
    news_items = await client.get_ticker_news(
        symbol,
        published_utc_gte=start * 1000,
        published_utc_lte=end * 1000,
        merge_all_pages=True,
    )

    if "results" in news_items:
        return news_items["results"]


async def save_data(db, data):
    # transform data to match db table
    news = []
    links = []
    for d in data:
        news.append(
            {
                "id": d["id"],
                "source": d["publisher"]["name"],
                "timestamp": int(
                    datetime.fromisoformat(d["published_utc"]).timestamp()
                ),
                "title": d["title"],
                "body": d.get("description", ""),
            }
        )
        for symbol in d["tickers"]:
            links.append(
                {
                    "company_id": (await get_or_create_company(db, symbol))[0],
                    "news_id": d["id"],
                }
            )
    # insert data and return new rows in News table
    n = await db.insert("News", news)
    await db.insert("NewsCompaniesLink", links)

    return n


async def main(db, companies=None):
    try:
        async with ReferenceClient(get_key("polygon_io"), True) as client:
            await fill_gaps(
                client,
                db,
                "News",
                load_data,
                get_data,
                save_data,
                companies,
                min_gap_size=3600,
            )
    except asyncio.CancelledError:
        return
