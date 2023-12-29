import asyncio
from datetime import datetime

from icecream import ic
from polygon import ReferenceClient

from project_utilities import get_key
from .helpers.missing_data import fill_gaps


async def main(db, companies=None):
    try:
        if not companies:
            companies = await db("SELECT * FROM Companies;", return_type="DataFrame")

        async with ReferenceClient(get_key("polygon_io"), True) as api:

            async def get_data(symbol, start, end):
                news_items = await api.get_ticker_news(
                    symbol,
                    published_utc_gte=start * 1000,
                    published_utc_lte=end * 1000,
                    merge_all_pages=True,
                )

                ic(news_items["status"])
                if news_items["status"] == "OK":
                    ic(news_items["results"][0]["published_utc"])
                if "results" not in news_items:
                    return

                results = [
                    {
                        "id": item["id"],
                        "source": item["publisher"]["name"],
                        "timestamp": int(
                            datetime.fromisoformat(
                                item["published_utc"].rstrip("Z")
                            ).timestamp()
                        ),
                        "title": item["title"],
                        "body": item.get("description", ""),
                    }
                    for item in news_items["results"]
                ]
                return results

            await fill_gaps(db, "News", get_data, companies, min_gap_size=3600)
    except asyncio.CancelledError:
        return
