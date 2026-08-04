import asyncio
import logging
from datetime import datetime, timezone

_log = logging.getLogger("data_sources.news")

from massive import RESTClient
from src.data_access.dao_manager import dao_manager
from src.data_sources.missing_data import fill_gaps
from src.data_sources.tickers import get_or_create_company
from src.utils.project_utilities import config, make_rest_client
from webrock.decorator import plugin

cmp = dao_manager.get_dao("Company")
news = dao_manager.get_dao("News")
nc_link = dao_manager.get_dao("NewsCompanyLink")


async def _get_data(client: RESTClient, symbol: str, start: int, end: int):
    """Fetch news articles for a symbol over a time range.

    Args:
        client: Massive REST client.
        symbol: Ticker symbol.
        start: Unix timestamp (seconds) for range start.
        end: Unix timestamp (seconds) for range end.

    Returns:
        List of TickerNews objects.
    """
    from_str = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
    to_str = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()
    return await asyncio.to_thread(
        lambda: list(
            client.list_ticker_news(
                symbol,
                published_utc_gte=from_str,
                published_utc_lte=to_str,
                limit=1000,
            )
        )
    )


async def save_data(company_id: int, data: list) -> int:
    """Persist a list of TickerNews articles to News and NewsCompanyLink.

    Args:
        company_id: Database ID of the primary company (unused directly; links built per ticker).
        data: List of TickerNews objects from the Massive API.

    Returns:
        Number of News rows inserted.
    """
    news_data = []
    n_c_link_data = []
    for d in data:
        news_data.append(
            {
                "id": d.id,
                "source": d.publisher.name if d.publisher else None,
                "timestamp": int(
                    datetime.fromisoformat(
                        d.published_utc.replace("Z", "+00:00")
                    ).timestamp()
                ),
                "title": d.title,
                "body": d.description or "",
            }
        )
        for ticker in (d.tickers or []):
            link_data = {
                "company_id": await get_or_create_company(ticker),
                "news_id": d.id,
                "sentiment": None,
                "sentiment_reasoning": None,
            }
            for insight in (d.insights or []):
                if insight.ticker == ticker:
                    link_data["sentiment"] = insight.sentiment
                    link_data["sentiment_reasoning"] = insight.sentiment_reasoning
                    break
            n_c_link_data.append(link_data)

    n = await news.insert(news_data)
    await nc_link.insert(n_c_link_data)
    if n > 0:
        _log.info("%d rows inserted into News", n)
    return n


@plugin()
async def fill_missing(companies: str = ""):
    """Fill gaps in News by fetching missing articles from Massive.

    Args:
        companies: Comma-separated ticker symbols, ``"watchlist"``, or empty for all.
    """
    try:
        client = make_rest_client(32)
        await fill_gaps(
            client,
            "News",
            news.get_timestamps_by_company,
            _get_data,
            save_data,
            companies,
            min_gap_size=1800,
            max_gap_size=86400 * 30,
        )
    except asyncio.CancelledError:
        return


@plugin()
async def query_api(symbol: str, start: int, end: int):
    """Fetch news articles directly from the Massive API for inspection.

    Args:
        symbol: Ticker symbol, or ``"all"``/``"*"`` for no filter.
        start: Unix timestamp (seconds) for range start.
        end: Unix timestamp (seconds) for range end.

    Returns:
        List of TickerNews objects.
    """
    if symbol in ("all", "*"):
        symbol = ""
    client = make_rest_client()
    return await _get_data(client, symbol, start, end)
