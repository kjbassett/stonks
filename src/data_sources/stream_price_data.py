import asyncio
import logging
from typing import List, Optional

from massive import WebSocketClient
from massive.websocket.models import EquityAgg, Feed, Market

from src.data_access.Company import Company
from src.data_sources.watchlist import get_watchlist_symbols
from src.utils.project_utilities import config
from webrock.decorator import plugin

_log = logging.getLogger("data_sources.stream")

RECONNECT_DELAY_S = 5
WATCHLIST_POLL_INTERVAL_S = 1800  # re-check watchlist every 30 minutes
_BAR_LOG_INTERVAL = 100  # log a summary every N bars received


@plugin(symbols={"ui_element": "textbox", "default": "watchlist"})
async def stream_price_data(db, symbols: str = "watchlist") -> None:
    """Stream Massive minute-bar data and persist to TradingData.

    Reconnects automatically on error.  Every ``WATCHLIST_POLL_INTERVAL_S``
    seconds the stream restarts to pick up watchlist changes.

    Args:
        db: AsyncDatabase instance injected by webrock.
        symbols: Comma-separated tickers, or ``"watchlist"`` to load from DB.
    """
    api_key = config["polygon_io"]
    while True:
        stream_client: Optional[WebSocketClient] = None
        try:
            tickers = await _resolve_symbols(db, symbols)
            _log.info("Streaming %d symbols.", len(tickers))
            subs = [f"AM.{t}" for t in tickers]
            stream_client = WebSocketClient(
                api_key=api_key,
                feed=Feed.RealTime,
                market=Market.Stocks,
                subscriptions=subs,
            )
            await asyncio.wait_for(
                stream_client.connect(processor=_make_bar_handler(db)),
                timeout=WATCHLIST_POLL_INTERVAL_S,
            )
        except asyncio.TimeoutError:
            _log.info("Watchlist refresh interval reached — reconnecting.")
        except asyncio.CancelledError:
            raise
        except Exception:
            _log.exception("Stream error, reconnecting in %ds.", RECONNECT_DELAY_S)
            await asyncio.sleep(RECONNECT_DELAY_S)
        finally:
            await _safe_close(stream_client)


async def _resolve_symbols(db, symbols_param: str) -> List[str]:
    """Return the list of tickers to subscribe to.

    Args:
        db: AsyncDatabase instance used to look up the watchlist.
        symbols_param: ``"watchlist"`` or a comma-separated ticker string.

    Returns:
        List of uppercase ticker symbols.
    """
    if symbols_param.strip().lower() == "watchlist":
        return await get_watchlist_symbols()
    return [s.strip().upper() for s in symbols_param.split(",") if s.strip()]


def _make_bar_handler(db):
    """Return an async handler that stores Massive minute bars to TradingData.

    Args:
        db: AsyncDatabase instance for DB writes.

    Returns:
        Async handler coroutine factory.
    """
    bars_received = 0

    async def _handler(msgs: list) -> None:
        nonlocal bars_received
        for bar in msgs:
            if not isinstance(bar, EquityAgg):
                continue
            bars_received += 1
            if bars_received % _BAR_LOG_INTERVAL == 0:
                _log.info("Received %d bars (latest: %s @ %.2f).", bars_received, bar.symbol, bar.close)
            cid = await Company(db).get_or_create_company(bar.symbol)
            await db.insert(
                "TradingData",
                {
                    "company_id": cid,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "vw_average": bar.vwap,
                    "volume": bar.volume,
                    "timestamp": bar.start_timestamp // 1000,
                },
            )

    return _handler


async def _safe_close(stream_client: Optional[WebSocketClient]) -> None:
    """Close the stream client, ignoring errors.

    Args:
        stream_client: Client to close, or None (no-op).
    """
    if stream_client is None:
        return
    try:
        await stream_client.close()
    except Exception:
        pass
