import logging
import time
from typing import List, Optional

from src.data_access.dao_manager import dao_manager
from src.utils.project_utilities import config
from webrock.decorator import plugin

_log = logging.getLogger("data_sources.watchlist")

_active_symbols: Optional[List[str]] = None

_LOOKBACK_DAYS_DEFAULT = 30
_MAX_SYMBOLS_DEFAULT = 500
_MIN_AVG_VOLUME_DEFAULT = 100_000


@plugin()
async def compute_watchlist() -> List[str]:
    """Rank enabled companies by recent average volume and refresh the Watchlist table.

    Config keys (all under ``"watchlist"`` in config.json):
        lookback_days: Number of days of history to rank by. Default 30.
        max_symbols: Maximum symbols to keep. Default 500.
        min_avg_volume: Minimum average volume threshold. Default 100000.

    Returns:
        Selected ticker symbols ordered by descending average volume.
    """
    cfg = config.get("watchlist", {})
    lookback_days = int(cfg.get("lookback_days", _LOOKBACK_DAYS_DEFAULT))
    max_symbols = int(cfg.get("max_symbols", _MAX_SYMBOLS_DEFAULT))
    min_vol = float(cfg.get("min_avg_volume", _MIN_AVG_VOLUME_DEFAULT))
    since = int(time.time()) - lookback_days * 86_400

    rows = await _query_top_symbols(since, min_vol, max_symbols)
    watchlist_dao = dao_manager.get_dao("Watchlist")
    await watchlist_dao.save_symbols(
        [{"company_id": r[0], "avg_volume": r[2]} for r in rows]
    )
    symbols = [r[1] for r in rows]
    _log.info("Watchlist updated: %d symbols selected.", len(symbols))
    return symbols


async def get_watchlist_symbols() -> List[str]:
    """Return the current watchlist tickers from the database.

    Returns:
        List of ticker symbols ordered by descending average volume.
    """
    watchlist_dao = dao_manager.get_dao("Watchlist")
    return await watchlist_dao.get_symbols()


async def get_watchlist_and_held_symbols(broker=None) -> List[str]:
    """Return watchlist symbols unioned with any symbols that have an open position.

    Held symbols are always included so sell recommendations are generated even
    after a ticker drops off the watchlist.

    Args:
        broker: Broker instance with an async ``get_positions()`` method.
            If None, a ``SchwabBroker`` is created from stored credentials.

    Returns:
        Deduplicated list of ticker symbols, watchlist order preserved.
    """
    if broker is None:
        from src.trading.brokers.schwab_broker import SchwabBroker
        broker = await SchwabBroker.from_auth(dry_run=True)
    watchlist = await get_watchlist_symbols()
    positions = await broker.get_positions()
    held = [sym for sym, pos in positions.items() if pos.shares > 0]
    return list(dict.fromkeys(watchlist + held))


def set_active_symbols(symbols: Optional[List[str]]) -> None:
    """Set the global symbol filter used by ``load_data`` during inference.

    Args:
        symbols: Tickers to restrict DataCompiler queries to, or None to disable.
    """
    global _active_symbols
    _active_symbols = symbols


def get_active_symbols() -> Optional[List[str]]:
    """Return the currently active symbol filter.

    Returns:
        List of tickers, or None if no filter is active.
    """
    return _active_symbols


async def _query_top_symbols(
    since: int, min_avg_volume: float, max_symbols: int
) -> list:
    """Query TradingDataAggregation for the top companies by average volume.

    Args:
        since: Earliest Unix timestamp to include in the volume average.
        min_avg_volume: Minimum qualifying average volume.
        max_symbols: Cap on the number of results.

    Returns:
        List of (company_id, symbol, avg_volume) tuples.
    """
    return await dao_manager.db.execute_query(
        """
        SELECT c.id, c.symbol, AVG(a.avg_volume) AS avg_vol
        FROM TradingDataAggregation a
        JOIN Company c ON c.id = a.company_id
        LEFT JOIN Exchange e ON c.primary_exchange = e.market_id
        WHERE a.start >= ? AND c.enabled = 1 AND e.active IS NOT 0
        GROUP BY c.id
        HAVING avg_vol >= ?
        ORDER BY avg_vol DESC
        LIMIT ?
        """,
        (since, min_avg_volume, max_symbols),
        return_type="list",
    )
