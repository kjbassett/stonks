import asyncio
import datetime
import logging
from typing import Optional

from src.trading.trading_engine import TradingEngine
from src.utils.market_calendar import is_currently_open

from ezmt.organism import Organism
from webrock.decorator import plugin

_log = logging.getLogger("trading.auto")


@plugin()
async def run_auto_trading(
    model_name: str,
    model_version: str,
    paper_trading: bool = True,
    adapt_policy: bool = False,
    rebalance_interval_hours: float = 1.0,
    poll_interval_seconds: float = 3600.0,
    start_datetime: Optional[datetime.datetime] = None,
    market_hours_only: bool = True,
):
    """
    Run the trading engine in a continuous production loop.

    Parameters
    ----------
    model_name / model_version : str
        Identifies the trained organism to load.
    paper_trading : bool
        True = PaperBroker (safe default). False = live SchwabBroker.
    adapt_policy : bool
        Whether to let the policy keep adapting on live returns. False = use
        the policy exactly as trained.
    rebalance_interval_hours : float
        Passed to TradingEngine; controls how often positions are rebalanced.
    poll_interval_seconds : float
        How often to fetch fresh predictions and step the engine. The schedule
        is anchored to start_datetime so it never drifts regardless of how
        long each cycle takes.
    start_datetime : datetime, optional
        When to execute the first tick. Subsequent ticks are spaced exactly
        poll_interval_seconds apart from this anchor. If None, starts
        immediately. Example: next whole hour + 5 minutes to let data settle.
    market_hours_only : bool
        If True (default), skip ticks when the NYSE is not currently open.
        Set False to allow trading outside market hours (useful for paper
        trading or testing on weekends).
    """
    model = Organism.load(model_name, model_version)
    policy = model.state["policy"]

    engine = TradingEngine(
        policy=policy,
        paper_trading=paper_trading,
        rebalance_interval_hours=rebalance_interval_hours,
    )
    await engine.executor._restore_intraday_state()

    _log.info(
        f"Auto trading started: model={model_name}/{model_version}, "
        f"paper={paper_trading}, adapt_policy={adapt_policy}, "
        f"market_hours_only={market_hours_only}"
    )

    interval = datetime.timedelta(seconds=poll_interval_seconds)
    next_tick = start_datetime or datetime.datetime.now()

    while True:
        # Sleep until the next scheduled tick.
        # Using an absolute target prevents drift: a slow cycle doesn't push
        # later ticks further out — the gap shrinks to catch back up.
        wait = (next_tick - datetime.datetime.now()).total_seconds()
        if wait > 0:
            await asyncio.sleep(wait)

        # Advance the schedule before executing so the next tick is always
        # anchored to the original cadence, not to when this cycle finishes.
        next_tick += interval

        if market_hours_only and not is_currently_open():
            _log.info("Market not currently open, skipping tick.")
            continue

        try:
            # model.run with mode="inference" gets the latest data from the database
            recommendations = await model.run(mode="inference", result_name="recommendations")
            await engine.step(recommendations, adapt_policy=adapt_policy)
        except Exception:
            _log.exception("Error during trading cycle, will retry next interval.")
