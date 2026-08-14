import logging

import pandas as pd
from ezmt.organism import Organism
from webrock.decorator import plugin

from src.data_access.dao_manager import dao_manager
from src.trading.brokers.paper_broker import PaperBroker
from src.trading.brokers.schwab_broker import SchwabBroker
from src.trading.trading_engine import TradingEngine
from src.utils.project_utilities import config

_log = logging.getLogger("trading.rebalance")

_engines: dict = {}
_organisms: dict = {}


@plugin(
    model_name={"ui_element": "textbox"},
    model_version={"ui_element": "textbox", "default": "latest"},
    paper_trading={"ui_element": "checkbox", "default": True},
    adapt_policy={"ui_element": "checkbox", "default": False},
)
async def run_rebalance(
    model_name: str,
    model_version: str = "latest",
    paper_trading: bool = True,
    adapt_policy: bool = False,
) -> None:
    """Execute trades from the latest Prediction rows for the given organism.

    Reads the most recent predictions saved by ``predict_latest_data`` and steps
    the TradingEngine. The engine is cached by model_name so PDT state and policy
    survive across hourly webrock invocations.

    Args:
        model_name: Organism name used to look up stored predictions.
        model_version: Organism version used to look up stored predictions.
        paper_trading: When True, paper-trades instead of using real capital.
        adapt_policy: When True, let the policy update on live returns.
    """
    model_id = await dao_manager.get_dao("Model").get_id(model_name, model_version)
    recommendations = (
        await dao_manager.get_dao("Prediction").load_latest(model_id)
        if model_id is not None
        else None
    )
    if recommendations is None:
        _log.warning(
            "No predictions found for %s/%s — skipping rebalance.",
            model_name,
            model_version,
        )
        return

    n_nonzero = int((recommendations["portfolio_weight"].abs() > 1e-6).sum())
    _log.info(
        "Loaded %d prediction(s) for %s/%s — %d with nonzero target weight.",
        len(recommendations), model_name, model_version, n_nonzero,
    )

    engine = await _get_or_create_engine(model_name, model_version, paper_trading)
    await engine.step(recommendations, adapt_policy=adapt_policy)
    _log.info("Rebalance complete.")


async def _get_or_create_engine(
    model_name: str, model_version: str, paper_trading: bool
) -> TradingEngine:
    """Return a cached TradingEngine, creating it on first call.

    Keyed by model_name so PDT state and policy survive across hourly invocations.

    Args:
        model_name: Cache key and organism name.
        model_version: Used only on first creation.
        paper_trading: When True, creates a PaperBroker; else connects Schwab live.

    Returns:
        Cached or freshly created TradingEngine.
    """
    if model_name not in _engines:
        broker = _make_paper_broker() if paper_trading else await SchwabBroker.from_auth(dry_run=False)
        model = _load_organism(model_name, model_version)
        policy = model.knowledge.get("policy")
        _engines[model_name] = TradingEngine(broker=broker, policy=policy)
    return _engines[model_name]


def _make_paper_broker() -> PaperBroker:
    """Create a PaperBroker configured from trading_rules in config.json.

    Returns:
        PaperBroker with empty price history and configured cash/fees.
    """
    rules = config.get("trading_rules", {})
    empty_history = pd.DataFrame(columns=["symbol", "timestamp", "close"])
    return PaperBroker(
        price_history=empty_history,
        starting_cash=float(rules.get("starting_cash", 100_000)),
        flat_fee=float(rules.get("flat_fee", 0.0)),
        percent_fee=float(rules.get("percent_fee", 0.0)),
    )


def _load_organism(model_name: str, model_version: str) -> Organism:
    """Return a cached Organism, loading from disk on first access.

    Args:
        model_name: Name of the organism.
        model_version: Version string.

    Returns:
        Loaded Organism instance.
    """
    key = (model_name, model_version)
    if key not in _organisms:
        _organisms[key] = Organism.load(model_name, model_version, directory=config['organism_folder'])
    return _organisms[key]
