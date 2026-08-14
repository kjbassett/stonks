import logging

import pandas as pd

from src.utils.project_utilities import config
from ezmt.organism import Organism
from src.data_access.dao_manager import dao_manager
from src.utils.email import send_email
from webrock.decorator import plugin

_log = logging.getLogger("prediction.predict")

WATCHLIST_KEYWORD = "watchlist"


@plugin()
async def predict_latest_data(
    name: str,
    version: str = "latest",
    symbols: str = "",
    log_states: bool = False,
    send_results: bool = False,
    recipients: str = None,
):
    """Run model inference and save predictions to Prediction.

    Args:
        name: Organism name to load.
        version: Organism version string (``"latest"`` resolves automatically).
        symbols: Comma-separated tickers, ``"watchlist"`` for watchlist+held, or
            empty to run without a symbol filter.
        log_states: Whether to log intermediate model states.
        send_results: When True, email high-confidence predictions to ``recipients``.
        recipients: Comma-separated email addresses; required when send_results is True.
    """
    resolved = await _resolve_symbols(symbols)
    if resolved:
        from src.data_sources.watchlist import set_active_symbols
        set_active_symbols(resolved)
    try:
        model = Organism.load(name, version, directory=config['organism_folder'])
        # model.folder ends in the *resolved* version — version itself stays "latest"
        # when that was passed in, so resolve it here rather than tagging predictions
        # with the literal string "latest".
        resolved_version = model.folder.rsplit("/", 1)[-1]
        predictions = await model.run(log_states=log_states, result_name="recommendations")
    finally:
        if resolved:
            from src.data_sources.watchlist import set_active_symbols
            set_active_symbols(None)

    model_id = await dao_manager.get_dao("Model").get_id(name, resolved_version)
    if model_id is None:
        raise ValueError(
            f"No Model row found for organism '{name}' version '{resolved_version}'. "
            "It must be trained/logged via run_genetic_algorithm or "
            "run_short_genetic_algorithm before predictions can be linked to it."
        )
    await dao_manager.get_dao("Prediction").save(predictions, model_id)
    _log.info("Inference complete: %d recommendations saved.", len(predictions))

    if send_results:
        filtered = predictions[
            predictions["prediction"] / (predictions["variance"].pow(0.5) + 1e-8) > 2.5
        ]
        if not recipients:
            raise ValueError("if send_results is truthy, recipients must have a value")
        send_email("Stock Recommendations", prepare_message(filtered), recipients)


async def _resolve_symbols(symbols: str) -> list:
    """Return a list of ticker symbols to filter model input, or empty list for no filter.

    Args:
        symbols: ``"watchlist"`` to load watchlist+held tickers, a comma-separated
            symbol string to use directly, or empty string for no filter.

    Returns:
        List of uppercase ticker strings, or empty list.
    """
    if symbols.strip().lower() == WATCHLIST_KEYWORD:
        from src.data_sources.watchlist import get_watchlist_and_held_symbols
        return await get_watchlist_and_held_symbols()
    if symbols.strip():
        return [s.strip().upper() for s in symbols.split(",") if s.strip()]
    return []


def prepare_message(df: pd.DataFrame) -> str:
    """Format a predictions DataFrame into a human-readable email body.

    Args:
        df: Predictions DataFrame with ``symbol`` and ``timestamp`` columns.

    Returns:
        Newline-separated string of ``SYMBOL, YYYY-MM-DD HH:MM`` entries.
    """
    dt_str_col = pd.to_datetime(df["timestamp"], unit="s").dt.strftime(", %Y-%m-%d %H:%M")
    return df["symbol"].str.cat(dt_str_col).str.cat(sep="\n")
