import datetime
import logging
import os
import re
import time
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch
from missforest import MissForest
from sklearn.preprocessing import OneHotEncoder
from src.data_access.dao_manager import dao_manager

_NEWS_ID_PAT = re.compile(r"^news\d+_id$")
_log = logging.getLogger("prediction.pipeline")


async def load_data(
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 24 * 60 * 60,
    include_target: bool = True,
    target_offset: int|str = "next_close",
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
    keep_latest_only: bool = False,
    embedding_model_name: str = "all-MiniLM-L6-v2",
    symbols: Optional[List] = None,
) -> tuple:
    """Load structured trading data and pre-computed news embeddings.

    Args:
        min_timestamp: Earliest row timestamp to include (0 = no limit).
        max_timestamp: Latest row timestamp to include (0 = no limit).
        max_window: Largest lag window for ratio features.
        num_windows: Number of lag windows to generate.
        num_news: Number of most-recent news articles to attach per row.
        news_history_threshold: Maximum news age in seconds.
        include_target: Whether to include the target column.
        target_offset: Target horizon ('next_close' or integer hours).
        include_close_ratio: Include close-price lag ratios.
        include_cv_close_ratio: Include CV-of-close lag ratios.
        include_avg_volume_ratio: Include average-volume lag ratios.
        include_cv_volume_ratio: Include CV-of-volume lag ratios.
        keep_latest_only: Keep only the most recent row per symbol.
        embedding_model_name: Sentence-transformer model for embedding lookup.

    Returns:
        Tuple of (structured_data, embedding_lookup, raw_price_data).
    """
    if min_timestamp < 0:
        min_timestamp = int(time.time()) + min_timestamp
    # Use actively set symbols filter when no explicit list was supplied.
    if symbols is None or not isinstance(symbols, list):
        from src.data_sources.watchlist import get_active_symbols
        symbols = get_active_symbols()
    structured_data_dao = dao_manager.get_dao("DataCompiler")
    structured_data = await structured_data_dao.get_data(
        "hour",
        min_timestamp,
        max_timestamp,
        max_window,
        num_windows,
        num_news,
        news_history_threshold,
        include_target,
        target_offset,
        include_close_ratio,
        include_cv_close_ratio,
        include_avg_volume_ratio,
        include_cv_volume_ratio,
        keep_latest_only,
        print_query=True,
        symbols=symbols,
    )
    if num_news > 0:
        news_id_cols = [c for c in structured_data.columns if _NEWS_ID_PAT.match(c)]
        all_ids = pd.unique(structured_data[news_id_cols].values.ravel("K"))
        all_ids = [i for i in all_ids if pd.notna(i)]
        emb_dao = dao_manager.get_dao("NewsEmbedding")
        embedding_lookup: Optional[Dict] = await emb_dao.get_embeddings(
            all_ids, embedding_model_name
        )
        if not embedding_lookup:
            raise ValueError(
                f"num_news={num_news} but no embeddings found in the database for "
                f"model '{embedding_model_name}'. Run the compute_news_embeddings() "
                f"plugin to populate the NewsEmbedding table before training."
            )
        n_found = len(embedding_lookup)
        n_requested = len(all_ids)
        if n_found < n_requested:
            _log.warning(
                f"Partial embedding coverage: {n_found}/{n_requested} news IDs have "
                f"embeddings. Missing IDs will use zero vectors."
            )
    else:
        embedding_lookup = None
    return structured_data, embedding_lookup, structured_data[["symbol", "timestamp", "close"]]


def load_short_data(file_name):
    t = time.time()
    _log.info("Loading data from %s", file_name)
    structured_data = pd.read_csv(file_name, index_col=None).reset_index(drop=True)
    _log.info("Data loaded in %ds", int(time.time() - t))
    return structured_data, None


def split_data(
    dataframe: pd.DataFrame, split: float = 0.8
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a dataframe into train and test sets by row order (no shuffle).

    Args:
        dataframe: The full dataset to split.
        split: Fraction of rows to use for training.

    Returns:
        A tuple of (train_df, test_df).
    """
    n_train = int(split * len(dataframe))
    return dataframe.iloc[:n_train].copy(), dataframe.iloc[n_train:].copy()


def filter_out_missing_data(
    structured_data, missing_data_threshold, ignore_cols=None, no_tolerance_cols=None
):
    ignore_cols = format_ignore_cols(ignore_cols)
    no_tolerance_cols = format_ignore_cols(no_tolerance_cols)

    # filter out rows if the percentage of missing values is above the threshold
    ignore_df = structured_data[ignore_cols]
    structured_data = structured_data[structured_data.columns.difference(ignore_cols)]
    n = len(structured_data)
    missing_data_ratio = structured_data.isnull().sum(axis=1) / structured_data.shape[1]
    filt = missing_data_ratio < missing_data_threshold
    structured_data = structured_data[filt]
    if not ignore_df.empty:
        ignore_df = ignore_df[filt]
        structured_data = pd.concat([structured_data, ignore_df], axis=1)

    # filter out no_tolerance columns if any value is null in them.
    # For example, filter out rows with a null target column
    if no_tolerance_cols:
        structured_data = structured_data[
            ~structured_data[no_tolerance_cols].isnull().any(axis=1)
        ]
    _log.info("Removed %d rows (missing data threshold)", n - len(structured_data))
    return structured_data


def _apply_clip(
    df: pd.DataFrame, column_limits: dict, ignore_cols: list
) -> pd.DataFrame:
    """Apply pre-computed clip limits to numeric columns of ``df``."""
    for col, limits in column_limits.items():
        if col in df.columns:
            df[col] = df[col].clip(lower=limits["lower"], upper=limits["upper"])
    return df


def clip_values(
    df: pd.DataFrame,
    test_df: Optional[pd.DataFrame] = None,
    ignore_cols: list = None,
    column_limits: dict = None,
) -> tuple:
    """Clip numeric columns to [p1, p99] bounds fitted on ``df``.

    Args:
        df: DataFrame to fit clip limits on and transform (training data).
        test_df: Optional test DataFrame to clip using the same limits.
            Fit is never performed on this data.
        ignore_cols: Columns to exclude from clipping.
        column_limits: Pre-computed limits. If None, fitted from ``df``.

    Returns:
        A 3-tuple ``(df, test_df, column_limits)`` where ``test_df`` may be None.
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    if column_limits is None:
        column_limits = {}
    for col in df.select_dtypes(include=[float, int]).columns.difference(ignore_cols):
        if col not in column_limits:
            column_limits[col] = {
                "lower": df[col].quantile(0.01),
                "upper": df[col].quantile(0.99),
            }
    df = _apply_clip(df, column_limits, ignore_cols)
    if test_df is not None:
        test_df = _apply_clip(test_df, column_limits, ignore_cols)
    return df, test_df, column_limits


def scale_data(
    dataframe,
    test_df=None,
    means=None,
    stds=None,
    ignore_cols=None,
    target_col="target",
    target_transform="asinh",
):
    """Standardize numeric columns of ``dataframe``, optionally applying the same
    transform to ``test_df`` using statistics fit only on ``dataframe``.

    Args:
        dataframe: The DataFrame to fit and transform (training data).
        test_df: Optional test DataFrame to transform using the same statistics.
            Fit is never performed on this data.
        means: Precomputed column means. If None, computed from ``dataframe``.
        stds: Precomputed column stds. If None, computed from ``dataframe``.
        ignore_cols: Columns to exclude from scaling.
        target_col: Name of the target column for optional transform.
        target_transform: Transform to apply to the target before scaling.

    Returns:
        A 4-tuple ``(dataframe, test_df, means, stds)`` where ``test_df`` may be None.
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    numeric_cols = dataframe.select_dtypes(include=[np.number]).columns.difference(
        ignore_cols
    )

    if target_transform == "asinh" and target_col in dataframe.columns:
        _log.debug("Applying asinh transform to target")
        dataframe[target_col] = np.arcsinh(dataframe[target_col])

    if means is None:
        means = dataframe[numeric_cols].mean().to_dict()
    if stds is None:
        stds = dataframe[numeric_cols].std().replace(0, 1e-8).to_dict()

    def _apply(df):
        cols = df.select_dtypes(include=[np.number]).columns.difference(ignore_cols)
        _means = {k: v for k, v in means.items() if k in cols}
        _stds = {k: v for k, v in stds.items() if k in cols}
        df.loc[:, cols] = (df[cols] - pd.Series(_means)) / pd.Series(_stds)
        return df

    dataframe = _apply(dataframe)

    if test_df is not None:
        if target_transform == "asinh" and target_col in test_df.columns:
            test_df[target_col] = np.arcsinh(test_df[target_col])
        test_df = _apply(test_df)

    return dataframe, test_df, means, stds


def unscale_data(
    predictions,
    means,
    stds,
    target_col="target",
    target_transform="asinh"
):
    mean, std = means["target"], stds["target"]  # scaling factors

    # store standardized values other colums for reference
    predictions["standardized_close"] = predictions["close"]
    predictions["standardized_prediction"] = predictions["prediction"]
    predictions["standardized_variance"] = predictions["variance"]

    # unscale target
    if "target" in predictions.columns:
        predictions["standardized_target"] = predictions["target"]
        predictions["target"] = predictions["target"] * std + mean

    # unscale prediction
    predictions["prediction"] = predictions["prediction"] * std + mean

    # unscale variance
    # scale factor is std. std dev scales linearly with scale factor.
    # So variance (std dev squared) scales with scale factor squared
    predictions["variance"] = predictions["variance"] * std**2

    # unscale close
    predictions["close"] = predictions["close"] * stds["close"] + means["close"]

    # --- inverse target transform ---
    if target_transform == "asinh":
        if target_col in predictions.columns:
            _log.debug("Applying inverse asinh transform to target")
            predictions[target_col] = np.sinh(predictions[target_col])
        mu_asinh = predictions["prediction"].copy()
        predictions["prediction"] = np.sinh(predictions["prediction"])
        predictions["variance"] = predictions["variance"] * np.cosh(mu_asinh) ** 2

    return predictions


def _apply_one_hot(
    dataframe: pd.DataFrame, encoder: OneHotEncoder, ignore_cols: list
) -> pd.DataFrame:
    """Apply a fitted encoder to ``dataframe`` and return the transformed DataFrame."""
    cols_to_encode = [
        c for c in dataframe.select_dtypes(include=["object"]).columns
        if c not in ignore_cols and not _NEWS_ID_PAT.match(c)
    ]
    encoded_data = encoder.transform(dataframe[cols_to_encode])
    encoded_df = pd.DataFrame(
        encoded_data.toarray(), columns=encoder.get_feature_names_out(cols_to_encode)
    )
    return pd.concat(
        [
            dataframe.drop(columns=cols_to_encode).reset_index(drop=True),
            encoded_df.reset_index(drop=True),
        ],
        axis=1,
    )


def one_hot_encode(
    dataframe: pd.DataFrame,
    test_df: pd.DataFrame | None = None,
    encoder: OneHotEncoder | None = None,
    ignore_cols: list = None,
    max_categories: int = None,
):
    """Fit and transform ``dataframe`` using one-hot encoding, optionally applying
    the same fitted encoder to ``test_df``.

    News ID columns (matching ``news{n}_id``) are always excluded — they are
    unique identifiers, not categories, and are consumed by HybridDataset.

    Args:
        dataframe: Training DataFrame to fit and transform.
        test_df: Optional test DataFrame to transform using the fitted encoder.
            Fit is never performed on this data.
        encoder: Pre-fitted OneHotEncoder. If None, a new one is fitted on ``dataframe``.
        ignore_cols: Columns to exclude from encoding.
        max_categories: Cap on categories per feature; rare ones become 'infrequent'.

    Returns:
        A 3-tuple ``(dataframe, test_df, encoder)`` where ``test_df`` may be None.
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    cols_to_encode = [
        c for c in dataframe.select_dtypes(include=["object"]).columns
        if c not in ignore_cols and not _NEWS_ID_PAT.match(c)
    ]
    if encoder is None:
        encoder = OneHotEncoder(
            handle_unknown="ignore",
            max_categories=max_categories,
        )
        encoder.fit(dataframe[cols_to_encode])

    dataframe = _apply_one_hot(dataframe, encoder, ignore_cols)
    if test_df is not None:
        test_df = _apply_one_hot(test_df, encoder, ignore_cols)

    return dataframe, test_df, encoder


def _apply_imputer(dataframe: pd.DataFrame, imputer: MissForest, ignore_cols: list) -> pd.DataFrame:
    """Apply a fitted imputer to ``dataframe`` and return the result."""
    all_ignore = ignore_cols + (
        dataframe.select_dtypes(include=["object"]).columns.difference(ignore_cols).tolist()
    )
    impute_df = dataframe.drop(columns=all_ignore)
    ignore_df = dataframe[all_ignore]
    columns = impute_df.columns
    imputed = pd.DataFrame(imputer.transform(impute_df), columns=columns)
    return pd.concat(
        [imputed.reset_index(drop=True), ignore_df.reset_index(drop=True)], axis=1
    )


def impute(
    dataframe: pd.DataFrame,
    test_df: pd.DataFrame | None = None,
    imputer=None,
    ignore_cols=None,
):
    """Impute missing values in ``dataframe``, optionally applying the same fitted
    imputer to ``test_df``.

    Args:
        dataframe: Training DataFrame to fit and impute.
        test_df: Optional test DataFrame to impute using the fitted imputer.
            Fit is never performed on this data.
        imputer: Pre-fitted MissForest imputer. If None, a new one is fitted on ``dataframe``.
        ignore_cols: Columns to exclude from imputation.

    Returns:
        A 3-tuple ``(dataframe, test_df, imputer)`` where ``test_df`` may be None.
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    all_ignore = ignore_cols + (
        dataframe.select_dtypes(include=["object"])
        .columns.difference(ignore_cols)
        .tolist()
    )

    impute_df = dataframe.drop(columns=all_ignore)

    # skip expensive imputation if no missing values
    if impute_df.isnull().sum().sum() == 0:
        return dataframe, test_df, imputer

    if imputer is None:
        imputer = MissForest()
        imputer.fit(impute_df)

    dataframe = _apply_imputer(dataframe, imputer, ignore_cols)
    if test_df is not None:
        test_df = _apply_imputer(test_df, imputer, ignore_cols)

    return dataframe, test_df, imputer


def save_data(structured_data, news_data, suffix=""):
    structured_data.to_csv(f"structured_data{suffix}.csv", index=False)
    if news_data is not None:
        news_data.to_csv("news_data{suffix}.csv", index=False)


def get_num_x_columns(
    structured_data: pd.DataFrame,
    ignore_cols: list = None,
    num_news: int = 0,
    embedding_lookup: Optional[Dict] = None,
) -> int:
    """Count the number of model input dimensions after preprocessing.

    Object-dtype columns (symbol, news IDs) and ``ignore_cols`` are excluded.
    When news embeddings are used, each news slot adds ``embedding_dim`` dims.

    Args:
        structured_data: Pre-processed training DataFrame.
        ignore_cols: Columns to exclude (e.g. symbol, timestamp, target).
        num_news: Number of news slots (from hyperparameter).
        embedding_lookup: Dict returned by load_data; used to infer embedding_dim.

    Returns:
        Total input dimension for NumericalModel / HybridDataset.
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    non_x_cols = set([
        *ignore_cols,
        *structured_data.select_dtypes(include="object").columns,
    ])
    n_cols = structured_data.shape[1] - len(non_x_cols)
    if num_news > 0 and embedding_lookup:
        embedding_dim = len(next(iter(embedding_lookup.values())))
        n_cols += num_news * embedding_dim
    return n_cols


def format_ignore_cols(ignore_cols):
    if ignore_cols is None:
        ignore_cols = []
    if not isinstance(ignore_cols, (list, tuple, set)):
        ignore_cols = [ignore_cols]
    return ignore_cols


def save_torch_state(folder, name, state):
    file_name = name + ".pt"
    path = os.path.join(folder, file_name)
    torch.save(state, path)
    return file_name


def load_torch_state(folder, file_name):
    path = os.path.join(folder, file_name)
    return torch.load(path)
