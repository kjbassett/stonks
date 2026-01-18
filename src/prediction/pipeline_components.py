import datetime
import os
import time

import numpy as np
import pandas as pd
import torch
from missforest import MissForest
from sklearn.preprocessing import OneHotEncoder
from src.data_access.dao_manager import dao_manager


async def load_data(
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 24 * 60 * 60,
    include_target: bool = True,
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
    keep_latest_only: bool = False,
):
    if min_timestamp < 0:
        min_timestamp = time.time() + min_timestamp
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
        include_close_ratio,
        include_cv_close_ratio,
        include_avg_volume_ratio,
        include_cv_volume_ratio,
        keep_latest_only,
        print_query=True,
    )
    if num_news > 0:
        news_data_dao = dao_manager.get_dao("News")
        news_data = await news_data_dao.get_all()
    else:
        news_data = None
    return structured_data, news_data


def load_short_data(file_name):
    print("loading data")
    t = time.time()
    # load csv of saved data from some intermediate step
    structured_data = pd.read_csv(file_name, index_col=None).reset_index(drop=True)
    print(f"data loaded after {int(time.time() - t)} seconds")
    return structured_data, None


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
    print(f"Removed {n - len(structured_data)} rows")
    return structured_data


def clip_values(df, ignore_cols=None, column_limits=None):
    ignore_cols = format_ignore_cols(ignore_cols)
    if not column_limits:
        column_limits = {}
    for col in df.select_dtypes(include=[float, int]).columns.difference(ignore_cols):
        if col not in column_limits:
            column_limits[col] = {
                "lower": df[col].quantile(0.01),
                "upper": df[col].quantile(0.99),
            }
        lower = column_limits[col]["lower"]
        upper = column_limits[col]["upper"]
        df[col] = df[col].clip(lower=lower, upper=upper)
    return df, column_limits


def standardize_data(dataframe, means=None, stds=None, ignore_cols=None):
    """
    Standardize the numeric columns of the DataFrame, ignoring object dtype columns.

    Parameters:
    - dataframe: pd.DataFrame
        The input DataFrame to be standardized.
    - means: dict, optional
        Precomputed means of the numeric columns. If None, means will be computed.
    - stds: dict, optional
        Precomputed stds of the numeric columns. If None, stds will be computed.

    Returns:
    - standardized_df: pd.DataFrame
        The standardized DataFrame.
    - means: dict
        Means of the numeric columns (JSON-serializable).
    - stds: dict
        Stds of the numeric columns (JSON-serializable).
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    numeric_cols = dataframe.select_dtypes(include=[np.number]).columns.difference(
        ignore_cols
    )

    if means is None:
        means = dataframe[numeric_cols].mean().to_dict()
    if stds is None:
        stds = dataframe[numeric_cols].std().to_dict()

    # inference doesn't have the target column
    _means = {k: v for k, v in means.items() if k in numeric_cols}
    _stds = {k: v for k, v in stds.items() if k in numeric_cols}

    # Use .loc to avoid SettingWithCopyWarning
    dataframe.loc[:, numeric_cols] = (
        dataframe[numeric_cols] - pd.Series(_means)
    ) / pd.Series(_stds)

    return dataframe, means, stds


def unstandardize(predictions, means, stds):
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

    dt = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    predictions.to_csv(f"predictions_{dt}.csv", index=False)
    return predictions


def one_hot_encode(
    dataframe: pd.DataFrame,
    encoder=None,
    ignore_cols: list = None,
    max_categories: int = None,
):
    """
    Fit and transform the DataFrame using one-hot encoding for all object dtype columns.
    If an encoder is provided, it will be used to transform the data.

    Parameters:
    - dataframe: pd.DataFrame
        The input DataFrame to be one-hot encoded.
    - encoder: OneHotEncoder, optional
        A pre-fitted OneHotEncoder. If None, a new encoder will be fitted.
    - ignore_cols: list, optional
        List of columns to ignore for encoding.
    - max_categories: int, optional
        If provided, limits the number of categories per feature.
        Rare categories are grouped into 'infrequent' bucket.

    Returns:
    - encoder: OneHotEncoder
        The fitted OneHotEncoder.
    - cols_to_encode: list
        List of columns that were encoded.
    - transformed_df: pd.DataFrame
        The DataFrame with one-hot encoded columns.
    """
    ignore_cols = format_ignore_cols(ignore_cols)
    cols_to_encode = dataframe.select_dtypes(include=["object"]).columns.difference(
        ignore_cols
    )
    if encoder is None:
        encoder = OneHotEncoder(
            handle_unknown="ignore",
            max_categories=max_categories,  # <-- controls category cap
        )
        encoded_data = encoder.fit_transform(dataframe[cols_to_encode])
    else:
        encoded_data = encoder.transform(dataframe[cols_to_encode])

    encoded_df = pd.DataFrame(
        encoded_data.toarray(), columns=encoder.get_feature_names_out(cols_to_encode)
    )
    dataframe = pd.concat(
        [
            dataframe.drop(columns=cols_to_encode).reset_index(drop=True),
            encoded_df.reset_index(drop=True),
        ],
        axis=1,
    )

    return dataframe, encoder


def impute(dataframe, imputer=None, ignore_cols=None):
    ignore_cols = format_ignore_cols(ignore_cols)
    # We can only impute numerical columns
    # add object cols to ignore_cols if object col is not already in ignore cols
    ignore_cols += (
        dataframe.select_dtypes(include=["object"])
        .columns.difference(ignore_cols)
        .tolist()
    )

    # split data into imputable and non-imputable
    impute_df = dataframe.drop(columns=ignore_cols)
    ignore_df = dataframe[ignore_cols]
    columns = impute_df.columns

    # check if there are any missing values before doing expensive impute operation
    if impute_df.isnull().sum().sum() == 0:
        return dataframe, None

    # if an imputer was not supplied, create one and fit it to data
    if imputer is None:
        imputer = MissForest()
        imputer.fit(impute_df)

    # impute data
    impute_df = imputer.transform(impute_df)
    impute_df = pd.DataFrame(impute_df, columns=columns)

    # recombine with non-imputable data
    dataframe = pd.concat(
        [impute_df.reset_index(drop=True), ignore_df.reset_index(drop=True)], axis=1
    )
    return dataframe, imputer


def save_data(structured_data, news_data, suffix=""):
    structured_data.to_csv(f"structured_data{suffix}.csv", index=False)
    if news_data is not None:
        news_data.to_csv("news_data{suffix}.csv", index=False)


def get_num_x_columns(structured_data, ignore_cols=None):
    ignore_cols = format_ignore_cols(ignore_cols)
    non_x_cols = [
        *ignore_cols,
        *structured_data.select_dtypes(include="object").columns,
    ]
    non_x_cols = set(non_x_cols)
    n_cols = structured_data.shape[1] - len(non_x_cols)
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
