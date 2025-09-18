import datetime

import numpy as np
import pandas as pd
from missforest import MissForest
from sklearn.preprocessing import OneHotEncoder


def load_short_data():
    # load csv of saved data from some intermediate step
    structured_data = pd.read_csv("structured_data.csv", index_col=None).reset_index(
        drop=True
    )
    return structured_data, None


def filter_out_missing_data(structured_data, missing_data_threshold, ignore_cols=None, no_tolerance_cols=None):
    # filter out rows with the number of missing values is above the threshold
    ignore_df = None
    if ignore_cols:
        ignore_df = structured_data[ignore_cols]
        structured_data = structured_data[
            structured_data.columns.difference(ignore_cols)
        ]
    n = len(structured_data)
    missing_data_ratio = structured_data.isnull().sum(axis=1) / structured_data.shape[1]
    filt = missing_data_ratio < missing_data_threshold
    structured_data = structured_data[filt]
    if ignore_df:
        ignore_df = ignore_df[filt]
        structured_data = pd.concat([structured_data, ignore_df], axis=1)
    print(f"Removed {n - len(structured_data)} rows")

    # filter out no_tolerance columns if any value is null in them.
    # For example, filter out rows with a null target column
    if no_tolerance_cols:
        structured_data = structured_data[~structured_data[no_tolerance_cols].isnull().any(axis=1)]

    return structured_data


def clip_values(df, column_limits=None):
    if not column_limits:
        column_limits = {}
    for col in df.select_dtypes(include=[float, int]).columns:
        if col not in column_limits:
            column_limits[col] = {
                "lower": df[col].quantile(0.01),
                "upper": df[col].quantile(0.99),
            }
        lower = column_limits[col]["lower"]
        upper = column_limits[col]["upper"]
        df[col] = df[col].clip(lower=lower, upper=upper)
    return df, column_limits


def standardize_data(dataframe, means=None, stds=None):
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
    numeric_cols = dataframe.select_dtypes(include=[np.number]).columns

    if means is None:
        means = dataframe[numeric_cols].mean().to_dict()
    if stds is None:
        stds = dataframe[numeric_cols].std().to_dict()

    # Use .loc to avoid SettingWithCopyWarning
    dataframe.loc[:, numeric_cols] = (
        dataframe[numeric_cols] - pd.Series(means)
    ) / pd.Series(stds)

    return dataframe, means, stds


def unstandardize(predictions, uncertainties, means, stds):
    mean, std = means["target"], stds["target"]

    uncertainties = np.sqrt(uncertainties)  # variance to standard deviation

    # unscale prediction
    predictions = predictions * std + mean
    # scale factor is std. predictions scale with a factor of std, and so do standard deviations
    uncertainties = uncertainties * std

    df = pd.DataFrame({"prediction": predictions, "uncertainty": uncertainties})
    dt = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    df.to_csv(f"predictions_{dt}.csv", index=False)
    return df


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
    if ignore_cols is None:
        ignore_cols = []
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
    if ignore_cols is None:
        ignore_cols = []
    ignore_cols += (
        dataframe.select_dtypes(include=["object"])
        .columns.difference(ignore_cols)
        .tolist()
    )
    df_to_impute = dataframe.drop(columns=ignore_cols)
    if df_to_impute.isnull().sum().sum() == 0:
        return dataframe, None
    if imputer is None:
        imputer = MissForest()
        imputer.fit(df_to_impute)
    imputed_array = imputer.transform(df_to_impute)
    imputed_data = pd.DataFrame(
        imputed_array, columns=df_to_impute.columns
    ).reset_index(drop=True)
    dataframe = pd.concat(
        [imputed_data, dataframe[ignore_cols].reset_index(drop=True)], axis=1
    )
    return dataframe, imputer


def save_data(structured_data, news_data, suffix=""):
    structured_data.to_csv(f"structured_data{suffix}.csv", index=False)
    if news_data is not None:
        news_data.to_csv("news_data{suffix}.csv", index=False)


def get_num_x_columns(structured_data):
    n_cols = (
        structured_data.shape[1]
        - structured_data.select_dtypes(include="object").shape[1]
    )
    if "target" in structured_data.columns:
        n_cols -= 1
    return n_cols


def get_score(history):
    return history.history["val_loss"][-1]
