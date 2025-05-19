import datetime
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tensorflow as tf
from data_access.dao_manager import dao_manager
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal
from ezmt.model_tuner import ModelTuner
from missforest import MissForest
from plugins.decorator import plugin
from plugins.prediction.create_model import create_combined_model
from plugins.prediction.data_generator import create_generators
from sklearn.preprocessing import OneHotEncoder


@plugin(model_name={"ui_element": "textbox"})
async def train_model(model_name: str, min_timestamp: int = 0, max_timestamp: int = 0):
    min_timestamp = 1734757200  # TODO Delete this line later!
    hyperparams, model_space = create_model_space(
        max_timestamp, min_timestamp, model_name
    )
    mt = ModelTuner(model_space, hyperparams, None, "target", 1, 1)
    model = await mt.run()
    model.save(model_name)


async def train_short(model_name: str, min_timestamp: int = 0, max_timestamp: int = 0):
    hyperparams, model_space = create_model_space(
        max_timestamp, min_timestamp, model_name
    )
    model_space = [
        {
            "name": "load_data",
            "train": {
                "func": load_short_data,
                "outputs": ["structured_data", "text_data"],
            },
        }
    ] + model_space[-3:]

    mt = ModelTuner(model_space, hyperparams, None, "target", 1, 1)
    model = await mt.run()
    model.save(model_name)


def load_short_data():
    structured_data = pd.read_csv("data6.csv", index_col=0).reset_index(drop=True)
    news_data = pd.read_csv("news_data.csv")
    return structured_data, news_data


def create_model_space(max_timestamp, min_timestamp, model_name):
    hyperparams = {
        "batch_size": DiscreteOrdinal([32]),
        "max_text_length": DiscreteOrdinal([512]),
        "price_change_offset": ContinuousRange(86400, 86400 * 10),
        "max_window": DiscreteOrdinal([500, 1000, 1500, 2000, 2500, 3000, 4000, 5000]),
        "num_windows": DiscreteOrdinal([3, 5, 10]),
        "num_news": DiscreteOrdinal([1]),
        "news_history_threshold": ContinuousRange(24 * 60 * 60, 5 * 24 * 60 * 60),
        "include_close_ratio": DiscreteOrdinal([True, False]),
        "include_cv_close_ratio": DiscreteOrdinal([True, False]),
        "include_avg_volume_ratio": DiscreteOrdinal([True, False]),
        "include_cv_volume_ratio": DiscreteOrdinal([True, False]),
        "n_hidden_layers": DiscreteOrdinal([1, 2, 3, 4, 5, 6, 7]),
        "hidden_layer_dim": DiscreteOrdinal([100, 250, 500, 750, 1000, 1500, 2000]),
        "dropout_rate": ContinuousRange(0.3, 0.4),
        "missing_data_%_threshold": ContinuousRange(0.2, 0.25),
    }
    model_space = [
        {
            "name": "load_data",
            "train": {
                "func": load_data,
                "kwargs": {
                    "price_change_offset": "price_change_offset",
                    "min_timestamp": min_timestamp,
                    "max_timestamp": max_timestamp,
                    "max_window": "max_window",
                    "num_windows": "num_windows",
                    "num_news": "num_news",
                    "news_history_threshold": "news_history_threshold",
                    "include_close_ratio": "include_close_ratio",
                    "include_cv_close_ratio": "include_cv_close_ratio",
                    "include_avg_volume_ratio": "include_avg_volume_ratio",
                    "include_cv_volume_ratio": "include_cv_volume_ratio",
                },
                "outputs": ["structured_data", "text_data"],
            },
        },
        {
            "name": "filter_out_missing_data",
            "func": filter_out_missing_data,
            "args": ["structured_data", "missing_data_%_threshold"],
            "outputs": "structured_data",
        },
        {
            "name": "standardize_data",
            "train": {
                "func": standardize_data,
                "args": ["structured_data"],
                "outputs": ["structured_data", "means", "stds"],
            },
            "inference": {
                "func": standardize_data,
                "args": ["structured_data", "means", "stds"],
                "outputs": "structured_data",
            },
        },
        {
            "name": "impute",
            "train": {
                "func": impute,
                "args": ["structured_data"],
                "kwargs": {"ignore_cols": ["target"]},
                "outputs": ["structured_data", "imputer"],
            },
            # "inference": {
            #     "func": impute,
            #     "args": ["structured_data", "imputer"],
            #     "outputs": "structured_data",
            # }
        },
        {
            "name": "one_hot_encode",
            "train": {
                "func": one_hot_encode,
                "args": ["structured_data"],
                "kwargs": {"ignore_cols": ["news1_id", "symbol", "name"]},
                "outputs": ["structured_data", "one_hot_encoder"],
            },
            # "inference": {
            #     "func": one_hot_encode,
            #     "args": ["structured_data", "one_hot_encoder"],
            #     "outputs": "structured_data",
            # }
        },
        {
            "name": "save_data",
            "train": {
                "func": save_data,
                "args": ["structured_data", "text_data"],
                "outputs": [],
            },
        },
        {
            "name": "create_generators",
            "train": {
                "func": create_generators,
                "args": [
                    "structured_data",
                    "text_data",
                    "batch_size",
                    "M-FAC/bert-tiny-finetuned-mrpc",
                    "max_text_length",
                ],
                "outputs": ["train_generator", "test_generator"],
            },
        },
        {
            "name": "get_structured_input_dim",
            "train": {
                "func": get_num_x_columns,
                "args": ["structured_data"],
                "outputs": "structured_input_dim",
            },
        },
        {
            "name": "create_and_train",
            "train": {
                "func": create_and_train,
                "args": [
                    model_name,
                    "num_news",
                    "structured_input_dim",
                    "n_hidden_layers",
                    "hidden_layer_dim",
                    "dropout_rate",
                    "train_generator",
                    "test_generator",
                    10,  # epochs
                ],
                "outputs": "score",
                "gpu": True,
            },
        },
    ]
    return hyperparams, model_space


async def load_data(
    price_change_offset: int = 86400,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 24 * 60 * 60,
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
):
    structured_data_dao = dao_manager.get_dao("DataCompiler")
    structured_data = await structured_data_dao.get_data(
        price_change_offset,
        min_timestamp,
        max_timestamp,
        max_window,
        num_windows,
        num_news,
        news_history_threshold,
        include_close_ratio,
        include_cv_close_ratio,
        include_avg_volume_ratio,
        include_cv_volume_ratio,
    )
    if num_news > 0:
        news_data_dao = dao_manager.get_dao("News")
        news_data = await news_data_dao.get_all()
    else:
        news_data = None
    return structured_data, news_data


def filter_out_missing_data(structured_data, missing_data_threshold):
    # filter out rows with  the number of missing values is above the threshold
    missing_data_ratio = structured_data.isnull().sum(axis=1) / structured_data.shape[1]
    return structured_data[missing_data_ratio <= missing_data_threshold]


def standardize_data(dataframe, means=None, stds=None):
    """
    Standardize the numeric columns of the DataFrame, ignoring object dtype columns.

    Parameters:
    - dataframe: pd.DataFrame
        The input DataFrame to be standardized.
    - means: pd.Series, optional
        Precomputed means of the numeric columns. If None, means will be computed from the DataFrame.
    - stds: pd.Series, optional
        Precomputed standard deviations of the numeric columns. If None, stds will be computed from the DataFrame.

    Returns:
    - standardized_df: pd.DataFrame
        The DataFrame with standardized numeric columns.
    - means: pd.Series
        The means of the numeric columns.
    - stds: pd.Series
        The standard deviations of the numeric columns.
    """
    numeric_cols = dataframe.select_dtypes(include=[np.number]).columns

    if means is None:
        means = dataframe[numeric_cols].mean()
    if stds is None:
        stds = dataframe[numeric_cols].std()

    standardized_df = dataframe.copy()
    standardized_df[numeric_cols] = (dataframe[numeric_cols] - means) / stds

    return standardized_df, means, stds


def one_hot_encode(dataframe: pd.DataFrame, encoder=None, ignore_cols: list = None):
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
        encoder = OneHotEncoder(handle_unknown="ignore")
        encoded_data = encoder.fit_transform(dataframe[cols_to_encode]).toarray()
    else:
        encoded_data = encoder.transform(dataframe[cols_to_encode]).toarray()

    encoded_df = pd.DataFrame(
        encoded_data, columns=encoder.get_feature_names_out(cols_to_encode)
    ).reset_index(drop=True)
    dataframe = pd.concat(
        [dataframe.drop(columns=cols_to_encode).reset_index(drop=True), encoded_df],
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


def save_data(structured_data, news_data):
    structured_data.to_csv("structured_data.csv", index=False)
    if news_data is not None:
        news_data.to_csv("news_data.csv", index=False)


def create_and_train(
    model_name,
    num_news,
    structured_input_dim,
    n_hidden_layers,
    hidden_layer_dim,
    dropout_rate,
    train_generator,
    test_generator,
    epochs,
):
    model = create_combined_model(
        num_news,
        structured_input_dim,
        n_hidden_layers,
        hidden_layer_dim,
        1,
        dropout_rate=dropout_rate,  # output dim
    )

    # Define the EarlyStopping callback
    early_stopping = tf.keras.callbacks.EarlyStopping(
        monitor="val_loss",  # Monitor the validation loss
        patience=3,  # Number of epochs with no improvement after which training will be stopped
        verbose=1,  # Verbosity mode
        restore_best_weights=True,  # Restore model weights from the epoch with the best value of the monitored quantity
    )

    history = model.fit(
        train_generator,
        epochs=epochs,
        validation_data=test_generator,
        callbacks=[early_stopping],
    )
    plot_moving_average(history, 10)
    save_model(model, model_name=model_name)
    avg_val_loss = np.mean(history.history["val_loss"][-10:])
    return avg_val_loss


def save_model(model, model_folder: str = "models", model_name: str = None):
    if model_name is None:
        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        model_name = f"model_{dt}_{pid}.h5"
    model_path = os.path.join(model_folder, model_name)
    model.save(model_path)
    return model_path


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


# TODO
#  Verify what format the bert encoder is expecting (tokens + input mask? standardized?)
#  OneHotEncoder has some nice options to limit the number of new columns (good for industry id)
#  Hyperparams for imputation
#  See DataCompiler for more to-do items
#  de-couple statistics and news data from initial data load
#  OR
#  make a separate query to get only the necessary info for new_data


def plot_moving_average(history, window_size):
    data = history.history["val_loss"]

    # Convert the list of numbers to a pandas Series
    series = pd.Series(data)

    # Calculate the moving average
    moving_average = series.rolling(window=window_size).mean()

    # Plot the original data
    plt.figure(figsize=(10, 6))
    plt.plot(series, label="Original Data", color="blue")

    # Plot the moving average
    plt.plot(
        moving_average, label=f"Moving Average (window={window_size})", color="red"
    )

    # Add labels and legend
    plt.title("Moving Average Plot")
    plt.xlabel("Index")
    plt.ylabel("Value")
    plt.legend()

    # Show the plot
    plt.savefig("loss_history.png")
