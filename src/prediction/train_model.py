import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal
from ezmt.model_tuner import ModelTuner
from missforest import MissForest
from sklearn.preprocessing import OneHotEncoder
from src.data_access.dao_manager import dao_manager
from src.prediction.dataset import create_datasets
from src.prediction.nn_model import create_and_train
from webrock.decorator import plugin


@plugin(model_name={"ui_element": "textbox"})
async def train_model(model_name: str, min_timestamp: int = 0, max_timestamp: int = 0):
    min_timestamp = 1734757200  # TODO Delete this line later!
    # define possible choices for all hyperparameters
    hyperparams, model_space = create_model_space(
        max_timestamp, min_timestamp, model_name
    )
    # Run genetic algorithm to tune hyperparameters
    mt = ModelTuner(model_space, hyperparams, None, "target", 1, 1)
    model = await mt.run()
    model.save(model_name)


@plugin()
async def train_short(model_name: str, min_timestamp: int = 0, max_timestamp: int = 0):
    # train from csv of saved data from some intermediate step
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
    ] + model_space[7:]

    mt = ModelTuner(model_space, hyperparams, None, "target", 1, 1)
    model = await mt.run()
    model.save(model_name)


def load_short_data():
    # load csv of saved data from some intermediate step
    structured_data = pd.read_csv("structured_data.csv", index_col=None).reset_index(
        drop=True
    )
    return structured_data, None


def create_model_space(max_timestamp, min_timestamp, model_name):
    hyperparams = {
        "batch_size": DiscreteOrdinal([64]),  # neural net batch size
        "max_text_length": DiscreteOrdinal([512]),  # text encoder length
        # for price_change_offset...
        # when aggregation is minutes, seconds ahead of current row for calculating percent changes
        # when aggregation is hours, rows ahead of current row for calculating percent changes
        # TODO fix this ^ nonsense. Go to rows only because it skips over closed market hours
        "price_change_offset": DiscreteOrdinal(range(1, 9)),
        "max_window": DiscreteOrdinal(
            range(3, 11)
        ),  # maximum row behind current row to see trends
        "num_windows": DiscreteOrdinal(
            [3, 5, 10]
        ),  # number of points in time behind current row to compare for trends
        "num_news": DiscreteOrdinal(
            [0]
        ),  # number of news articles previous to the current row to include
        "news_history_threshold": ContinuousRange(
            24 * 60 * 60, 5 * 24 * 60 * 60
        ),  # The oldest a news article could be
        "include_close_ratio": DiscreteOrdinal(
            [True, False]
        ),  # include ratio of current close to past close
        "include_cv_close_ratio": DiscreteOrdinal(
            [True, False]
        ),  # include coef var of close data from past to current
        "include_avg_volume_ratio": DiscreteOrdinal(
            [True, False]
        ),  # include avg volume from past to current
        "include_cv_volume_ratio": DiscreteOrdinal(
            [True, False]
        ),  # include coef car of volume from past to current
        "max_one_hot_categories": DiscreteOrdinal(
            [20]
        ),  # maximum number of categories/columns will be created per original column when one-hot encoding
        "n_hidden_layers": DiscreteOrdinal(
            [1, 2, 3, 4, 5, 6, 7]
        ),  # number of hidden layers in NN
        "hidden_dim": DiscreteOrdinal(
            [100, 250, 500, 750, 1000, 1500, 2000]
        ),  # number of nodes per hidden layer
        "dropout_rate": ContinuousRange(
            0.3, 0.4
        ),  # chance of dropout per dropout layer in NN
        "missing_data_%_threshold": ContinuousRange(
            0.5, 0.75
        ),  # threshold of % of missing data to remove pt.
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
            "name": "clip_values",
            "train": {
                "func": clip_values,
                "args": "structured_data",
                "outputs": "structured_data",
            },
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
                "outputs": ["structured_data", "means", "stds"],
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
                "kwargs": {
                    "ignore_cols": ["news1_id"],
                    "max_categories": "max_one_hot_categories",
                },
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
            "name": "create_datasets",
            "train": {
                "func": create_datasets,
                "args": [
                    "structured_data",
                    "text_data",
                    "M-FAC/bert-tiny-finetuned-mrpc",
                    "max_text_length",
                ],
                "outputs": ["train_dataset", "test_dataset"],
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
                    "structured_input_dim",
                    "n_hidden_layers",
                    "hidden_dim",
                    "dropout_rate",
                    "train_dataset",
                    "test_dataset",
                    "batch_size",
                    10,
                    "num_news",
                    "M-FAC/bert-tiny-finetuned-mrpc",
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
        "hour",
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
        print_query=True,
    )
    if num_news > 0:
        news_data_dao = dao_manager.get_dao("News")
        news_data = await news_data_dao.get_all()
    else:
        news_data = None
    return structured_data, news_data


def filter_out_missing_data(structured_data, missing_data_threshold):
    # filter out rows with the number of missing values is above the threshold
    n = len(structured_data)
    missing_data_ratio = structured_data.isnull().sum(axis=1) / structured_data.shape[1]
    structured_data = structured_data[missing_data_ratio < missing_data_threshold]
    print(f"Removed {n - len(structured_data)} rows")
    return structured_data


def clip_values(df):
    for col in df.select_dtypes(include=[float, int]).columns:
        df[col] = df[col].clip(
            lower=df[col].quantile(0.01), upper=df[col].quantile(0.99)
        )
    df.to_csv("clipped_data")
    return df


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

    dataframe[numeric_cols] = (dataframe[numeric_cols] - means) / stds

    return dataframe, means, stds


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


# TODO
#  OneHotEncoder has some nice options to limit the number of new columns (good for industry id)
#  Fix flow when num_news > 0
#  Hyperparams for imputation
#  See DataCompiler for more to-do items


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
