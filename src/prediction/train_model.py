import matplotlib.pyplot as plt
import pandas as pd
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal
from ezmt.model_tuner import ModelTuner
from src.data_access.dao_manager import dao_manager
from src.prediction.dataset import create_datasets
from src.prediction.nn_model import create_and_train
from src.prediction.pipeline_components import (
    load_short_data,
    filter_out_missing_data,
    clip_values,
    standardize_data,
    one_hot_encode,
    impute,
    save_data,
    get_num_x_columns,
)
from webrock.decorator import plugin


@plugin(model_name={"ui_element": "textbox"})
async def train_model(model_name: str, min_timestamp: int = 0, max_timestamp: int = 0):
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


# TODO
#  OneHotEncoder has some nice options to limit the number of new columns (good for industry id)
#  Fix flow when num_news > 0
#  Hyperparams for imputation
#  See DataCompiler for more to-do items


def plot_moving_average(loss_history, window_size):
    # Convert the list of numbers to a pandas Series
    series = pd.Series(loss_history)

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
