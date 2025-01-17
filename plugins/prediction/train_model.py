import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

from data_access.dao_manager import dao_manager
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal, DiscreteNonOrdinal
from ezmt.model_tuner import ModelTuner
from plugins.decorator import plugin
from plugins.prediction.create_model import create_combined_model
from plugins.prediction.data_generator import create_generators
from transformers import BertTokenizer


@plugin(model_name={"ui_element": "textbox", "default": "Genesis"})
async def train_model(model_name: str, min_timestamp:int=0, max_timestamp:int=0):
    hyperparams = {
        "batch_size": DiscreteOrdinal([20]),
        "max_text_length": DiscreteOrdinal([512]),
        "max_window": DiscreteOrdinal([500, 1000, 1500, 2000]),
        "num_windows": DiscreteOrdinal([3, 5, 10]),
        "num_news": DiscreteOrdinal([1]),
        "news_history_threshold": ContinuousRange(24*60*60, 5*24*60*60),
        "include_symbol": DiscreteOrdinal([True, False]),
        "include_industry": DiscreteOrdinal([True, False]),
        "include_office": DiscreteOrdinal([True, False]),
        "include_price_change": DiscreteOrdinal([True, False]),
        "include_volume_change": DiscreteOrdinal([True, False]),
        "include_coeff_var": DiscreteOrdinal([True, False]),
        "include_price_over_average": DiscreteOrdinal([True, False]),
        "include_volume_over_average": DiscreteOrdinal([True, False]),
        "n_hidden_layers": DiscreteOrdinal([1, 2, 3, 4, 5, 6]),
        "hidden_layer_dim": DiscreteOrdinal([100, 250, 500, 750, 1000, 1500, 2000]),
        "dropout_rate": ContinuousRange(0.1, 0.3)
    }
    model_space = [
        {
            "name": "load_data",
            "train": {
                "func": load_data,
                "args": [min_timestamp, max_timestamp],
                "kwargs": {
                    "max_window": "max_window",
                    "num_windows": "num_windows",
                    "num_news": "num_news",
                    "news_history_threshold": "news_history_threshold",
                    "include_symbol": "include_symbol",
                    "include_industry": "include_industry",
                    "include_office": "include_office",
                    "include_price_change": "include_price_change",
                    "include_volume_change": "include_volume_change",
                    "include_coeff_var": "include_coeff_var",
                    "include_price_over_average": "include_price_over_average",
                    "include_volume_over_average": "include_volume_over_average",
                    "text_encoder": "M-FAC/bert-tiny-finetuned-mrpc",
                    "max_text_length": "max_text_length"
                }
            }
        },
        {
            "name": "create_generators",
            "train": {
                "func": create_generators,
                "args": [
                    "batch_size",
                ],
                "kwargs": {
                    "price_change_window": 86400,
                    "avg_close": "avg_close",
                    "avg_volume": "avg_volume",
                    "std_dev": "std_dev",
                    "n_news": "num_news",
                    "news_relative_age_threshold": "news_relative_age_threshold",
                },
                "outputs": ["train_generator", "test_generator"],
                "run_in_parent_process": True,
            },
        },
        {
            "name": "load_train_batch",
            "train": {
                "func": "train_generator.get_batch",
                "args": 0,
                "outputs": ["x_train", "y_train"],
                "run_in_parent_process": True,
            },
        },
        {
            "name": "get_structured_input_dim",
            "train": {
                "func": count_structured_input_dim,
                "args": ["x_train", "num_news"],
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
                    "hidden_layer_dim",
                    "train_generator",
                    "test_generator",
                    10,  # epochs
                ],
                "outputs": "score",
                "gpu": True,
            },
        },
        # {
        #     'name': 'score',
        #     'train': {
        #         'func': get_score,
        #         'args': 'history',
        #         'outputs': 'score'
        #     }
        # }
    ]
    mt = ModelTuner(model_space, hyperparams, None, "target", 1, 1)
    model = await mt.run()
    model.save(model_name)


async def load_data(
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 24 * 60 * 60,
    include_symbol: bool = False,
    include_industry: bool = False,
    include_office: bool = False,
    include_price_change: bool = False,
    include_volume_change: bool = False,
    include_coeff_var: bool = False,
    include_price_over_average: bool = False,
    include_volume_over_average: bool = False,
    text_encoder: str = None,
    max_text_length: int = 512,
):
    structure_data = await get_structure_data(
        min_timestamp,
        max_timestamp,
        max_window,
        num_windows,
        num_news,
        news_history_threshold,
        include_symbol,
        include_industry,
        include_office,
        include_price_change,
        include_volume_change,
        include_coeff_var,
        include_price_over_average,
        include_volume_over_average,
    )
    if num_news > 0:
        news_data = await get_news_data(text_encoder, max_text_length)
    else:
        news_data = None
    return structure_data, news_data


async def get_structure_data(
    min_timestamp,
    max_timestamp,
    max_window,
    num_windows,
    num_news,
    news_history_threshold,
    include_symbol,
    include_industry,
    include_office,
    include_price_change,
    include_volume_change,
    include_coeff_var,
    include_price_over_average,
    include_volume_over_average,
):
    structured_data_dao = dao_manager.get_dao("DataAggregator")
    return await structured_data_dao.get_data(
        min_timestamp,
        max_timestamp,
        max_window,
        num_windows,
        num_news,
        news_history_threshold,
        include_symbol,
        include_industry,
        include_office,
        include_price_change,
        include_volume_change,
        include_coeff_var,
        include_price_over_average,
        include_volume_over_average,
    )


async def get_news_data(text_encoder, max_text_length):
    # Should I init the dao manager?
    news_data_dao = dao_manager.get_dao("News")
    news_data = await news_data_dao.get_all()
    tokenizer = BertTokenizer.from_pretrained(text_encoder)  # tokenizer uses same name as encoder
    news_data["body"] = news_data["body"].apply(encode_text, tokenizer, max_text_length)
    print(news_data["body"])  # debug line
    return news_data[['id', 'body']]

    # TODO You left off here 1/15/25
    #  last commit WIP separating data loading and generation
    #  Generator can become synchronous and be used directly in fit.
    #  Just need to merge batch of structured and news data in __getiem__


def encode_text(text, tokenizer, max_length):
    encoded_dict = tokenizer.encode_plus(
        text,
        add_special_tokens=True,
        max_length=max_length,
        padding="max_length",
        truncation=True,
        return_attention_mask=True,
        return_tensors="tf"
    )

    return np.hstack([encoded_dict["input_ids"], encoded_dict["attention_mask"]])[0]


async def create_and_train(
    model_name,
    num_news,
    structured_input_dim,
    hidden_layer_dim,
    train_generator,
    test_generator,
    epochs,
):
    model = create_combined_model(
        num_news, structured_input_dim, hidden_layer_dim, 1  # output dim
    )
    history = model.fit(train_generator, epochs=epochs, validation_data=test_generator)
    plot_moving_average(history, 50)
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


def count_structured_input_dim(array, num_news):
    print("array shape:", array.shape)
    print("num_news:", num_news)
    return array.shape[1] - 512 * 2 * num_news


def get_score(history):
    return history.history["val_loss"][-1]


# TODO
#  Normalization
#  de-couple statistics and news data from initial data load
#  OR
#  make a separate query to get only the necessary info for new_data


def plot_moving_average(data, window_size):
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
