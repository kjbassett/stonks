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
from missforest import MissForest
from sklearn.preprocessing import OneHotEncoder


@plugin(model_name={"ui_element": "textbox"})
async def train_model(model_name: str, min_timestamp: int = 0, max_timestamp: int = 0):
    hyperparams = {
        "batch_size": DiscreteOrdinal([64]),
        "max_text_length": DiscreteOrdinal([512]),
        "price_change_offset": ContinuousRange(86400, 86400*5),
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
        "dropout_rate": ContinuousRange(0.3, 0.4),
        "missing_data_%_threshold": ContinuousRange(0.15, 0.25)  # Should go from 0 to x, not 0.15
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
                    "include_symbol": "include_symbol",
                    "include_industry": "include_industry",
                    "include_office": "include_office",
                    "include_price_change": "include_price_change",
                    "include_volume_change": "include_volume_change",
                    "include_coeff_var": "include_coeff_var",
                    "include_price_over_average": "include_price_over_average",
                    "include_volume_over_average": "include_volume_over_average",
                    "text_encoder": "M-FAC/bert-tiny-finetuned-mrpc",
                    "max_text_length": "max_text_length",
                },
                "outputs": ["structured_data", "enc_text_data"],
            }
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
                "func": standardize_data, "args": ["structured_data"], "outputs": ["structured_data", "means", "stds"]
            },
            "inference": {
                "func": standardize_data,
                "args": ["structured_data", "means", "stds"],
                "outputs": "structured_data"
            }
        },
        {
            "name": "impute",
            "train": {
                "func": impute,
                "args": ["structured_data"],
                "kwargs": {"ignore_cols": ["news1_id", "target"]},
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
                "kwargs": {"ignore_cols": ["news1_id"]},
                "outputs": ["structured_data", "one_hot_encoder"]
            },
            # "inference": {
            #     "func": one_hot_encode,
            #     "args": ["structured_data", "one_hot_encoder"],
            #     "outputs": "structured_data",
            # }
        },
        {
            "name": "create_generators",
            "train": {
                "func": create_generators,
                "args": [
                    "batch_size",
                    "structured_data",
                    "enc_text_data",
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
    price_change_offset: int = 86400,
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
        price_change_offset,
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
    price_change_offset,
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
        price_change_offset,
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
        include_volume_over_average
    )


async def get_news_data(text_encoder, max_text_length):
    # Should I init the dao manager?
    news_data_dao = dao_manager.get_dao("News")
    news_data = await news_data_dao.get_all()
    tokenizer = BertTokenizer.from_pretrained(text_encoder)  # tokenizer uses same name as encoder
    enocded = news_data["body"].apply(encode_text, args=(tokenizer, max_text_length))
    enocded_expanded = pd.DataFrame(enocded.tolist(), index=news_data.index)
    news_data = pd.concat([news_data.drop(columns=['body']), enocded_expanded], axis=1)
    news_data = news_data[['id'] + list(enocded_expanded.columns)]
    news_data.to_csv("news_data.csv")
    return news_data


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
    cols_to_encode = dataframe.select_dtypes(include=['object']).columns.difference(ignore_cols)

    if encoder is None:
        encoder = OneHotEncoder(handle_unknown='ignore')
        encoded_data = encoder.fit_transform(dataframe[cols_to_encode]).toarray()
    else:
        encoded_data = encoder.transform(dataframe[cols_to_encode]).toarray()

    encoded_df = pd.DataFrame(encoded_data, columns=encoder.get_feature_names_out(cols_to_encode))
    dataframe = pd.concat([dataframe.drop(columns=cols_to_encode), encoded_df], axis=1)

    return dataframe, encoder


def impute(dataframe, imputer=None, ignore_cols=None):
    if ignore_cols is None:
        ignore_cols = []
    ignore_cols += dataframe.select_dtypes(include=['object']).columns.difference(ignore_cols).tolist()
    df_to_impute = dataframe.drop(columns=ignore_cols)
    if df_to_impute.isnull().sum().sum() == 0:
        return dataframe, None
    if imputer is None:
        imputer = MissForest()
        imputer.fit(df_to_impute)
    imputed_array = imputer.transform(df_to_impute)
    imputed_data = pd.DataFrame(imputed_array, columns=df_to_impute.columns)
    dataframe = pd.concat([imputed_data, dataframe[ignore_cols]], axis=1)
    return dataframe, imputer


def create_and_train(
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


def get_num_x_columns(structured_data):
    if "target" in structured_data.columns:
        return structured_data.shape[1] - 1  # exclude target
    else:
        return structured_data.shape[1]


def get_score(history):
    return history.history["val_loss"][-1]


# TODO
#  Verify what format the bert encoder is expecting (tokens + input mask? standardized?)
#  OneHotEncoder has some nice options to limit the number of new columns (good for industry id)
#  Hyperparams for imputation
#  See DataAggregator for more to-do items
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
