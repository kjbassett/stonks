import matplotlib.pyplot as plt
import pandas as pd
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal
from ezmt.model_tuner import ModelTuner
from src.prediction.dataset import create_datasets
from src.prediction.nn_model import create_model, train_model, load_model, infer
from src.prediction.pipeline_components import (
    filter_out_missing_data,
    scale_data,
    one_hot_encode,
    impute,
    get_num_x_columns,
    unscale_data,
    load_data,
    save_torch_state,
    load_torch_state,
)
from src.simulation.simulator import train_trading_policy, apply_trading_policy
from webrock.decorator import plugin


@plugin()
async def run_genetic_algorithm(
    run_name: str,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    log_states: bool = False,
):
    # define possible choices for all hyperparameters
    model_space, hyperparam_space, save_load_funcs = create_model_space(
        max_timestamp, min_timestamp
    )
    # Run genetic algorithm to tune hyperparameters
    mt = ModelTuner(
        model_space, hyperparam_space, save_load_funcs, None, "target", 1, 1
    )
    model = await mt.run(run_name, log_states=log_states)
    model.save()


@plugin()
async def run_short_genetic_algorithm(
    source_name: str,
    source_version: str = "latest",
    gene_index: str = None,
    new_name: str = None,
    new_version: str = None,
    log_states: bool = False,
    recreate_dna: bool = False,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
):
    from ezmt.organism import Organism

    model = Organism.load(source_name, source_version, gene_index=gene_index)
    model.new_version(name=new_name, version=new_version)
    if recreate_dna:
        from ezmt.model_tuner import choose_dna, validate_config

        model_space, hyperparam_space, _ = create_model_space(
            max_timestamp, min_timestamp
        )
        model_space = validate_config(model_space, hyperparam_space)
        dna = choose_dna(model_space)
        model.dna = dna
    result = await model.run(mode="train", log_states=log_states, result_name="score")
    model.save()


def create_model_space(max_timestamp, min_timestamp):
    hyperparam_space = {
        "batch_size": DiscreteOrdinal([64]),  # neural net batch size
        "max_text_length": DiscreteOrdinal([512]),  # text encoder length
        # for price_change_offset...
        # when aggregation is minutes, seconds ahead of current row for calculating percent changes
        # when aggregation is hours, rows ahead of current row for calculating percent changes
        # TODO fix this ^ nonsense. Go to rows only because it skips over closed market hours
        "max_window": DiscreteOrdinal(
            [24]  # range(24, 73)
        ),  # maximum row behind current row to see trends
        "num_windows": DiscreteOrdinal(
            [6]  # [5, 10, 15]
        ),  # number of points in time behind current row to compare for trends
        "num_news": DiscreteOrdinal(
            [0]
        ),  # number of news articles previous to the current row to include
        "news_history_threshold": ContinuousRange(
            24 * 60 * 60, 24 * 60 * 60 + 0.00000001  # 5 * 24 * 60 * 60
        ),  # The oldest a news article could be
        "target_offset": DiscreteOrdinal(
            [24]  # How far in the future we are predicting. "next_close" is an option
        ),
        "include_close_ratio": DiscreteOrdinal(
            [True]  # [True, False]
        ),  # include ratio of current close to past close
        "include_cv_close_ratio": DiscreteOrdinal(
            [True]  # [True, False]
        ),  # include coef var of close data from past to current
        "include_avg_volume_ratio": DiscreteOrdinal(
            [True]  # [True, False]
        ),  # include avg volume from past to current
        "include_cv_volume_ratio": DiscreteOrdinal(
            [False]  # [True, False]
        ),  # include coef car of volume from past to current
        "max_one_hot_categories": DiscreteOrdinal(
            [20]
        ),  # maximum number of categories/columns will be created per original column when one-hot encoding
        "n_hidden_layers": DiscreteOrdinal(
            [5]  # [1, 2, 3, 4, 5, 6, 7]
        ),  # number of hidden layers in NN
        "hidden_dim": DiscreteOrdinal(
            [250]  # [100, 250, 500, 750, 1000]
        ),  # number of nodes per hidden layer
        "dropout_rate": ContinuousRange(
            0.3, 0.30000000001  # 4
        ),  # chance of dropout per dropout layer in NN
        "missing_data_%_threshold": ContinuousRange(
            0, 0.000000001  # 0.5, 0.75
        ),  # threshold of % of missing data to remove pt.
    }
    save_load_funcs = {
        "model_state_dict": {"save": save_torch_state, "load": load_torch_state},
        "optimizer_state_dict": {"save": save_torch_state, "load": load_torch_state},
    }
    model_space = [
        {
            "name": "load_data",
            "train": {
                "func": load_data,
                "kwargs": {
                    "min_timestamp": min_timestamp,
                    "max_timestamp": max_timestamp,
                    "max_window": "max_window",
                    "num_windows": "num_windows",
                    "num_news": "num_news",
                    "news_history_threshold": "news_history_threshold",
                    "target_offset": "target_offset",
                    "include_close_ratio": "include_close_ratio",
                    "include_cv_close_ratio": "include_cv_close_ratio",
                    "include_avg_volume_ratio": "include_avg_volume_ratio",
                    "include_cv_volume_ratio": "include_cv_volume_ratio",
                },
                "outputs": ["structured_data", "text_data"],
            },
            "inference": {
                "func": load_data,
                "kwargs": {
                    "min_timestamp": -3600
                    * 24
                    * 14,  # TODO find a better way to do this
                    "max_window": "max_window",
                    "num_windows": "num_windows",
                    "num_news": "num_news",
                    "news_history_threshold": "news_history_threshold",
                    "include_target": False,
                    "include_close_ratio": "include_close_ratio",
                    "include_cv_close_ratio": "include_cv_close_ratio",
                    "include_avg_volume_ratio": "include_avg_volume_ratio",
                    "include_cv_volume_ratio": "include_cv_volume_ratio",
                    "keep_latest_only": True,
                },
                "outputs": ["structured_data", "text_data"],
            },
        },
        {
            "name": "filter_out_missing_data",
            "train": {
                "func": filter_out_missing_data,
                "args": ["structured_data", "missing_data_%_threshold"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp"],
                    "no_tolerance_cols": ["symbol", "timestamp", "target"],
                },
                "outputs": "structured_data",
            },
            "inference": {
                "func": filter_out_missing_data,
                "args": ["structured_data", "missing_data_%_threshold"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp"],
                    "no_tolerance_cols": ["symbol", "timestamp"],
                },
                "outputs": "structured_data",
            },
        },
        # {
        #     "name": "clip_values",
        #     "train": {
        #         "func": clip_values,
        #         "args": "structured_data",
        #         "kwargs": {
        #             "ignore_cols": [
        #                 "symbol",
        #                 "timestamp",
        #                 "hour",
        #                 "hour_cos",
        #                 "hour_sin",
        #             ]
        #         },
        #         "outputs": ["structured_data", "column_limits"],
        #     },
        #     "inference": {
        #         "func": clip_values,
        #         "args": "structured_data",
        #         "kwargs": {
        #             "ignore_cols": [
        #                 "symbol",
        #                 "timestamp",
        #                 "hour",
        #                 "hour_cos",
        #                 "hour_sin",
        #             ],
        #             "column_limits": "column_limits",
        #         },
        #         "outputs": ["structured_data", "column_limits"],
        #     },
        # },
        {
            "name": "scale_data",
            "train": {
                "func": scale_data,
                "args": ["structured_data"],
                "kwargs": {
                    "ignore_cols": [
                        "symbol",
                        "timestamp",
                        "hour_cos",
                        "hour_sin",
                    ]
                },
                "outputs": ["structured_data", "means", "stds"],
            },
            "inference": {
                "func": scale_data,
                "args": ["structured_data", "means", "stds"],
                "kwargs": {
                    "ignore_cols": [
                        "symbol",
                        "timestamp",
                        "hour_cos",
                        "hour_sin",
                    ]
                },
                "outputs": ["structured_data", "means", "stds"],
            },
        },
        {
            "name": "impute",
            "train": {
                "func": impute,
                "args": ["structured_data"],
                "kwargs": {"ignore_cols": ["target", "symbol", "timestamp"]},
                "outputs": ["structured_data", "imputer"],
            },
            "inference": {
                "func": impute,
                "args": ["structured_data", "imputer"],
                "kwargs": {"ignore_cols": ["symbol", "timestamp"]},
                "outputs": ["structured_data", "imputer"],
            },
        },
        {
            "name": "one_hot_encode",
            "train": {
                "func": one_hot_encode,
                "args": ["structured_data"],
                "kwargs": {
                    "ignore_cols": ["target", "symbol", "timestamp"],
                    "max_categories": "max_one_hot_categories",
                },
                "outputs": ["structured_data", "one_hot_encoder"],
            },
            "inference": {
                "func": one_hot_encode,
                "args": ["structured_data", "one_hot_encoder"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp"],
                    "max_categories": "max_one_hot_categories",
                },
                "outputs": ["structured_data", "one_hot_encoder"],
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
                "kwargs": {"split": 0.8},
                "outputs": ["train_dataset", "test_dataset"],
            },
            "inference": {
                "func": create_datasets,
                "args": [
                    "structured_data",
                    "text_data",
                    "M-FAC/bert-tiny-finetuned-mrpc",
                    "max_text_length",
                ],
                "kwargs": {"use_weights": False},
                "outputs": ["inference_dataset"],
            },
        },
        {
            "name": "get_structured_input_dim",
            "train": {
                "func": get_num_x_columns,
                "args": ["structured_data"],
                "kwargs": {"ignore_cols": ["symbol", "timestamp", "target"]},
                "outputs": "structured_input_dim",
            },
        },
        {
            "name": "create/load_model",
            "train": {
                "func": create_model,
                "args": [
                    "structured_input_dim",
                    "n_hidden_layers",
                    "hidden_dim",
                    "dropout_rate",
                    "num_news",
                    "M-FAC/bert-tiny-finetuned-mrpc",
                ],
                "outputs": ["model"],
                "run_in_parent_process": True,  # model is not pickleable
            },
            "inference": {
                "func": load_model,
                "args": [
                    "model_state_dict",
                    "optimizer_state_dict",
                    "epoch",
                    "structured_input_dim",
                    "n_hidden_layers",
                    "hidden_dim",
                    "dropout_rate",
                    "num_news",
                    "text_model_name",
                ],
                "output": ["model", "optimizer", "epoch"],
            },
        },
        {
            "name": "train/predict",
            "train": {
                "func": train_model,
                "args": [
                    "model",
                    "train_dataset",
                    "test_dataset",
                    "batch_size",
                    5,  # epochs
                    "num_news",
                ],
                "outputs": [
                    "model_state_dict",
                    "optimizer_state_dict",
                    "epoch",
                    "val_loss",
                    "predictions",
                ],
                "gpu": True,
            },
            "inference": {
                "func": infer,
                "args": ["model", "inference_dataset"],
                "outputs": "predictions",
            },
        },
        {
            "name": "unscale_data",
            "func": unscale_data,
            "args": ["predictions", "means", "stds"],
            "outputs": "predictions",
            "run_in_parent_process": True,  # TODO this step should not have to pickle model and pass to another process
            # TODO Also model shouldn't be pickled in the first place
        },
        {
            "name": "trading_policy",
            "train": {
                "func": train_trading_policy,
                "args": ["predictions"],
                "kwargs": {
                    "starting_cash": 100_000,
                    "flat_fee": 0.0,
                    "percent_fee": 0.0,
                },
                "outputs": ["policy", "score"],
                "run_in_parent_process": True,
            },
            "inference": {
                "func": apply_trading_policy,
                "args": ["predictions", "policy"],
                "outputs": ["recommendations"],
                "run_in_parent_process": True,
            },
        },
    ]
    return model_space, hyperparam_space, save_load_funcs


# TODO
#  save state dict of model as .pth file (would normally get treated as a dict. it's actually on ordered dict)
#  Keep symbol and timestamp (fix dataset and training logic)
#  Add filter to latest timestamp (and age cutoff) by symbol step to inference pipeline until a better solution is found
#  Fix flow when num_news > 0
#  Hyperparams for imputation
#  See DataCompiler for more to-do items
#  keep best version of nn based on val loss
#  pytorch model should not be pickled after training because it an inference function creates it


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
