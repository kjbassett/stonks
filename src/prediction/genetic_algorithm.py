import matplotlib.pyplot as plt
import pandas as pd
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal
from ezmt.model_tuner import ModelTuner
from src.prediction.dataset import create_datasets
from src.prediction.nn_model import create_model, train_model, load_model, infer
from src.prediction.pipeline_components import (
    filter_out_missing_data,
    split_data,
    scale_data,
    one_hot_encode,
    impute,
    get_num_x_columns,
    unscale_data,
    load_data,
    save_torch_state,
    load_torch_state,
)
from src.trading.trading_engine import train_trading_policy, apply_trading_policy
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
    start_after_gene_index: str = None,
    new_name: str = None,
    new_version: str = None,
    log_states: bool = False,
    recreate_dna: bool = False,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
):
    from ezmt.organism import Organism

    model = Organism.load(source_name, source_version, gene_index=start_after_gene_index)
    model.new_version(name=new_name, version=new_version)
    if recreate_dna:
        from ezmt.model_tuner import choose_dna, validate_config, choose_hyperparams

        model_space, hyperparam_space, _ = create_model_space(
            max_timestamp, min_timestamp
        )
        model_space = validate_config(model_space, hyperparam_space)
        dna = choose_dna(model_space)
        hyperparams = choose_hyperparams(hyperparam_space)
        model.dna = dna
        model.parameters = hyperparams
    result = await model.run(mode="train", log_states=log_states, result_name="score")
    model.save()
    print(result)


def create_model_space(max_timestamp, min_timestamp):
    hyperparam_space = {
        "batch_size": DiscreteOrdinal([64]),  # neural net batch size
        "text_model_name": DiscreteOrdinal(["M-FAC/bert-tiny-finetuned-mrpc"]),  # HuggingFace tokenizer/encoder
        "max_text_length": DiscreteOrdinal([512]),  # text encoder length
        # for price_change_offset...
        # when aggregation is minutes, seconds ahead of current row for calculating percent changes
        # when aggregation is hours, rows ahead of current row for calculating percent changes
        # TODO fix this ^ nonsense. Go to rows only because it skips over closed market hours
        "max_window": DiscreteOrdinal(
            [24]  # range(24, 73)
        ),  # maximum row behind current row to see trends
        "num_windows": DiscreteOrdinal(
            [12]  # [5, 10, 15]
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
            [True]  # [True, False]
        ),  # include coef car of volume from past to current
        "max_one_hot_categories": DiscreteOrdinal(
            [20]
        ),  # maximum number of categories/columns will be created per original column when one-hot encoding
        "n_hidden_layers": DiscreteOrdinal(
            [7]  # [1, 2, 3, 4, 5, 6, 7]
        ),  # number of hidden layers in NN
        "hidden_dim": DiscreteOrdinal(
            [500]  # [100, 250, 500, 750, 1000]
        ),  # number of nodes per hidden layer
        "dropout_rate": ContinuousRange(
            0.3, 0.30000000001  # 4
        ),  # chance of dropout per dropout layer in NN
        "missing_data_%_threshold": ContinuousRange(
            0, 0.000000001  # 0.5, 0.75
        ),  # threshold of % of missing data to remove pt.
        "negative_pair_weight": ContinuousRange(
            0.2, 0.20000001
        ),  # loss weight when both target and prediction are negative (0 = ignore magnitude)
        "false_positive_weight": ContinuousRange(
            1.5, 1.50000001
        ),  # loss weight when target is negative but prediction is positive (buying a loser)
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
                "outputs": ["structured_data", "text_data", "raw_price_data"],
            },
            "inference": {
                "func": load_data,
                "kwargs": {
                    "min_timestamp": -3600 * 24 * 14,
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
                "outputs": ["structured_data", "text_data", "raw_price_data"],
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
            "name": "split_data",
            "train": {
                "func": split_data,
                "args": ["structured_data"],
                "kwargs": {"split": 0.8},
                "outputs": ["train_data", "test_data"],
            },
        },
        {
            "name": "scale_data",
            "train": {
                "func": scale_data,
                "args": ["train_data"],
                "kwargs": {
                    "test_df": "test_data",
                    "ignore_cols": [
                        "symbol",
                        "timestamp",
                        "hour_cos",
                        "hour_sin",
                    ],
                },
                "outputs": ["train_data", "test_data", "means", "stds"],
            },
            "inference": {
                "func": scale_data,
                "args": ["structured_data"],
                "kwargs": {
                    "means": "means",
                    "stds": "stds",
                    "ignore_cols": [
                        "symbol",
                        "timestamp",
                        "hour_cos",
                        "hour_sin",
                    ],
                },
                "outputs": ["structured_data", "_", "means", "stds"],
            },
        },
        {
            "name": "impute",
            "train": {
                "func": impute,
                "args": ["train_data"],
                "kwargs": {
                    "test_df": "test_data",
                    "ignore_cols": ["target", "symbol", "timestamp"],
                },
                "outputs": ["train_data", "test_data", "imputer"],
            },
            "inference": {
                "func": impute,
                "args": ["structured_data"],
                "kwargs": {
                    "imputer": "imputer",
                    "ignore_cols": ["symbol", "timestamp"],
                },
                "outputs": ["structured_data", "_", "imputer"],
            },
        },
        {
            "name": "one_hot_encode",
            "train": {
                "func": one_hot_encode,
                "args": ["train_data"],
                "kwargs": {
                    "test_df": "test_data",
                    "ignore_cols": ["target", "symbol", "timestamp"],
                    "max_categories": "max_one_hot_categories",
                },
                "outputs": ["train_data", "test_data", "one_hot_encoder"],
            },
            "inference": {
                "func": one_hot_encode,
                "args": ["structured_data"],
                "kwargs": {
                    "encoder": "one_hot_encoder",
                    "ignore_cols": ["symbol", "timestamp"],
                    "max_categories": "max_one_hot_categories",
                },
                "outputs": ["structured_data", "_", "one_hot_encoder"],
            },
        },
        {
            "name": "create_datasets",
            "train": {
                "func": create_datasets,
                "args": [
                    "train_data",
                    "text_data",
                    "text_model_name",
                    "max_text_length",
                ],
                "kwargs": {"test_data": "test_data"},
                "outputs": ["train_dataset", "test_dataset"],
            },
            "inference": {
                "func": create_datasets,
                "args": [
                    "structured_data",
                    "text_data",
                    "text_model_name",
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
                "args": ["train_data"],
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
                    "text_model_name",
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
                "outputs": ["model", "optimizer", "epoch"],
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
                    20,  # epochs
                    "num_news",
                ],
                "kwargs": {
                    "batches_before_validation": 1000,
                    "negative_pair_weight": "negative_pair_weight",
                    "false_positive_weight": "false_positive_weight",
                },
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
                "args": ["predictions", "raw_price_data"],
                "kwargs": {
                    "rebalance_interval_hours": 1,
                    "allow_intraday":False,
                    "stop_loss_pct": 0.1,
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
#  Fix flow when num_news > 0
#  Hyperparams for imputation
#  See DataCompiler for more to-do items
#  pytorch model should not be pickled after training because an inference function creates it
#  log states should save entire model, but use should be able to choose after which steps

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
