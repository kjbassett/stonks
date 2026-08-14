import logging
import matplotlib.pyplot as plt
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal
from ezmt.model_tuner import ModelTuner
from ezmt.organism import dna2str
from typing import Union

_log = logging.getLogger("prediction.genetic_algorithm")
from stonks.src.plot import plot_training
from src.data_access.dao_manager import dao_manager
from src.prediction.dataset import create_datasets
from src.prediction.nn_model import create_model, train_model, load_model, infer
from src.ml_diagnostics.checks import run_data_quality_checks
from src.prediction.pipeline_components import (
    clip_values,
    drop_near_zero,
    filter_out_missing_data,
    split_data,
    scale_data,
    one_hot_encode,
    impute,
    get_num_x_columns,
    get_text_input_dim,
    unscale_data,
    load_data,
    save_torch_state,
    load_torch_state,
)
from src.trading.trading_engine import train_trading_policy, apply_trading_policy
from webrock.decorator import plugin


def _parse_log_states(value: str) -> Union[bool, int, list]:
    """Parse a string form value into the type accepted by resolve_log_states.

    Args:
        value: "true"/"all" to log all states, "false"/"none"/empty to log
            none, a single integer string, or comma-separated integers.

    Returns:
        True, False, a single int, or a list of ints.
    """
    stripped = value.strip().lower()
    if stripped in ("true", "all"):
        return True
    if stripped in ("false", "none", ""):
        return False
    parts = [p.strip() for p in stripped.split(",") if p.strip()]
    if len(parts) == 1:
        return int(parts[0])
    return [int(p) for p in parts]


@plugin()
async def run_genetic_algorithm(
    run_name: str,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    log_states: str = "false",
    organisms_dir: str = "H:/organisms",
    notes: str = None,
):
    # define possible choices for all hyperparameters
    model_space, hyperparam_space, save_load_funcs = create_model_space(
        max_timestamp, min_timestamp
    )
    # Run genetic algorithm to tune hyperparameters
    mt = ModelTuner(
        model_space, hyperparam_space, save_load_funcs, None, "target", 1, 1,
        directory=organisms_dir,
    )
    model = await mt.run(run_name, log_states=_parse_log_states(log_states))
    model.save()
    # ModelTuner.score_fitness populates both model.score and model.fitness.
    await log_organism_version(model, fitness=model.fitness, notes=notes)


@plugin()
async def run_short_genetic_algorithm(
    source_name: str,
    source_version: str = "latest",
    start_after_gene_index: str | int | None = None,
    new_name: str | None = None,
    new_version: str | None = None,
    log_states: str = "false",
    recreate_dna: bool = False,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    organisms_dir: str = "H:/organisms",
    notes: str = None,
):
    from ezmt.organism import Organism
    if isinstance(start_after_gene_index, str) and start_after_gene_index.isnumeric():
        start_after_gene_index = int(start_after_gene_index)

    model = Organism.load(source_name, source_version, gene_index=start_after_gene_index, directory=organisms_dir)
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
    result = await model.run(
        mode="train",
        log_states=_parse_log_states(log_states),
        result_name="score",
        update_knowledge=True,
    )
    # model.run() only returns the score; it never updates model.score itself
    # (that only happens via ModelTuner.score_fitness, which this path bypasses).
    # Setting it here makes model.score authoritative from this point on, correct
    # regardless of when/whether the organism is saved.
    model.score = result
    model.save()
    # No ModelTuner involved on this path, so there's no population to compute a
    # relative fitness against — log None rather than the object's stale default (0).
    await log_organism_version(model, fitness=None, notes=notes)
    _log.info("GA run complete: %s", result)


@plugin()
async def backfill_model_version(
    name: str, version: str, organisms_dir: str = "H:/organisms", notes: str = None
) -> int:
    """Retroactively log an on-disk organism version to Model.

    For organism versions saved before the Model-logging feature existed (or
    otherwise missing their Model row) — loads dna/parameters/knowledge
    straight from the organism's saved folder and logs them via
    log_organism_version.

    Args:
        name: Organism name.
        version: Exact organism version folder name (not "latest").
        organisms_dir: Root organisms folder.
        notes: Optional free-text note; defaults to a note explaining the backfill.

    Returns:
        The new Model row's id.
    """
    from ezmt.organism import Organism

    model = Organism.load(name, version, directory=organisms_dir)
    # load() always resets score/fitness to 0 regardless of the saved version's
    # real history — that's not recoverable from disk, so store it as unknown
    # (None) rather than the misleading fixed 0.
    model.score = None
    if notes is None:
        notes = "Backfilled from on-disk organism data; predates Model-logging feature."
    return await log_organism_version(model, fitness=None, notes=notes)


async def log_organism_version(model, fitness: float | None, notes: str = None) -> int:
    """Persist a saved organism's score/hyperparameters/diff to the Model table.

    Args:
        model: The Organism that was just saved (model.save() must already
            have run so model.folder reflects the final version).
        fitness: Population-relative fitness, or None when this run didn't go
            through ModelTuner (model.fitness would otherwise be a stale
            uninitialized 0, not a real "not applicable" signal).
        notes: Optional free-text note to attach to this version.

    Returns:
        The new Model row's id.
    """
    resolved_version = model.folder.rsplit("/", 1)[-1]
    knowledge = model.knowledge or {}

    # data_quality_metrics: properties of the *input data* (feature ranges,
    # outliers, loss spikes). classification_metrics is about how well the
    # *model* performs, so it moves to model_performance instead.
    data_quality_metrics = dict(knowledge.get("data_quality_report") or {})
    classification_metrics = data_quality_metrics.pop("classification_metrics", None)

    predictions_df = knowledge.get("predictions")
    final_mse = (
        float(((predictions_df["target"] - predictions_df["prediction"]) ** 2).mean())
        if predictions_df is not None and not predictions_df.empty
        else None
    )
    model_performance = {
        "classification_metrics": classification_metrics,
        "final_mse": final_mse,
        "warmup_mse": knowledge.get("warmup_mse"),
        "final_nll": knowledge.get("val_loss"),
    }

    return await dao_manager.get_dao("Model").log_version(
        model.name,
        resolved_version,
        score=model.score,
        fitness=fitness,
        parameters=model.parameters,
        dna_summary=dna2str(model.dna),
        data_quality_metrics=data_quality_metrics or None,
        model_performance=model_performance,
        notes=notes,
    )


def save_figure(folder, key, fig):
    path = f"{folder}/{key}.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def create_model_space(max_timestamp, min_timestamp):
    hyperparam_space = {
        "batch_size": DiscreteOrdinal([64]),  # neural net batch size
        "embedding_model_name": DiscreteOrdinal(["all-MiniLM-L6-v2"]),  # sentence-transformer model for news
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
            [1]
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
            [8]  # [1, 2, 3, 4, 5, 6, 7]
        ),  # number of hidden layers in NN
        "hidden_dim": DiscreteOrdinal(
            [750]  # [100, 250, 500, 750, 1000]
        ),  # number of nodes per hidden layer
        "dropout_rate": ContinuousRange(
            0.3, 0.30000000001  # 4
        ),  # chance of dropout per dropout layer in NN
        "learning_rate": ContinuousRange(1e-5, 1e-5 + 1e-10),
        "missing_data_%_threshold": ContinuousRange(
            0.1, 0.10000000001  # 0.05, 0.2
        ),  # threshold of % of missing data to remove pt.
        "patience": DiscreteOrdinal(
            [20]
        ),
        "max_drop_prob": ContinuousRange(
            0, 0.000000001  # 0, 0.6
        ),  # probability of dropping a target=0 row (decays to 0 by zero_width); 0 = disabled
        "zero_width": ContinuousRange(
            0.01, 0.01000001  # 0.001, 0.05
        ),  # |target| distance at which drop probability reaches 0
        "negative_pair_weight": ContinuousRange(
            1.0, 1.00000001
        ),  # loss weight when both target and prediction are negative (0 = ignore magnitude)
        "false_positive_weight": ContinuousRange(
            1.0, 1.00000001
        ),  # loss weight when target is negative but prediction is positive (buying a loser)
        # --- Pre-trunk subnetworks (0 layers = passthrough, today's behavior) ---
        "numeric_subnet_n_layers": DiscreteOrdinal([3]),  # [0, 1, 2, 3]
        "numeric_subnet_width_mult": ContinuousRange(
            1.0, 1.00000001  # 0.25, 2.0
        ),  # numeric subnet hidden width as a multiple of its own input dim
        "text_subnet_n_layers": DiscreteOrdinal([3]),  # [0, 1, 2, 3]
        "text_subnet_width_mult": ContinuousRange(
            1.0, 1.00000001  # 0.25, 2.0
        ),  # text subnet hidden width as a multiple of text_input_dim
        # --- Post-trunk heads (0 layers = bare nn.Linear, today's behavior) ---
        "y_subnet_n_layers": DiscreteOrdinal([3]),  # [0, 1, 2]
        "y_subnet_width_mult": ContinuousRange(
            1, 1.00000001  # 0.25, 2.0
        ),  # y-subnet hidden width as a multiple of hidden_dim
        "uncertainty_subnet_n_layers": DiscreteOrdinal([3]),  # [0, 1, 2]
        "uncertainty_subnet_width_mult": ContinuousRange(
            1, 1.00000001  # 0.25, 2.0
        ),  # uncertainty-subnet hidden width as a multiple of hidden_dim
        # --- Two-phase (mean warm-up) training ---
        "warmup_max_epochs": DiscreteOrdinal(
            [10]  # [0, 3, 5, 8]
        ),  # cap on mean-only warm-up epochs; 0 = today's single-phase training.
        # Warm-up ends adaptively (via warmup_patience) whenever MSE plateaus,
        # not necessarily after running this many epochs.
        "warmup_patience": DiscreteOrdinal([10]),  # [3, 5, 8]
    }
    save_load_funcs = {
        "model_state_dict": {"save": save_torch_state, "load": load_torch_state},
        "optimizer_state_dict": {"save": save_torch_state, "load": load_torch_state},
        "scatter_figure": {"save": save_figure},
        "pred_hist_figure": {"save": save_figure},
        "loss_history_figure": {"save": save_figure},
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
                    "embedding_model_name": "embedding_model_name",
                    "include_after_hours": False,
                },
                "outputs": ["structured_data", "embedding_lookup", "raw_price_data"],
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
                    "embedding_model_name": "embedding_model_name",
                    "include_after_hours": False,
                },
                "outputs": ["structured_data", "embedding_lookup", "raw_price_data"],
            },
        },
        {
            "name": "drop_near_zero",
            "train": {
                "func": drop_near_zero,
                "args": ["structured_data"],
                "kwargs": {
                    "max_drop_prob": "max_drop_prob",
                    "zero_width": "zero_width",
                },
                "outputs": "structured_data",
            },
            # inference is never target-filtered — no target column to drop by
        },
        {
            "name": "filter_out_missing_data",
            "train": {
                "func": filter_out_missing_data,
                "args": ["structured_data", "missing_data_%_threshold"],
                "kwargs": {
                    # news1_* coupled to num_news being pinned to 1 (DiscreteOrdinal([1])
                    # above) — a missing article in the trailing news_history_threshold
                    # is normal/expected, not a data-quality problem, so it shouldn't
                    # count against a row's missing-data ratio. Revisit if num_news is
                    # ever unpinned to explore values > 1.
                    "ignore_cols": ["symbol", "timestamp", "news1_id", "news1_sentiment", "news1_age"],
                    "no_tolerance_cols": ["symbol", "timestamp", "target"],
                },
                "outputs": "structured_data",
            },
            "inference": {
                "func": filter_out_missing_data,
                "args": ["structured_data", "missing_data_%_threshold"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp", "news1_id", "news1_sentiment", "news1_age"],
                    "no_tolerance_cols": ["symbol", "timestamp"],
                },
                "outputs": "structured_data",
            },
        },
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
            "name": "clip_values",
            "train": {
                "func": clip_values,
                "args": ["train_data"],
                "kwargs": {
                    "test_df": "test_data",
                    "ignore_cols": ["symbol", "timestamp", "hour_cos", "hour_sin"],
                },
                "outputs": ["train_data", "test_data", "column_limits"],
            },
            "inference": {
                "func": clip_values,
                "args": ["structured_data"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp", "hour_cos", "hour_sin"],
                    "column_limits": "column_limits",
                },
                "outputs": ["structured_data", "_", "column_limits"],
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
                "args": ["train_data", "embedding_lookup"],
                "kwargs": {"test_data": "test_data"},
                "outputs": ["train_dataset", "test_dataset", "validation_dataset"],
            },
            "inference": {
                "func": create_datasets,
                "args": ["structured_data", "embedding_lookup"],
                "kwargs": {"use_weights": False},
                "outputs": ["inference_dataset"],
            },
        },
        {
            "name": "get_structured_input_dim",
            "train": {
                "func": get_num_x_columns,
                "args": ["train_data"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp", "target"],
                    "num_news": "num_news",
                    "embedding_lookup": "embedding_lookup",
                },
                "outputs": "structured_input_dim",
            },
        },
        {
            "name": "get_text_input_dim",
            "train": {
                "func": get_text_input_dim,
                "args": ["train_data"],
                "kwargs": {
                    "num_news": "num_news",
                    "embedding_lookup": "embedding_lookup",
                },
                "outputs": ["text_input_dim"],
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
                ],
                "kwargs": {
                    "text_input_dim": "text_input_dim",
                    "numeric_subnet_n_layers": "numeric_subnet_n_layers",
                    "numeric_subnet_width_mult": "numeric_subnet_width_mult",
                    "text_subnet_n_layers": "text_subnet_n_layers",
                    "text_subnet_width_mult": "text_subnet_width_mult",
                    "y_subnet_n_layers": "y_subnet_n_layers",
                    "y_subnet_width_mult": "y_subnet_width_mult",
                    "uncertainty_subnet_n_layers": "uncertainty_subnet_n_layers",
                    "uncertainty_subnet_width_mult": "uncertainty_subnet_width_mult",
                },
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
                ],
                "kwargs": {
                    "text_input_dim": "text_input_dim",
                    "numeric_subnet_n_layers": "numeric_subnet_n_layers",
                    "numeric_subnet_width_mult": "numeric_subnet_width_mult",
                    "text_subnet_n_layers": "text_subnet_n_layers",
                    "text_subnet_width_mult": "text_subnet_width_mult",
                    "y_subnet_n_layers": "y_subnet_n_layers",
                    "y_subnet_width_mult": "y_subnet_width_mult",
                    "uncertainty_subnet_n_layers": "uncertainty_subnet_n_layers",
                    "uncertainty_subnet_width_mult": "uncertainty_subnet_width_mult",
                },
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
                ],
                "kwargs": {
                    "lr": "learning_rate",
                    "patience": "patience",
                    "batches_before_validation": 250,
                    "val_batches": 50,
                    "negative_pair_weight": "negative_pair_weight",
                    "false_positive_weight": "false_positive_weight",
                    "warmup_max_epochs": "warmup_max_epochs",
                    "warmup_patience": "warmup_patience",
                },
                "outputs": [
                    "model_state_dict",
                    "optimizer_state_dict",
                    "epoch",
                    "val_loss",
                    "predictions",
                    "train_loss_history",
                    "val_loss_history",
                    "warmup_mse",
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
            "name": "plot_training",
            "train": {
                "func": plot_training,
                "args": ["predictions", "train_loss_history", "val_loss_history"],
                "outputs": ["scatter_figure", "pred_hist_figure", "loss_history_figure"],
                "run_in_parent_process": True,
            },
        },
        {
            "name": "check_data_quality",
            "train": {
                "func": run_data_quality_checks,
                "args": ["train_data", "train_loss_history", "val_loss_history"],
                "kwargs": {
                    "ignore_cols": ["symbol", "timestamp", "target"],
                    "predictions": "predictions",
                },
                "outputs": ["data_quality_report"],
                "run_in_parent_process": True,
            },
            "inference": None,
        },
        {
            "name": "infer_validation",
            "train": {
                "func": infer,
                "args": ["model", "validation_dataset"],
                "outputs": "validation_predictions",
                "run_in_parent_process": True,
            },
        },
        {
            "name": "unscale_validation",
            "train": {
                "func": unscale_data,
                "args": ["validation_predictions", "means", "stds"],
                "outputs": "validation_predictions",
                "run_in_parent_process": True,
            },
        },
        # {
        #     "name": "score_placeholder",
        #     "func": lambda x: 1,
        #     "outputs": "score"
        # }
        {
            "name": "trading_policy",
            "train": {
                "func": train_trading_policy,
                "args": ["validation_predictions", "raw_price_data"],
                "kwargs": {
                    "rebalance_interval_hours": 1,
                    "allow_intraday":False,
                    "stop_loss_pct": 0.1,
                },
                "outputs": ["policy", "score", "trade_log"],
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
#  Hyperparams for imputation
#  See DataCompiler for more to-do items
#  pytorch model should not be pickled after training because an inference function creates it
#  log states should save entire model, but use should be able to choose after which steps

