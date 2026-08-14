import collections
import copy
import datetime
import logging
import os
from typing import Callable, Optional

_log = logging.getLogger("prediction.nn_model")

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
from tqdm import tqdm

from src.ml_diagnostics.checks import _compute_classification_metrics


def asymmetric_nll_loss(
    mu: torch.Tensor,
    target: torch.Tensor,
    var: torch.Tensor,
    negative_pair_weight: float = 0.1,
    false_positive_weight: float = 2.0,
) -> torch.Tensor:
    """Gaussian NLL with asymmetric weighting based on prediction/target sign.

    Since we never short stocks, the model only needs to know the sign of negative
    predictions — not their magnitude. We also upweight the worst trading mistake:
    predicting a gain on a stock that actually falls (false positive).

    Quadrant weights (target sign, prediction sign):
      (+, +): 1.0 — normal, rank correctly
      (+, -): 1.0 — missed opportunity, penalise normally
      (-, +): false_positive_weight — would buy a loser, most costly
      (-, -): negative_pair_weight — correct direction, magnitude doesn't matter

    Args:
        mu: Predicted mean, shape (N, 1).
        target: Actual target, shape (N, 1).
        var: Predicted variance, shape (N, 1).
        negative_pair_weight: Loss weight when both target and prediction are negative.
        false_positive_weight: Loss weight when target is negative but prediction is positive.

    Returns:
        Per-sample loss tensor of shape (N, 1).
    """
    loss = F.gaussian_nll_loss(mu, target, var, reduction="none")
    neg_target = target < 0
    false_positive_mask = neg_target & (mu >= 0)
    negative_pair_mask = neg_target & (mu < 0)

    # false_positive_weight > 1 must only amplify penalties, not rewards.
    # NLL can go negative (tight variance), and multiplying a negative loss by a
    # weight > 1 would make it more negative — rewarding a bad prediction.
    # Clamp to 0 before amplifying so the false-positive quadrant is always a cost.
    fp_loss = torch.clamp(loss, min=0) * false_positive_weight
    loss = torch.where(false_positive_mask, fp_loss, loss)
    loss = torch.where(negative_pair_mask, loss * negative_pair_weight, loss)
    return loss


def asymmetric_mse_loss(
    mu: torch.Tensor,
    target: torch.Tensor,
    var: torch.Tensor = None,  # unused; accepted only for call-site parity with asymmetric_nll_loss
    negative_pair_weight: float = 0.1,
    false_positive_weight: float = 2.0,
) -> torch.Tensor:
    """Plain MSE with the same asymmetric sign-based weighting as asymmetric_nll_loss.

    Used for the mean-only "warm-up" phase of two-phase training, where the
    uncertainty subnetwork is frozen and there's no variance to fit an NLL
    against. Gaussian NLL with a *fixed* variance is a constant plus MSE scaled
    by a constant, so minimizing this during warm-up is equivalent to
    fixed-variance NLL, matching the mean-only warm-up described in todo.txt.

    Args:
        mu: Predicted mean, shape (N, 1).
        target: Actual target, shape (N, 1).
        var: Unused (kept so this can be passed to the same call sites as
            asymmetric_nll_loss without branching on phase).
        negative_pair_weight: Loss weight when both target and prediction are negative.
        false_positive_weight: Loss weight when target is negative but prediction is positive.

    Returns:
        Per-sample loss tensor of shape (N, 1).
    """
    loss = F.mse_loss(mu, target, reduction="none")
    neg_target = target < 0
    false_positive_mask = neg_target & (mu >= 0)
    negative_pair_mask = neg_target & (mu < 0)
    # MSE is never negative, so unlike asymmetric_nll_loss no clamp is needed
    # before amplifying — there's no risk of making a negative loss "worse".
    loss = torch.where(false_positive_mask, loss * false_positive_weight, loss)
    loss = torch.where(negative_pair_mask, loss * negative_pair_weight, loss)
    return loss


def _build_mlp(
    in_dim: int,
    hidden_layers: list,
    out_dim: int = None,
    activation=nn.ReLU,
    dropout_rate: float = 0.0,
) -> nn.Module:
    """Build a small MLP, with two distinct "empty" behaviors depending on role.

    Args:
        in_dim: Input dimension.
        hidden_layers: Hidden layer widths, in order. Empty means passthrough
            (out_dim=None) or a single linear projection (out_dim set).
        out_dim: When None, this is a pre-trunk "feature subnet" — an empty
            config returns nn.Identity() (zero params, no state_dict keys).
            When set, this is a post-trunk "head" — an empty config returns a
            bare nn.Linear(in_dim, out_dim), matching a plain single-layer head.
            This distinction (Identity vs bare Linear) is deliberate: it's what
            lets a model built with all-empty subnet configs have byte-identical
            module structure/state_dict keys to a model with no subnets at all,
            so existing saved checkpoints keep loading under default config.
        activation: Activation class used between hidden layers.
        dropout_rate: Dropout probability after each hidden layer.

    Returns:
        nn.Identity, a bare nn.Linear, or an nn.Sequential MLP.
    """
    if not hidden_layers:
        return nn.Identity() if out_dim is None else nn.Linear(in_dim, out_dim)

    layers = []
    prev_dim = in_dim
    for width in hidden_layers:
        layers.append(nn.Linear(prev_dim, width))
        layers.append(activation())
        layers.append(nn.Dropout(dropout_rate))
        prev_dim = width
    if out_dim is not None:
        layers.append(nn.Linear(prev_dim, out_dim))
    return nn.Sequential(*layers)


def _final_linear(module: nn.Module) -> nn.Linear:
    """Return the last nn.Linear layer of a head, whether bare or an nn.Sequential."""
    return module if isinstance(module, nn.Linear) else module[-1]


class NumericalModel(nn.Module):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        output_dim: int = 1,  # output dim PER HEAD
        n_hidden_layers: int = 2,
        dropout_rate: float = 0.3,
        text_input_dim: int = 0,
        numeric_subnet_layers: list = None,
        text_subnet_layers: list = None,
        y_subnet_layers: list = None,
        uncertainty_subnet_layers: list = None,
    ):
        super().__init__()
        numeric_subnet_layers = numeric_subnet_layers or []
        text_subnet_layers = text_subnet_layers or []
        y_subnet_layers = y_subnet_layers or []
        uncertainty_subnet_layers = uncertainty_subnet_layers or []
        if text_input_dim == 0:
            # Guard against a degenerate Linear(0, x) first layer if the GA
            # ever mutates a nonzero text-subnet depth while num_news == 0.
            text_subnet_layers = []

        self.numeric_dim = input_dim - text_input_dim
        self.text_input_dim = text_input_dim

        self.numeric_subnet = _build_mlp(
            self.numeric_dim, numeric_subnet_layers, dropout_rate=dropout_rate
        )
        numeric_out_dim = numeric_subnet_layers[-1] if numeric_subnet_layers else self.numeric_dim

        self.text_subnet = _build_mlp(
            text_input_dim, text_subnet_layers, dropout_rate=dropout_rate
        )
        text_out_dim = text_subnet_layers[-1] if text_subnet_layers else text_input_dim

        trunk_input_dim = numeric_out_dim + text_out_dim

        layers = []
        for layer in range(n_hidden_layers):
            layer_input = trunk_input_dim if layer == 0 else hidden_dim
            layers.append(nn.Linear(layer_input, hidden_dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout_rate))
        self.fc = nn.Sequential(*layers)

        # Two separate heads: mean and variance
        self.mean_head = _build_mlp(hidden_dim, y_subnet_layers, out_dim=output_dim, dropout_rate=dropout_rate)
        self.variance_head = _build_mlp(
            hidden_dim, uncertainty_subnet_layers, out_dim=output_dim, dropout_rate=dropout_rate
        )

    def forward(self, x):
        h = self.numeric_subnet(x[:, : self.numeric_dim])
        if self.text_input_dim > 0:
            h = torch.cat([h, self.text_subnet(x[:, self.numeric_dim :])], dim=1)
        h = self.fc(h)
        mu = self.mean_head(h)
        var = self.variance_head(h)  # variance (pre-softplus)
        var = nn.functional.softplus(var)  # enforce that variance must be positive
        return mu, var


async def _run_validation(
    model: nn.Module,
    test_loader: DataLoader,
    loss_fn: nn.Module,
    device: torch.device,
    desc: str = "val",
    val_batches: int | None = None,
    pause_check: Optional[Callable] = None,
    loss_label: str = "NLL",
) -> tuple[float, pd.DataFrame]:
    """Run a validation pass over exactly val_batches batches (or all if None).

    Args:
        model: Model to evaluate.
        test_loader: Validation DataLoader (should use shuffle=True for sampling).
        loss_fn: Loss function.
        device: Torch device.
        desc: tqdm description prefix.
        val_batches: Exact number of batches to process. When set, every checkpoint
            sees the same sample count, making loss values directly comparable.
            When None, iterates the full loader.
        loss_label: Name to show in the log line for the reported loss value
            (e.g. "MSE(warmup)" during a mean-only warm-up phase, "NLL" otherwise).

    Returns:
        Tuple of (val_loss, predictions_df).
    """
    model.eval()
    val_loss = 0.0
    samples_seen = 0
    preds, variances, targets, symbols, timestamps, closes = [], [], [], [], [], []
    with torch.no_grad():
        for i, (x_batch, y_batch, weights, meta) in enumerate(tqdm(test_loader, desc=desc)):
            if val_batches is not None and i >= val_batches:
                break
            if pause_check is not None:
                await pause_check()
            x_batch = x_batch.to(device)
            y_batch = y_batch.to(device).unsqueeze(1)
            weights = weights.to(device)

            mu, var = model(x_batch)
            loss = loss_fn(mu, y_batch, var)
            val_loss += (loss * weights).sum().item()
            samples_seen += len(y_batch)

            preds.extend(mu.cpu().numpy().flatten())
            variances.extend(var.cpu().numpy().flatten())
            targets.extend(y_batch.cpu().numpy().flatten())
            symbols.extend(meta["symbol"])
            timestamps.extend(meta["timestamp"].numpy())
            closes.extend(meta["close"].numpy())

    val_loss /= samples_seen
    predictions = pd.DataFrame(
        {
            "symbol": symbols,
            "timestamp": timestamps,
            "target": targets,
            "prediction": preds,
            "variance": variances,
            "close": closes,
        }
    )
    mse = ((predictions["target"] - predictions["prediction"]) ** 2).mean()
    var_mean = float(np.mean(predictions["variance"]))
    var_p90 = float(np.percentile(predictions["variance"], 90))
    var_max = float(np.max(predictions["variance"]))
    optimal_var = mse / var_mean
    m = _compute_classification_metrics(predictions)
    _log.info(
        "%s=%.4f | MSE=%.4f | optimality=%.4f (ideal≈1) | "
        "var: mean=%.4e p90=%.4e max=%.4e | "
        "TP=%.1f%% FP=%.1f%% TN=%.1f%% FN=%.1f%% | "
        "precision=%.1f%% recall=%.1f%% accuracy=%.1f%% F1=%.3f",
        loss_label, val_loss, mse, optimal_var,
        var_mean, var_p90, var_max,
        m["true_positive_pct"] * 100, m["false_positive_pct"] * 100,
        m["true_negative_pct"] * 100, m["false_negative_pct"] * 100,
        m["precision"] * 100, m["recall"] * 100, m["accuracy"] * 100, m["f1_score"],
    )
    return val_loss, predictions


# --- Training loop for numerical-only model ---
async def train_numerical_model(
    model,
    train_loader,
    test_loader,
    device,
    epochs,
    optimizer,
    loss_fn,
    batches_before_validation: int | None = None,
    patience: int = 10,
    val_batches: int | None = None,
    val_smoothing_window: int = 3,
    pause_check: Optional[Callable] = None,
    loss_label: str = "NLL",
):
    # Validate once per epoch by default; caller can override to a fixed interval.
    if batches_before_validation is None:
        batches_before_validation = len(train_loader)

    # --- Initialization of variance_head ---
    # Harmless to redo when variance_head is frozen (e.g. during a warm-up
    # phase called via train_model) — it's a direct tensor write, not a
    # gradient-based update, and simply reinitializes to the same value.
    y_all = np.concatenate([y.numpy() for _, y, _, _ in train_loader], axis=0)
    init_var = np.var(y_all) + 1e-6
    with torch.no_grad():
        _final_linear(model.variance_head).bias.data.fill_(init_var)

    best_smoothed_loss = float("inf")
    best_state_dict = None
    best_optimizer_state_dict = None
    best_epoch = None
    best_predictions = None

    train_loss_history = []
    val_loss_history = []
    val_loss_window: collections.deque = collections.deque(maxlen=val_smoothing_window)

    global_step = 0
    patience_counter = 0
    stop_training = False

    for epoch in range(epochs):
        if stop_training:
            break
        if pause_check is not None:
            await pause_check()
        # --- Training ---
        model.train()  # signal to layers like dropout to act differently
        for x_batch, y_batch, weights, _ in tqdm(
            train_loader, desc=f"Epoch {epoch+1} [train]"
        ):
            global_step += 1
            x_batch = x_batch.to(device)
            y_batch = y_batch.to(device).unsqueeze(1)
            weights = weights.to(device)

            optimizer.zero_grad()
            mu, var = model(x_batch)
            loss = loss_fn(mu, y_batch, var)
            loss = (loss * weights).mean()
            train_loss_history.append(loss.item())
            loss.backward()
            optimizer.step()

            # --- Validation every N batches ---
            if global_step % batches_before_validation != 0:
                continue

            val_loss, predictions = await _run_validation(
                model, test_loader, loss_fn, device, desc=f"Epoch {epoch+1} [val]",
                val_batches=val_batches, pause_check=pause_check, loss_label=loss_label,
            )
            val_loss_history.append((global_step, val_loss))

            # --- Trailing-average best-model selection ---
            val_loss_window.append(val_loss)
            smoothed = sum(val_loss_window) / len(val_loss_window)
            if smoothed < best_smoothed_loss:
                best_smoothed_loss = smoothed
                best_state_dict = copy.deepcopy(model.state_dict())
                best_optimizer_state_dict = copy.deepcopy(optimizer.state_dict())
                best_epoch = epoch + 1
                best_predictions = predictions

                patience_counter = 0
                _log.info("New best model at step %d (smoothed=%.4f)", global_step, smoothed)
            else:
                patience_counter += 1
                _log.info(
                    "No improvement (%d/%d patience) smoothed=%.4f",
                    patience_counter, patience, smoothed,
                )

                if patience_counter >= patience:
                    _log.info(
                        "Early stopping at step %d (best epoch was %s)",
                        global_step, best_epoch,
                    )
                    stop_training = True
                    break

            model.train()  # switch back to training mode

    # If validation never fired, do a final pass now so we always return a valid checkpoint.
    if best_state_dict is None:
        _log.info("Validation never fired — running final validation pass.")
        val_loss, predictions = await _run_validation(
            model, test_loader, loss_fn, device, desc="final val", val_batches=val_batches,
            pause_check=pause_check, loss_label=loss_label,
        )
        best_smoothed_loss = val_loss
        best_state_dict = copy.deepcopy(model.state_dict())
        best_optimizer_state_dict = copy.deepcopy(optimizer.state_dict())
        best_epoch = epochs
        best_predictions = predictions
        val_loss_history.append((global_step, val_loss))

    # restore best weights + optimizer state
    model.load_state_dict(best_state_dict)
    optimizer.load_state_dict(best_optimizer_state_dict)

    return (
        best_state_dict,
        best_optimizer_state_dict,
        best_epoch,
        best_smoothed_loss,
        best_predictions,
        train_loss_history,
        val_loss_history,
    )


def save_model(model, model_folder: str = "models", model_name: str = None):
    if model_name is None:
        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        model_name = f"model_{dt}_{pid}.pth"
    model_path = os.path.join(model_folder, model_name)
    torch.save(model.state_dict(), model_path)
    return model_path


def create_model(
    structured_input_dim: int,
    n_hidden_layers: int,
    hidden_dim: int,
    dropout_rate: float,
    text_input_dim: int = 0,
    numeric_subnet_n_layers: int = 0,
    numeric_subnet_width_mult: float = 1.0,
    text_subnet_n_layers: int = 0,
    text_subnet_width_mult: float = 1.0,
    y_subnet_n_layers: int = 0,
    y_subnet_width_mult: float = 1.0,
    uncertainty_subnet_n_layers: int = 0,
    uncertainty_subnet_width_mult: float = 1.0,
) -> NumericalModel:
    """Create a NumericalModel with the given architecture.

    New parameters are all appended strictly after the original four —
    ezmt organisms saved before this feature existed replay their exact old
    positional args against this live function, so every new parameter must
    default to reproducing the original single-trunk, bare-Linear-head
    architecture unchanged.

    Each subnetwork's width is a multiplier of the dimension it's actually
    summarizing (numeric_input_dim, text_input_dim, or the trunk's hidden_dim
    for the two post-trunk heads) rather than an arbitrary absolute width, so
    subnet capacity scales with what it has to represent regardless of how
    num_news/embedding model/hidden_dim happen to be configured elsewhere.

    Args:
        structured_input_dim: Total input dimension (numerical + embedding dims).
        n_hidden_layers: Number of trunk hidden layers.
        hidden_dim: Width of each trunk hidden layer.
        dropout_rate: Dropout probability.
        text_input_dim: Portion of structured_input_dim that is news-embedding
            data (num_news * embedding_dim); 0 when there's no text/news input.
        numeric_subnet_n_layers: Depth of the pre-trunk numeric-feature subnet;
            0 = passthrough (today's behavior).
        numeric_subnet_width_mult: Numeric subnet hidden width as a multiple of
            (structured_input_dim - text_input_dim).
        text_subnet_n_layers: Depth of the pre-trunk news-embedding subnet;
            0 = passthrough.
        text_subnet_width_mult: Text subnet hidden width as a multiple of text_input_dim.
        y_subnet_n_layers: Depth of the mean-prediction head; 0 = bare nn.Linear
            (today's behavior).
        y_subnet_width_mult: y-subnet hidden width as a multiple of hidden_dim.
        uncertainty_subnet_n_layers: Depth of the variance-prediction head;
            0 = bare nn.Linear (today's behavior).
        uncertainty_subnet_width_mult: Uncertainty-subnet hidden width as a
            multiple of hidden_dim.

    Returns:
        Initialised NumericalModel.
    """
    numeric_dim = structured_input_dim - text_input_dim

    def _width(n_layers, mult, reference_dim):
        return max(1, round(mult * reference_dim)) if n_layers and reference_dim > 0 else 0

    numeric_width = _width(numeric_subnet_n_layers, numeric_subnet_width_mult, numeric_dim)
    text_width = _width(text_subnet_n_layers, text_subnet_width_mult, text_input_dim)
    y_width = _width(y_subnet_n_layers, y_subnet_width_mult, hidden_dim)
    uncertainty_width = _width(uncertainty_subnet_n_layers, uncertainty_subnet_width_mult, hidden_dim)

    return NumericalModel(
        structured_input_dim,
        hidden_dim,
        1,
        n_hidden_layers,
        dropout_rate=dropout_rate,
        text_input_dim=text_input_dim,
        numeric_subnet_layers=[numeric_width] * numeric_subnet_n_layers,
        text_subnet_layers=[text_width] * text_subnet_n_layers,
        y_subnet_layers=[y_width] * y_subnet_n_layers,
        uncertainty_subnet_layers=[uncertainty_width] * uncertainty_subnet_n_layers,
    )


async def train_model(
    model: NumericalModel,
    train_dataset,
    test_dataset,
    batch_size: int = 32,
    epochs: int = 10,
    lr: float = 1e-4,
    patience=10,
    batches_before_validation: int | None = None,
    negative_pair_weight: float = 0.1,
    false_positive_weight: float = 2.0,
    val_batches: int | None = None,
    val_smoothing_window: int = 3,
    pause_check: Optional[Callable] = None,
    warmup_max_epochs: int = 0,
    warmup_patience: int = 10,
) -> tuple:
    """Train ``model`` and return the best checkpoint.

    Args:
        model: NumericalModel to train.
        train_dataset: Training Dataset.
        test_dataset: Validation Dataset.
        batch_size: Mini-batch size.
        epochs: Maximum number of epochs for the main (NLL) training phase.
        lr: Adam learning rate.
        batches_before_validation: Validate every N batches (default: once per epoch).
        negative_pair_weight: Loss weight when both target and prediction are negative.
        false_positive_weight: Loss weight when target is negative but prediction positive.
        val_batches: Exact number of batches per validation pass. When set, the test
            loader is shuffled so each checkpoint sees a fresh random subset.
            When None, the full test set is used.
        val_smoothing_window: Number of recent val losses to average for best-model
            selection and early-stopping patience.
        warmup_max_epochs: When > 0, run a mean-only warm-up phase first: the
            uncertainty subnetwork's parameters are frozen and the model trains
            against asymmetric_mse_loss instead of asymmetric_nll_loss, for at
            most this many epochs — but the phase ends earlier, adaptively,
            whenever MSE plateaus for warmup_patience validations (reusing
            train_numerical_model's own early-stopping, not a fixed duration).
            0 (default) skips warm-up entirely, matching prior behavior.
            Addresses the "variance attenuation" pathology of heteroscedastic
            NLL regression, where the network can cheaply reduce loss by
            inflating predicted variance instead of improving the mean —
            described in this project's todo.txt as the standard fix.
        warmup_patience: Early-stopping patience for the warm-up phase.

    Returns:
        8-tuple: (model_state_dict, optimizer_state_dict, epoch, smoothed_val_loss,
                  predictions, train_loss_history, val_loss_history, warmup_mse) —
                  the first 7 are from the main (NLL) phase; the warm-up phase's
                  checkpoint itself is never returned (a model with an untrained
                  uncertainty head should never be the one that ends up saved),
                  but its best smoothed loss is, for tracking/comparison.
                  warmup_mse is None when warmup_max_epochs is 0.
    """
    import functools

    nll_loss_fn = functools.partial(
        asymmetric_nll_loss,
        negative_pair_weight=negative_pair_weight,
        false_positive_weight=false_positive_weight,
    )
    mse_loss_fn = functools.partial(
        asymmetric_mse_loss,
        negative_pair_weight=negative_pair_weight,
        false_positive_weight=false_positive_weight,
    )
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    shuffle_test = val_batches is not None
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=shuffle_test)
    model = model.to(device)
    # Built once, before any freezing — Adam lazily initializes per-parameter
    # moment state the first time it sees a non-None grad, not at construction
    # time, so a frozen-then-unfrozen parameter picks up fresh state exactly
    # when it starts training again. No optimizer rebuild needed between phases.
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    warmup_mse = None
    if warmup_max_epochs > 0:
        for p in model.variance_head.parameters():
            p.requires_grad_(False)
        warmup_result = await train_numerical_model(
            model, train_loader, test_loader, device, warmup_max_epochs, optimizer, mse_loss_fn,
            batches_before_validation, patience=warmup_patience, val_batches=val_batches,
            val_smoothing_window=val_smoothing_window, pause_check=pause_check,
            loss_label="MSE(warmup)",
        )
        warmup_mse = warmup_result[3]  # best_smoothed_loss
        for p in model.variance_head.parameters():
            p.requires_grad_(True)

    final_result = await train_numerical_model(
        model, train_loader, test_loader, device, epochs, optimizer, nll_loss_fn,
        batches_before_validation, patience=patience, val_batches=val_batches,
        val_smoothing_window=val_smoothing_window, pause_check=pause_check,
        loss_label="NLL",
    )
    return (*final_result, warmup_mse)


_CURRENT_MODEL_VERSION = 1


def load_model(
    model_state: dict,
    optimizer_state: dict,
    epoch: int,
    structured_input_dim: int,
    n_hidden_layers: int,
    hidden_dim: int,
    dropout_rate: float,
    text_input_dim: int = 0,
    numeric_subnet_n_layers: int = 0,
    numeric_subnet_width_mult: float = 1.0,
    text_subnet_n_layers: int = 0,
    text_subnet_width_mult: float = 1.0,
    y_subnet_n_layers: int = 0,
    y_subnet_width_mult: float = 1.0,
    uncertainty_subnet_n_layers: int = 0,
    uncertainty_subnet_width_mult: float = 1.0,
    model_version: int = 1,
    lr: float = 1e-4,
) -> tuple:
    """Recreate a NumericalModel and restore saved weights.

    Args:
        model_state: state_dict from a previous training run.
        optimizer_state: optimizer state_dict from a previous training run.
        epoch: Last training epoch (passed through unchanged).
        structured_input_dim: Model input dimension.
        n_hidden_layers: Number of hidden layers.
        hidden_dim: Width of each hidden layer.
        dropout_rate: Dropout probability.
        text_input_dim, numeric_subnet_*, text_subnet_*, y_subnet_*,
            uncertainty_subnet_*: see create_model — must match what the
            checkpoint was trained with.
        model_version: Architecture version the checkpoint was saved under.
            Checked against this module's current version so a mismatch raises
            a clear error instead of a cryptic state_dict key-mismatch trace.
        lr: Adam learning rate.

    Returns:
        Tuple of (model, optimizer, epoch).
    """
    if model_version != _CURRENT_MODEL_VERSION:
        raise ValueError(
            f"Checkpoint was saved under model_version={model_version}, but this "
            f"code is at model_version={_CURRENT_MODEL_VERSION}. The architecture "
            "may have changed incompatibly — retrain instead of loading."
        )
    model = create_model(
        structured_input_dim, n_hidden_layers, hidden_dim, dropout_rate,
        text_input_dim=text_input_dim,
        numeric_subnet_n_layers=numeric_subnet_n_layers,
        numeric_subnet_width_mult=numeric_subnet_width_mult,
        text_subnet_n_layers=text_subnet_n_layers,
        text_subnet_width_mult=text_subnet_width_mult,
        y_subnet_n_layers=y_subnet_n_layers,
        y_subnet_width_mult=y_subnet_width_mult,
        uncertainty_subnet_n_layers=uncertainty_subnet_n_layers,
        uncertainty_subnet_width_mult=uncertainty_subnet_width_mult,
    )
    model.load_state_dict(model_state)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    optimizer.load_state_dict(optimizer_state)
    return model, optimizer, epoch


def infer(model, dataset):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    model.eval()

    data_loader = DataLoader(dataset, batch_size=32, shuffle=False)

    preds, variances, symbols, timestamps, closes = [], [], [], [], []
    with torch.no_grad():
        for x_batch, _, _weights, meta in tqdm(data_loader, desc="Running inference"):
            x_batch = x_batch.to(device)
            meta = {
                k: v.numpy() if isinstance(v, torch.Tensor) else v
                for k, v in meta.items()
            }
            mu, var = model(x_batch)

            preds.extend(mu.cpu().numpy().flatten())
            variances.extend(var.cpu().numpy().flatten())
            symbols.extend(meta["symbol"])
            timestamps.extend(meta["timestamp"])
            closes.extend(meta["close"])

    predictions = pd.DataFrame(
        {
            "symbol": symbols,
            "timestamp": timestamps,
            "prediction": preds,
            "variance": variances,
            "close": closes,
        }
    )

    return predictions
