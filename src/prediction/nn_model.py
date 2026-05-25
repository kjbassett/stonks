import copy
import datetime
import os

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


class NumericalModel(nn.Module):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        output_dim: int = 1,  # output dim PER HEAD
        n_hidden_layers: int = 2,
        dropout_rate: float = 0.3,
    ):
        super().__init__()
        layers = []
        for layer in range(n_hidden_layers):
            layer_input = input_dim if layer == 0 else hidden_dim
            layers.append(nn.Linear(layer_input, hidden_dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout_rate))
        self.fc = nn.Sequential(*layers)

        # Two separate heads: mean and variance
        self.mean_head = nn.Linear(hidden_dim, output_dim)
        self.variance_head = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        h = self.fc(x)
        mu = self.mean_head(h)
        var = self.variance_head(h)  # variance (pre-softplus)
        var = nn.functional.softplus(var)  # enforce that variance must be positive
        return mu, var


def _run_validation(
    model: nn.Module,
    test_loader: DataLoader,
    loss_fn: nn.Module,
    device: torch.device,
    desc: str = "val",
) -> tuple[float, pd.DataFrame]:
    """Run one full validation pass. Returns (val_loss, predictions_df)."""
    model.eval()
    val_loss = 0.0
    preds, variances, targets, symbols, timestamps, closes = [], [], [], [], [], []
    with torch.no_grad():
        for x_batch, y_batch, weights, meta in tqdm(test_loader, desc=desc):
            x_batch = x_batch.to(device)
            y_batch = y_batch.to(device).unsqueeze(1)
            weights = weights.to(device)

            mu, var = model(x_batch)
            loss = loss_fn(mu, y_batch, var)
            val_loss += (loss * weights).sum().item()

            preds.extend(mu.cpu().numpy().flatten())
            variances.extend(var.cpu().numpy().flatten())
            targets.extend(y_batch.cpu().numpy().flatten())
            symbols.extend(meta["symbol"])
            timestamps.extend(meta["timestamp"].numpy())
            closes.extend(meta["close"].numpy())

    val_loss /= len(test_loader.dataset)
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
    print(
        f"NLL={val_loss:.4f} | MSE={mse:.4f} | optimality={optimal_var:.4f} (ideal≈1)\n"
        f"var: mean={var_mean:.4e}  p90={var_p90:.4e}  max={var_max:.4e}\n"
        f"TP={m['true_positive_pct']:.1%}  FP={m['false_positive_pct']:.1%}  "
        f"TN={m['true_negative_pct']:.1%}  FN={m['false_negative_pct']:.1%} | "
        f"precision={m['precision']:.1%}  recall={m['recall']:.1%}  "
        f"accuracy={m['accuracy']:.1%}  F1={m['f1_score']:.3f}"
    )
    return val_loss, predictions


# --- Training loop for numerical-only model ---
def train_numerical_model(
    model,
    train_loader,
    test_loader,
    device,
    epochs,
    optimizer,
    loss_fn,
    batches_before_validation: int | None = None,
    patience: int = 5,
):
    # Validate once per epoch by default; caller can override to a fixed interval.
    if batches_before_validation is None:
        batches_before_validation = len(train_loader)

    # --- Initialization of variance_head ---
    y_all = np.concatenate([y.numpy() for _, y, _, _ in train_loader], axis=0)
    init_var = np.var(y_all) + 1e-6
    with torch.no_grad():
        model.variance_head.bias.data.fill_(init_var)

    best_val_loss = float("inf")
    best_state_dict = None
    best_optimizer_state_dict = None
    best_epoch = None
    best_predictions = None

    train_loss_history = []
    val_loss_history = []

    global_step = 0
    patience_counter = 0
    stop_training = False

    for epoch in range(epochs):
        if stop_training:
            break
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

            val_loss, predictions = _run_validation(
                model, test_loader, loss_fn, device, desc=f"Epoch {epoch+1} [val]"
            )
            val_loss_history.append((global_step, val_loss))

            # --- track best model + optimizer ---
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state_dict = copy.deepcopy(model.state_dict())
                best_optimizer_state_dict = copy.deepcopy(optimizer.state_dict())
                best_epoch = epoch + 1
                best_predictions = predictions

                patience_counter = 0
                print(f"  ✔ New best model at step {global_step}")
            else:
                patience_counter += 1
                print(
                    f"  ✖ No improvement ({patience_counter}/{patience} patience)"
                )

                if patience_counter >= patience:
                    print(
                        f"Early stopping triggered at step {global_step} "
                        f"(best epoch was {best_epoch})"
                    )
                    stop_training = True
                    break

            model.train()  # switch back to training mode

    # If validation never fired, do a final pass now so we always return a valid checkpoint.
    if best_state_dict is None:
        print("Validation never fired during training — running final validation pass.")
        val_loss, predictions = _run_validation(
            model, test_loader, loss_fn, device, desc="final val"
        )
        best_val_loss = val_loss
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
        best_val_loss,
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
) -> NumericalModel:
    """Create a NumericalModel with the given architecture.

    Args:
        structured_input_dim: Total input dimension (numerical + embedding dims).
        n_hidden_layers: Number of hidden layers.
        hidden_dim: Width of each hidden layer.
        dropout_rate: Dropout probability.

    Returns:
        Initialised NumericalModel.
    """
    return NumericalModel(
        structured_input_dim,
        hidden_dim,
        1,
        n_hidden_layers,
        dropout_rate=dropout_rate,
    )


def train_model(
    model: NumericalModel,
    train_dataset,
    test_dataset,
    batch_size: int = 32,
    epochs: int = 10,
    lr: float = 1e-4,
    batches_before_validation: int | None = None,
    negative_pair_weight: float = 0.1,
    false_positive_weight: float = 2.0,
) -> tuple:
    """Train ``model`` and return the best checkpoint.

    Args:
        model: NumericalModel to train.
        train_dataset: Training Dataset.
        test_dataset: Validation Dataset.
        batch_size: Mini-batch size.
        epochs: Maximum number of epochs.
        lr: Adam learning rate.
        batches_before_validation: Validate every N batches (default: once per epoch).
        negative_pair_weight: Loss weight when both target and prediction are negative.
        false_positive_weight: Loss weight when target is negative but prediction positive.

    Returns:
        7-tuple: (model_state_dict, optimizer_state_dict, epoch, val_loss,
                  predictions, train_loss_history, val_loss_history).
    """
    import functools

    loss_fn = functools.partial(
        asymmetric_nll_loss,
        negative_pair_weight=negative_pair_weight,
        false_positive_weight=false_positive_weight,
    )
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)
    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    return train_numerical_model(
        model, train_loader, test_loader, device, epochs, optimizer, loss_fn,
        batches_before_validation,
    )


def load_model(
    model_state: dict,
    optimizer_state: dict,
    epoch: int,
    structured_input_dim: int,
    n_hidden_layers: int,
    hidden_dim: int,
    dropout_rate: float,
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
        lr: Adam learning rate.

    Returns:
        Tuple of (model, optimizer, epoch).
    """
    model = create_model(structured_input_dim, n_hidden_layers, hidden_dim, dropout_rate)
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
            closes.extend(meta["close"].numpy())

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
