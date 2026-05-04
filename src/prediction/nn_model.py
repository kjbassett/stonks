import copy
import datetime
import os

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import AutoModel


class HybridModel(nn.Module):
    def __init__(
        self,
        structured_input_dim: int,
        combined_hidden_dim: int,
        output_dim: int,
        n_hidden_layers: int = 2,
        text_model_name: str = "bert-base-uncased",
        dropout_rate: float = 0.3,
        output_activation: str = "linear",
        freeze_text_encoder: bool = True,
    ):
        super().__init__()

        # --- Text encoder (HuggingFace) ---
        self.text_encoder = AutoModel.from_pretrained(text_model_name)
        if freeze_text_encoder:
            for param in self.text_encoder.parameters():
                param.requires_grad = False
        hidden_size = self.text_encoder.config.hidden_size

        # Optional normalization for pooled embeddings
        self.text_norm = nn.LayerNorm(hidden_size)

        # --- Structured (numerical) branch ---
        self.num_encoder = nn.Sequential(
            nn.Linear(structured_input_dim, combined_hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout_rate),
        )

        # --- Combined MLP ---
        layers = []
        input_dim = hidden_size + combined_hidden_dim
        for _ in range(n_hidden_layers):
            layers.append(nn.Linear(input_dim, combined_hidden_dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout_rate))
            input_dim = combined_hidden_dim
        self.fc = nn.Sequential(*layers)

        # --- Output layer ---
        self.output_layer = nn.Linear(combined_hidden_dim, output_dim)
        if output_activation == "sigmoid":
            self.activation = nn.Sigmoid()
        elif output_activation == "tanh":
            self.activation = nn.Tanh()
        elif output_activation == "relu":
            self.activation = nn.ReLU()
        else:
            self.activation = nn.Identity()  # linear

    def forward(self, structured_input, input_ids_list, attention_mask_list):
        """
        structured_input: (batch, structured_dim)
        input_ids_list: list of tensors [(batch, seq_len), ...] for multiple news articles
        attention_mask_list: list of tensors [(batch, seq_len), ...] matching input_ids_list
        """

        # --- Encode each article with BERT ---
        article_embeddings = []
        for input_ids, attn_mask in zip(input_ids_list, attention_mask_list):
            outputs = self.text_encoder(input_ids=input_ids, attention_mask=attn_mask)
            # CLS pooling
            cls_emb = outputs.last_hidden_state[:, 0, :]  # (batch, hidden_size)
            cls_emb = self.text_norm(cls_emb)
            article_embeddings.append(cls_emb)

        # Mean pooling across all articles for each example
        if len(article_embeddings) > 0:
            text_emb = torch.stack(article_embeddings, dim=0).mean(
                dim=0
            )  # (batch, hidden_size)
        else:
            # no news available
            text_emb = torch.zeros(
                structured_input.size(0), self.text_encoder.config.hidden_size
            ).to(structured_input.device)

        # --- Encode structured numeric data ---
        num_emb = self.num_encoder(structured_input)

        # --- Combine ---
        combined = torch.cat([text_emb, num_emb], dim=1)
        combined = self.fc(combined)

        # --- Output ---
        out = self.output_layer(combined)
        out = self.activation(out)
        return out


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
    print(
        f"NLL={val_loss:.4f} MSE={mse:.4f} (MSE should not increase by a lot or be super big)\n"
        f"var_mean={var_mean:.4e} var_p90={var_p90:.4e} var_max={var_max:.4e} (var_p90 shouldn't be >> var_mean)\n"
        f"optimality test: {optimal_var:.4f} (should be close to 1 when varianced is reduced as far as it can with the current mu)"
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
            loss.backward()
            optimizer.step()

            # --- Validation every N batches ---
            if global_step % batches_before_validation != 0:
                continue

            val_loss, predictions = _run_validation(
                model, test_loader, loss_fn, device, desc=f"Epoch {epoch+1} [val]"
            )

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

    # restore best weights + optimizer state
    model.load_state_dict(best_state_dict)
    optimizer.load_state_dict(best_optimizer_state_dict)

    return (
        best_state_dict,
        best_optimizer_state_dict,
        best_epoch,
        best_val_loss,
        best_predictions,
    )


# --- Training loop for hybrid model (numerical + text) ---
def train_hybrid_model(
    model, train_loader, test_loader, device, epochs, optimizer, loss_fn
):
    for epoch in range(epochs):
        model.train()
        train_loss = 0.0
        for structured, input_ids_list, attn_mask_list, y in tqdm(
            train_loader, desc=f"Epoch {epoch+1} [train]"
        ):
            structured = structured.to(device)
            y = y.to(device).unsqueeze(1)

            input_ids_list = [x.to(device) for x in input_ids_list]
            attn_mask_list = [x.to(device) for x in attn_mask_list]

            optimizer.zero_grad()
            y_pred = model(structured, input_ids_list, attn_mask_list)
            loss = loss_fn(y_pred, y)
            loss.backward()
            optimizer.step()

            train_loss += loss.item() * structured.size(0)
        train_loss /= len(train_loader.dataset)

        # --- Validation ---
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for structured, input_ids_list, attn_mask_list, y in tqdm(
                test_loader, desc=f"Epoch {epoch+1} [val]"
            ):
                structured = structured.to(device)
                y = y.to(device).unsqueeze(1)

                input_ids_list = [x.to(device) for x in input_ids_list]
                attn_mask_list = [x.to(device) for x in attn_mask_list]

                y_pred = model(structured, input_ids_list, attn_mask_list)
                loss = loss_fn(y_pred, y)
                val_loss += loss.item() * structured.size(0)
        val_loss /= len(test_loader.dataset)

        print(f"Epoch {epoch+1}: Train Loss={train_loss:.4f}, Val Loss={val_loss:.4f}")
    return val_loss


def save_model(model, model_folder: str = "models", model_name: str = None):
    if model_name is None:
        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        model_name = f"model_{dt}_{pid}.pth"
    model_path = os.path.join(model_folder, model_name)
    torch.save(model.state_dict(), model_path)
    return model_path


def create_model(
    structured_input_dim,
    n_hidden_layers,
    hidden_dim,
    dropout_rate,
    n_news,
    text_model_name=None,
):
    """
    creates the right type of model based on the specified arguments.
    Current: if news articles -> hybrid model, otherwise -> numerical
    """
    if n_news > 0:  # --- Hybrid ---
        return HybridModel(
            structured_input_dim,
            hidden_dim,
            1,
            n_hidden_layers,
            text_model_name=text_model_name,
            dropout_rate=dropout_rate,
        )
    else:
        return NumericalModel(
            structured_input_dim,
            hidden_dim,
            1,
            n_hidden_layers,
            dropout_rate=dropout_rate,
        )


def train_model(
    model,
    train_dataset,
    test_dataset,
    batch_size=32,
    epochs=10,
    n_news=0,
    loss_fn=nn.GaussianNLLLoss(reduction="none"),
    lr=1e-4,
    batches_before_validation: int | None = None,
):

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    # TODO already shuffling here. Remove shuffling from create_datasets
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    train_fn = train_hybrid_model if n_news > 0 else train_numerical_model

    model_state_dict, optimizer_state_dict, epoch, val_loss, predictions = train_fn(
        model, train_loader, test_loader, device, epochs, optimizer, loss_fn,
        batches_before_validation,
    )

    return model_state_dict, optimizer_state_dict, epoch, val_loss, predictions


def load_model(
    model_state,
    optimizer_state,
    epoch,
    structured_input_dim,
    n_hidden_layers,
    hidden_dim,
    dropout_rate,
    n_news,
    text_model_name=None,
    lr=1e-4,
):
    # recreate architecture
    model = create_model(
        structured_input_dim,
        n_hidden_layers,
        hidden_dim,
        dropout_rate,
        n_news,
        text_model_name=text_model_name,
    )

    # load and apply state
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
        for x_batch, _, meta in tqdm(data_loader, desc="Running inference"):
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
