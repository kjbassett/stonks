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
        self.uncertainty_head = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        h = self.fc(x)
        mu = self.mean_head(h)
        var = self.uncertainty_head(h)  # variance (pre-softplus)
        var = nn.functional.softplus(var)  # enforce that variance must be positive
        return mu, var


# --- Training loop for numerical-only model ---
def train_numerical_model(
    model, train_loader, test_loader, device, epochs, optimizer, loss_fn
):
    # set inital value for uncertainty head, so the model starts close to reality
    # Without this, it might start witha  tiny or huge variance, causing unstable gradients.
    # compute training target variance
    y_all = np.concatenate([y.numpy() for _, y in train_loader], axis=0)
    init_var = np.var(y_all) + 1e-6
    with torch.no_grad():
        model.uncertainty_head.bias.data.fill_(init_var)

    for epoch in range(epochs):
        model.train()
        train_loss = 0.0
        for x_batch, y_batch in tqdm(train_loader, desc=f"Epoch {epoch+1} [train]"):
            x_batch = x_batch.to(device)
            y_batch = y_batch.to(device).unsqueeze(1)

            optimizer.zero_grad()
            mu, var = model(x_batch)
            loss = loss_fn(mu, y_batch, var)
            loss.backward()
            optimizer.step()

            train_loss += loss.item() * x_batch.size(0)
        train_loss /= len(train_loader.dataset)

        # --- Validation ---
        model.eval()
        val_loss = 0.0
        preds, uncertainies, targets = [], [], []
        with torch.no_grad():
            for x_batch, y_batch in tqdm(test_loader, desc=f"Epoch {epoch+1} [val]"):
                x_batch = x_batch.to(device)
                y_batch = y_batch.to(device).unsqueeze(1)

                mu, var = model(x_batch)
                loss = loss_fn(mu, y_batch, var)
                val_loss += loss.item() * x_batch.size(0)

                preds.extend(mu.cpu().numpy().flatten())
                uncertainies.extend(var.cpu().numpy().flatten())
                targets.extend(y_batch.cpu().numpy().flatten())

        val_loss /= len(test_loader.dataset)

        # save predictions + targets
        df = pd.DataFrame(
            {"target": targets, "prediction": preds, "uncertainty": uncertainies}
        )
        df.to_csv(f"validation_{epoch}.csv", index=False)

        print(f"Epoch {epoch+1}: Train Loss={train_loss:.4f}, Val Loss={val_loss:.4f}")
    return val_loss


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


def create_and_train(
    structured_input_dim,
    n_hidden_layers,
    hidden_dim,
    dropout_rate,
    train_dataset,
    test_dataset,
    batch_size=32,
    epochs=10,
    n_news=0,
    text_model_name=None,
    loss_fn=nn.GaussianNLLLoss(),
):
    # This is one function because the model tuner may want to run the creation / training in another process
    # TODO there are better ways around ^. in ezmt parent_process=True, save model and return path, etc
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    model = create_model(
        structured_input_dim,
        n_hidden_layers,
        hidden_dim,
        dropout_rate,
        n_news,
        text_model_name=text_model_name,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-4)

    train_fn = train_hybrid_model if n_news > 0 else train_numerical_model

    final_val_loss = train_fn(
        model, train_loader, test_loader, device, epochs, optimizer, loss_fn
    )
    model_path = save_model(model)
    return model_path, final_val_loss


def load_model(
    model_path,
    structured_input_dim,
    n_hidden_layers,
    hidden_dim,
    dropout_rate,
    n_news,
    text_model_name=None,
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
    model.load_state_dict(torch.load(model_path))

# TODO split create_and_train up into two separate steps with parent_process = True
#  Infer function
