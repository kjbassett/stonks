import datetime
import os

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import AutoModel


class CombinedModel(nn.Module):
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


def smape_loss(y_pred, y_true):
    # Symmetric Mean Absolute Percentage Error (SMAPE)
    return 100 * torch.mean(
        2 * torch.abs(y_true - y_pred) / (torch.abs(y_true) + torch.abs(y_pred) + 1e-8)
    )


def create_and_train(
    model_name,
    text_model_name,
    structured_input_dim,
    n_hidden_layers,
    hidden_layer_dim,
    dropout_rate,
    train_dataset,
    test_dataset,
    epochs,
):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = CombinedModel(
        structured_input_dim,
        hidden_layer_dim,
        1,
        n_hidden_layers,
        text_model_name=text_model_name,
        dropout_rate=dropout_rate,
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=2e-4)
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False)

    for epoch in range(epochs):
        model.train()
        train_loss = 0.0
        for structured, input_ids_list, attn_mask_list, y in tqdm(
            train_loader, desc=f"Epoch {epoch+1} [train]"
        ):
            structured = structured.to(device)
            y = y.to(device).unsqueeze(1)  # (batch,1)

            # Stack articles → batch dimension
            input_ids_list = [x.to(device) for x in input_ids_list]
            attn_mask_list = [x.to(device) for x in attn_mask_list]

            optimizer.zero_grad()
            y_pred = model(structured, input_ids_list, attn_mask_list)

            loss = smape_loss(y_pred, y)
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
                loss = smape_loss(y_pred, y)
                val_loss += loss.item() * structured.size(0)

        val_loss /= len(test_loader.dataset)

        print(f"Epoch {epoch+1}: Train Loss={train_loss:.4f}, Val Loss={val_loss:.4f}")
    save_model(model)
    return val_loss


def save_model(model, model_folder: str = "models", model_name: str = None):
    if model_name is None:
        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        model_name = f"model_{dt}_{pid}.pth"
    model_path = os.path.join(model_folder, model_name)
    torch.save(model.state_dict(), model_path)
    return model_path
