import torch
import torch.nn as nn
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
