import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset
from transformers import BertTokenizer


class HybridDataset(Dataset):
    def __init__(
        self, data, news_data, tokenizer_name="bert-base-uncased", max_text_length=512
    ):
        """
        data: main DataFrame with numeric features + news id columns + target
        news_data: DataFrame with columns ['id', 'symbol', 'name', 'body']
        """
        self.data = data.reset_index(drop=True)
        self.news_data = news_data
        self.max_text_length = max_text_length
        self.tokenizer = BertTokenizer.from_pretrained(tokenizer_name)
        self.news_columns = self._get_news_columns(self.data)

        # Which columns are numeric features?
        self.numerical_columns = self.data.drop(
            columns=self.news_columns + ["target"]
        ).columns

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        row = self.data.iloc[idx]

        # --- Structured numeric features ---
        x_structured = row[self.numerical_columns].fillna(0).values.astype(np.float32)
        x_structured = torch.tensor(x_structured, dtype=torch.float)

        # --- News articles for this sample ---
        input_ids_list, attn_mask_list = [], []
        for col in self.news_columns:
            news_id = row[col]
            if pd.isna(news_id):
                # Missing article → just pad with zeros
                input_ids = torch.zeros(self.max_text_length, dtype=torch.long)
                attn_mask = torch.zeros(self.max_text_length, dtype=torch.long)
            else:
                news_row = self.news_data[self.news_data["id"] == news_id]
                if len(news_row) == 0:
                    text = ""
                else:
                    text = f"{news_row.iloc[0]['symbol']} {news_row.iloc[0]['name']} {news_row.iloc[0]['body']}"

                encoded = self.tokenizer.encode_plus(
                    text,
                    add_special_tokens=True,
                    max_length=self.max_text_length,
                    padding="max_length",
                    truncation=True,
                    return_attention_mask=True,
                    return_tensors="pt",
                )
                # get rid of the batch dimension
                input_ids = encoded["input_ids"].squeeze(0)  # (seq_len,)
                attn_mask = encoded["attention_mask"].squeeze(0)  # (seq_len,)

            input_ids_list.append(input_ids)
            attn_mask_list.append(attn_mask)

        # --- Target ---
        y = torch.tensor(row["target"], dtype=torch.float)

        return x_structured, input_ids_list, attn_mask_list, y

    def _get_news_columns(self, df):
        cols = []
        i = 1
        while True:
            col = f"news{i}_id"
            if col not in df.columns:
                break
            cols.append(col)
            i += 1
        return cols


class NumericalDataset(Dataset):
    def __init__(
        self,
        data: pd.DataFrame,
        n_bins: int = 50,
        use_weights: bool = False,
        max_weight: float = 10.0,
    ):
        self.symbols = data["symbol"].values
        self.timestamps = data["timestamp"].values
        self.closes = data["close"].values
        self.x = torch.tensor(
            data[data.columns.difference(["target", "symbol", "timestamp"])].values,
            dtype=torch.float32,
        )
        if use_weights:
            # requires target column
            y_np = data["target"].values.astype(np.float32)
            self.y = torch.tensor(y_np, dtype=torch.float32)

            # --- density-aware sample weights ---
            hist, bin_edges = np.histogram(y_np, bins=n_bins, density=False)
            bin_idx = np.digitize(y_np, bin_edges[:-1], right=True)

            # avoid zero density
            hist = hist.astype(np.float32) + 1e-6
            density = hist[bin_idx - 1]

            weights = 1.0 / density
            weights = weights / weights.mean()  # keep loss scale stable
            weights = np.clip(weights, 0.0, max_weight)

            self.weights = torch.tensor(weights, dtype=torch.float32)
        else:
            if "target" in data.columns:
                y_np = data["target"].values.astype(np.float32)
                self.y = torch.tensor(y_np, dtype=torch.float32)
            else:
                self.y = torch.full((len(data),), float("nan"), dtype=torch.float32)
            self.weights = torch.ones(len(data), dtype=torch.float32)

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        meta = {
            "symbol": self.symbols[idx],
            "timestamp": self.timestamps[idx],
            "close": self.closes[idx],
        }
        return self.x[idx], self.y[idx], self.weights[idx], meta


def shuffle(df):
    # shuffle the dataframe in place, and reset index afterwards
    return df.sample(frac=1, random_state=42)


async def create_datasets(
    structured_data,
    news_data=None,
    tokenizer="M-FAC/bert-tiny-finetuned-mrpc",
    max_text_length=512,
    split=0,
    use_weights=False,
) -> list | Dataset:
    # split structured data (numerical data)
    if split:
        # We don't split news data because it has a one-to-many relationship with structured data.
        # Later on, we retrieve correct news article(s) as needed.
        # This saves memory over joining it with the trading data
        n_train = int(split * len(structured_data))
        train, test = structured_data.iloc[:n_train], structured_data.iloc[n_train:]
        # assume that training set uses weights and validation does not
        return [
            create_dataset(
                train,
                use_weights=True,
                news_data=news_data,
                tokenizer=tokenizer,
                max_text_length=max_text_length,
            ),
            create_dataset(
                test,
                use_weights=False,
                news_data=news_data,
                tokenizer=tokenizer,
                max_text_length=max_text_length,
            ),
        ]
    return create_dataset(
        structured_data,
        use_weights=use_weights,
        news_data=news_data,
        tokenizer=tokenizer,
        max_text_length=max_text_length,
    )


def create_dataset(
    data,
    use_weights=False,
    news_data=None,
    tokenizer="M-FAC/bert-tiny-finetuned-mrpc",
    max_text_length=None,
):
    if news_data is None:
        return NumericalDataset(data, use_weights=use_weights)
    else:
        return HybridDataset(
            data,
            news_data,
            tokenizer_name=tokenizer,
            max_text_length=max_text_length,
        )
