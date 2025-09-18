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
    def __init__(self, data):
        self.x = torch.tensor(data.drop(columns=["target"]).values, dtype=torch.float32)
        self.y = torch.tensor(data["target"].values, dtype=torch.float32)

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        return self.x[idx], self.y[idx]


def shuffle(df):
    # shuffle the dataframe in place, and reset index afterwards
    return df.sample(frac=1, random_state=42)


async def create_datasets(
    structured_data,
    news_data=None,
    tokenizer="M-FAC/bert-tiny-finetuned-mrpc",
    max_text_length=512,
    split=0,
    shuffle_rows=False,
) -> (Dataset, Dataset):
    datasets = []
    # split structured data (numerical data)
    if shuffle_rows:
        structured_data = shuffle(structured_data)
    if split:
        n_train = int(split * len(structured_data))
        datasets.append(structured_data.iloc[:n_train])
        datasets.append(structured_data.iloc[n_train:])
    else:
        datasets = [structured_data]
    if news_data is None:
        datasets = [NumericalDataset(dataset) for dataset in datasets]
    else:
        # We don't split news data because it has a one-to-many relationship with structured data.
        # Later on, we retrieve correct news article(s) as needed. This saves memory.
        datasets = [
            HybridDataset(
                dataset,
                news_data,
                tokenizer_name=tokenizer,
                max_text_length=max_text_length,
            )
            for dataset in datasets
        ]
    return datasets
