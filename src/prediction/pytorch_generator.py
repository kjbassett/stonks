import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset
from transformers import BertTokenizer


class StonksDataset(Dataset):
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
        self.num_columns = self.data.drop(
            columns=self.news_columns + ["name", "symbol", "target"]
        ).columns

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        row = self.data.iloc[idx]

        # --- Structured numeric features ---
        x_structured = row[self.num_columns].fillna(0).values.astype(np.float32)
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
                input_ids = encoded["input_ids"].squeeze(0)  # (seq_len,) get rid of the batch dimension
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
