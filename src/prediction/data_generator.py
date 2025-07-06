import numpy as np
from functools import lru_cache
from transformers import BertTokenizer
from tensorflow import keras
import pandas as pd


class DataGenerator(keras.utils.Sequence):
    def __init__(self, data, news_data, tokenizer="M-FAC/bert-tiny-finetuned-mrpc", batch_size=32, max_text_length=512):
        self.data = data
        self.news_data = news_data
        self.batch_size = batch_size
        self.max_text_length = max_text_length
        self.tokenizer = BertTokenizer.from_pretrained(tokenizer)
        self.news_columns = _get_news_columns(self.data)

    def __len__(self):
        return int(np.floor(len(self.data) / self.batch_size))

    def __getitem__(self, index):
        index = index % len(self)
        batch_data = self.data[index * self.batch_size: (index + 1) * self.batch_size]

        x_structured = batch_data.drop(columns=self.news_columns + ["name", "symbol", "target"]).fillna(0)
        x = [x_structured.values]

        # Merge news data for each news column
        for column in self.news_columns:
            news_batch = batch_data[["symbol", "name", column]].merge(
                self.news_data, left_on=column, right_on="id", how="left"
            )
            news_batch = self.give_context_and_tokenize(news_batch)

            # Extract input IDs and attention masks from the merged data
            input_ids = news_batch.iloc[:, -2 * self.max_text_length: -self.max_text_length].fillna(0).values
            attention_masks = news_batch.iloc[:, -self.max_text_length:].fillna(0).values

            x.append(input_ids)
            x.append(attention_masks)

        # Extract targets
        y = batch_data[["target"]].values

        return x, y

    def get_random_batch(self):
        indices = np.random.choice(len(self), self.batch_size, replace=False)
        batch_data = self.data.iloc[indices]
        raise NotImplementedError("Tell Kenny to finish this method!")

    def merge_news(self, batch_data):
        for column in self.news_columns:
            batch_data = batch_data.merge(self.news_data, left_on=column, right_on="id", how="left")
        batch_data = batch_data.drop(columns=self.news_columns + ["id"])
        return batch_data

    def give_context_and_tokenize(self, news_data):
        # TODO memoize or something to avoid repeated tokenization of company + news body combo
        # Let the encoder know the company in question by prepending it to the beginning of the news text.
        contextualized_news = news_data["symbol"] + " " + news_data["name"] + " " + news_data["body"]
        contextualized_news = contextualized_news.fillna("")  # Fill missing news with empty string
        encoded = contextualized_news.apply(encode_text, args=(self.tokenizer, self.max_text_length))
        encoded_expanded = pd.DataFrame(encoded.tolist(), index=news_data.index)
        return encoded_expanded


def _get_news_columns(batch_data):
    news_columns = []
    i = 1
    while True:
        news_col = f"news{i}_id"
        if news_col not in batch_data.columns:
            break
        else:
            news_columns.append(news_col)
        i += 1
    return news_columns


@lru_cache(maxsize=1024)
def encode_text(text, tokenizer, max_length):
    encoded_dict = tokenizer.encode_plus(
        text,
        add_special_tokens=True,
        max_length=max_length,
        padding="max_length",
        truncation=True,
        return_attention_mask=True,
        return_tensors="tf"
    )

    return np.hstack([encoded_dict["input_ids"], encoded_dict["attention_mask"]])[0]

def shuffle(df):
    return df.sample(frac=1, random_state=42)  # shuffle the dataframe in place, and reset index afterwards


async def create_generators(
    structured_data,
    news_data=None,
    batch_size=32,
    tokenizer="M-FAC/bert-tiny-finetuned-mrpc",
    max_text_length=512,
) -> (DataGenerator, DataGenerator):
    n_train = int(0.8 * len(structured_data))
    if batch_size == 0:
        batch_size = n_train
    structured_data = shuffle(structured_data)
    train = structured_data.iloc[:n_train]
    test = structured_data.iloc[n_train:]
    train_generator = DataGenerator(
        train, news_data, tokenizer=tokenizer, batch_size=batch_size, max_text_length=max_text_length
    )
    test_generator = DataGenerator(test, news_data, batch_size=15)
    return train_generator, test_generator
