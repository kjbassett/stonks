import asyncio

import numpy as np
import pandas as pd
from data_access.dao_manager import dao_manager
from transformers import BertTokenizer

data_dao = dao_manager.get_dao("DataAggregator")
news_dao = dao_manager.get_dao("News")


# We don't use a generator that inherits Sequence because we are relying on asynchronous db operations for each batch
class DataGenerator:
    def __init__(self, data, batch_size=32, max_text_length=512, shuffle_data=True):
        self.data = data
        self.data["target"] = 1  # delete me later!
        self.batch_size = batch_size
        self.max_text_length = max_text_length
        self.shuffle = shuffle
        self.tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
        if shuffle_data:
            self.data = shuffle(self.data)

    def __len__(self):
        return int(np.floor(len(self.data) / self.batch_size))

    def encode_texts(self, texts):
        result = []
        for text in texts:
            encoded = self.tokenizer.encode_plus(
                text,
                add_special_tokens=True,
                max_length=self.max_text_length,
                padding="max_length",
                truncation=True,
                return_attention_mask=True,
                return_tensors="tf",
            )
            result.append(encoded["input_ids"])
            result.append(encoded["attention_mask"])
        result = np.hstack(result)[0]
        return result

    async def load_batch(self, index):
        batch_data = self.data[index * self.batch_size : (index + 1) * self.batch_size]
        encoded_text = []

        news_columns = _get_news_columns(batch_data)

        news_texts_list = await fetch_all_news(batch_data, news_columns)
        for news_texts in news_texts_list:
            encoded_text.append(self.encode_texts(news_texts))

        structured_data = batch_data.drop(columns=news_columns + ["target"]).values
        X = np.hstack([encoded_text, structured_data])
        y = batch_data["target"].values
        return X, y


def shuffle(data):
    return data.sample(frac=1).reset_index(drop=True)


async def fetch_all_news(batch_data, news_columns):
    tasks = []
    for _, row in batch_data.iterrows():
        tasks.append(fetch_news(row[news_columns].values.tolist()))
    return await asyncio.gather(*tasks)


async def fetch_news(news_ids):
    news_texts = []
    for news_id in news_ids:
        if pd.isna(news_id):
            news_texts.append("")
        else:
            news_data = await news_dao.get_data(news_id=news_id)
            news_texts.append(news_data["text"].values[0])
    return news_texts


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


def create_generators(
    batch_size,
    max_text_length,
    company_id: int = None,
    min_timestamp: int = None,
    max_timestamp: int = None,
    avg_close: bool = True,
    avg_volume: bool = True,
    std_dev: bool = True,
    windows: iter = None,
    n_news: int = 3,
    news_relative_age_threshold: int = 24 * 60 * 60,
) -> pd.DataFrame:
    if windows is None:
        windows = [4, 19, 59, 389]
    data = data_dao.get_data(
        company_id,
        min_timestamp,
        max_timestamp,
        avg_close,
        avg_volume,
        std_dev,
        windows,
        n_news,
        news_relative_age_threshold,
    )
    data = shuffle(data)
    train = data.loc[: int(0.8 * len(data))]
    test = data.loc[int(0.8 * len(data)) :]
    train_generator = DataGenerator(
        train, batch_size=batch_size, max_text_length=max_text_length, shuffle_data=True
    )
    test_generator = DataGenerator(
        test, batch_size=len(test.index), max_text_length=max_text_length
    )
    return train_generator, test_generator
