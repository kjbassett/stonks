import asyncio

import numpy as np
import pandas as pd
from data_access.dao_manager import dao_manager
from transformers import BertTokenizer


# We don't use a generator that inherits Sequence because we are relying on asynchronous db operations for each batch
class DataGenerator:
    def __init__(self, data, news_data, batch_size=32, max_text_length=512):
        self.data = data
        self.news_data = news_data
        self.batch_size = batch_size
        self.max_text_length = max_text_length
        self.shuffle = shuffle
        self.tokenizer = BertTokenizer.from_pretrained("M-FAC/bert-tiny-finetuned-mrpc")
        self.data = shuffle(self.data)

    def __len__(self):
        return int(np.floor(len(self.data) / self.batch_size))

    async def __call__(self):
        for batch_index in range(len(self)):
            x, y = await self.get_batch(batch_index)
            yield x, y
        self.data = shuffle(self.data)

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

    async def get_batch(self, index):
        index = index % len(self)
        batch_data = self.data[index * self.batch_size : (index + 1) * self.batch_size]
        # for debugging. delete me later
        if len(batch_data.index) < 50 and index == 0:
            batch_data.to_csv(f"batch{index}_data.csv")
        y = batch_data["target"].values
        x = await self.get_news(batch_data)
        return x, y

    async def get_news(self, batch_data):
        encoded_text = []
        news_columns = _get_news_columns(batch_data)
        news_texts_list = await fetch_all_news(batch_data, news_columns)
        for news_texts in news_texts_list:
            encoded_text.append(self.encode_texts(news_texts))

        structured_data = batch_data.drop(columns=news_columns + ["target"]).values
        try:
            x = np.hstack([encoded_text, structured_data])
        except ValueError as e:
            print(f"Error occurred while fetching news")
            raise e
        return x

    async def get_random_batch(self):
        indices = np.random.choice(len(self), self.batch_size, replace=False)
        batch_data = self.data.iloc[indices]
        y = batch_data["target"].values
        x = await self.get_news(batch_data)
        return x, y


def shuffle(data):
    return data.sample(frac=1).reset_index(drop=True)


async def fetch_all_news(batch_data, news_columns):
    tasks = []
    for _, row in batch_data.iterrows():
        tasks.append(fetch_news(row[news_columns].values.tolist()))
    return await asyncio.gather(*tasks)


async def fetch_news(news_ids: list):
    """
    Fetches news data for a give list of news_ids
    """
    news_dao = dao_manager.get_dao("News")
    news_texts = []
    for news_id in news_ids:
        if pd.isna(news_id):
            news_texts.append("")
        else:
            news_data = await news_dao.get(news_id)
            news_texts.append(news_data["body"].values[0])
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


async def create_generators(
    batch_size,
    structured_data,
    news_data=None
) -> (DataGenerator, DataGenerator):
    n_train = int(0.8 * len(structured_data))
    if batch_size == 0:
        batch_size = n_train
    train = structured_data.loc[:n_train]
    test = structured_data.loc[n_train:]
    train_generator = DataGenerator(train, news_data, batch_size=batch_size)
    test_generator = DataGenerator(test, news_data, batch_size=15)
    return train_generator, test_generator
