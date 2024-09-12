import asyncio

import numpy as np
import pandas as pd
from data_access.dao_manager import dao_manager
from icecream import ic
from plugins.decorator import plugin
from sklearn.preprocessing import StandardScaler
from transformers import BertTokenizer, pipeline

data_dao = dao_manager.get_dao("DataAggregator")
news_dao = dao_manager.get_dao("News")


# We don't use a generator that inherits Sequence because we are relying on asynchronous db operations for each batch


class DataGenerator:
    def __init__(
        self,
        data,
        n_news,
        batch_size=32,
        max_text_length=512,
        shuffle_data=True,
        hide_company_names=False,
    ):
        self.data = data
        self.data["target"] = 1  # delete me later!

        self.news_columns = [f"news{i}_id" for i in range(1, n_news + 1)]
        exclude = ["company_id", "symbol", "name", "target"] + self.news_columns
        self.x_columns = [col for col in self.data.columns if col not in exclude]

        self.batch_size = batch_size
        self.max_text_length = max_text_length
        self.tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
        if shuffle_data:
            self.data = shuffle(self.data)
        self.hide_company_names = hide_company_names
        self.scaler = StandardScaler()

        self.scaler.fit(self.data[self.x_columns])

    def __len__(self):
        return int(np.floor(len(self.data) / self.batch_size))

    async def load_batch(self, index):
        batch_data = self.data[index * self.batch_size : (index + 1) * self.batch_size]
        encoded_text = []
        # fetch all news for each company in the batch, and encode them into a list of tensors

        for _, row in batch_data.iterrows():
            encoded_text = asyncio.create_task(self.fetch_and_encode_texts(row))
        await asyncio.gather(*encoded_text)

        structured_data = batch_data[self.x_columns].values
        # TODO check if there are some columns that should be excluded
        structured_data = self.scaler.transform(structured_data)
        X = np.hstack([encoded_text, structured_data])
        y = batch_data["target"].values
        return X, y

    async def fetch_and_encode_texts(self, row):
        tasks = []
        name, symbol = row["name"], row["symbol"]
        for col in self.news_columns:
            # fetch
            task = asyncio.create_task(fetch_news([row[col]]))
            # encode
            task.add_done_callback(
                lambda future: self.encode_text(future.result(), name, symbol)
            )
            tasks.append(task)

        encoded_texts = []
        for task in tasks:
            enc_text = await task
            encoded_texts.append(enc_text["input_ids"])
            encoded_texts.append(enc_text["attention_mask"])
        return np.hstack(encoded_texts)[0]

    async def encode_text(self, text, name, symbol):
        if self.hide_company_names:
            text = replace_company_names(text, name, symbol)
        return self.tokenizer.encode_plus(
            text,
            add_special_tokens=True,
            max_length=self.max_text_length,
            padding="max_length",
            truncation=True,
            return_attention_mask=True,
            return_tensors="tf",
        )


def shuffle(data):
    return data.sample(frac=1).reset_index(drop=True)


async def fetch_news(news_id):
    if pd.isna(news_id):
        return ""
    else:
        # TODO could be faster by querying all at once. Also cache?
        news_data = await news_dao.get_data(news_id=news_id)
        return news_data["text"].values[0]


def replace_company_names(
    text, subject_name, subject_symbol, ner_model_name="dslim/bert-base-NER-uncased"
):
    ner_pipeline = pipeline("ner", model=ner_model_name, tokenizer=ner_model_name)
    ner_results = ner_pipeline(text)

    # Initialize variables to keep track of entities
    entities = []
    current_entity = ""
    current_label = ""

    # Group B-ORG and I-ORG tokens into full entity names
    for entity in ner_results:
        entity_text = text[entity["start"] : entity["end"]]
        # B-ORG is start of a new oganization, save last one if new one found
        if entity["entity"] == "B-ORG":
            if current_entity:
                entities.append((current_entity, current_label))
            current_entity = entity_text
            current_label = entity["entity"]
        # I-ORG is continuation of an existing organization, add to current entity
        elif entity["entity"] == "I-ORG" and current_entity:
            current_entity += " " + entity_text

    # Add the last entity if any
    if current_entity:
        entities.append((current_entity, current_label))

    replaced_text = text

    # Replace entities with appropriate placeholders
    for entity_text, _ in entities:
        if entity_text == subject_name or entity_text == subject_symbol:
            replaced_text = replaced_text.replace(entity_text, "this company")
        else:
            replaced_text = replaced_text.replace(entity_text, "another company")

    return replaced_text


async def create_generators(
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
) -> tuple[DataGenerator, DataGenerator]:
    if windows is None:
        windows = [4, 19, 59, 389]
    data = await data_dao.get_data(
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
        train,
        n_news,
        batch_size=batch_size,
        max_text_length=max_text_length,
        shuffle_data=True,
    )
    test_generator = DataGenerator(
        test, n_news, batch_size=len(test.index), max_text_length=max_text_length
    )
    return train_generator, test_generator


@plugin()
async def scratch():
    # Create generators
    train_generator, test_generator = await create_generators(
        batch_size=32,
        max_text_length=512,
        avg_close=True,
        avg_volume=True,
        std_dev=True,
        windows=(4, 19, 59, 389),
        n_news=3,
        news_relative_age_threshold=24 * 60 * 60,
    )
    for i in range(len(train_generator)):
        print(f"Batch {i+1}/{len(train_generator)}:")
        d = await train_generator.load_batch(i)
        ic(d)
