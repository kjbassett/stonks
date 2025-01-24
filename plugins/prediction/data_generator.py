import numpy as np
from transformers import BertTokenizer
from icecream import ic
from tensorflow import keras


# We don't use a generator that inherits Sequence because we are relying on asynchronous db operations for each batch
class DataGenerator(keras.utils.Sequence):
    def __init__(self, data, news_data, batch_size=32, max_text_length=512):
        self.data = data
        self.news_data = news_data
        self.batch_size = batch_size
        self.max_text_length = max_text_length
        self.tokenizer = BertTokenizer.from_pretrained("M-FAC/bert-tiny-finetuned-mrpc")
        self.news_columns = _get_news_columns(self.data)

    def __len__(self):
        return int(np.floor(len(self.data) / self.batch_size))

    def __getitem__(self, index):
        index = index % len(self)
        batch_data = self.data[index * self.batch_size: (index + 1) * self.batch_size]

        x_structured = batch_data.drop(columns=self.news_columns + ["target"]).fillna(0)
        x = [x_structured.values]

        # Merge news data for each news column
        for column in self.news_columns:
            news_batch = batch_data.merge(self.news_data, left_on=column, right_on="id", how="left")

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
        x = self.merge_news(batch_data)
        y = batch_data[["target"]]
        x = x.drop(columns=["target"])
        x = x.fillna(0)
        return x.values, y.values

    def merge_news(self, batch_data):
        for column in self.news_columns:
            batch_data = batch_data.merge(self.news_data, left_on=column, right_on="id", how="left")
        batch_data = batch_data.drop(columns=self.news_columns + ["id"])
        return batch_data


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


def shuffle(df):
    return df.sample(frac=1, random_state=42)  # shuffle the dataframe in place, and reset index afterwards


async def create_generators(
    batch_size,
    structured_data,
    news_data=None
) -> (DataGenerator, DataGenerator):
    n_train = int(0.8 * len(structured_data))
    if batch_size == 0:
        batch_size = n_train
    structured_data = shuffle(structured_data)
    train = structured_data.loc[:n_train]
    test = structured_data.loc[n_train:]
    train_generator = DataGenerator(train, news_data, batch_size=batch_size)
    test_generator = DataGenerator(test, news_data, batch_size=15)
    return train_generator, test_generator
