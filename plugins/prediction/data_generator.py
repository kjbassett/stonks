import numpy as np
from transformers import BertTokenizer


# We don't use a generator that inherits Sequence because we are relying on asynchronous db operations for each batch
class DataGenerator():
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
        y = batch_data["target"].values
        x = self.merge_news(batch_data).values
        return x, y

    def get_random_batch(self):
        indices = np.random.choice(len(self), self.batch_size, replace=False)
        batch_data = self.data.iloc[indices]
        y = batch_data["target"].values
        x = self.merge_news(batch_data).values
        return x, y

    def merge_news(self, batch_data):
        for column in self.news_columns:
            batch_data = batch_data.merge(self.news_data, left_on=column, right_on="id")
        batch_data = batch_data.drop(columns=self.news_columns)
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
