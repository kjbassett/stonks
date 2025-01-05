from data_access.dao_manager import dao_manager
from plugins.decorator import plugin

from .data_generator import create_generators
from .create_model import create_combined_model
from ezmt.model_tuner import ModelTuner
from ezmt.hyperparameters import ContinuousRange, DiscreteOrdinal, DiscreteNonOrdinal

import tensorflow as tf
from tensorflow import keras

data_dao = dao_manager.get_dao("DataAggregator")


@plugin(model_name={"ui_element": "textbox", "default": "Genesis"})
async def train_model(model_name: str):
    hyperparams = {
        'batch_size': DiscreteOrdinal([16, 32, 64, 128, 256]),
        'max_text_length': DiscreteOrdinal([512, 1024]),
        'avg_close': DiscreteNonOrdinal([True, False]),
        'avg_volume': DiscreteNonOrdinal([True, False]),
        'std_dev': DiscreteNonOrdinal([True, False]),
        'num_news': DiscreteOrdinal([0, 1, 2, 3, 4, 5]),
        'news_relative_age_threshold': ContinuousRange(6*60*60, 48*60*60),
        'hidden_layer_dim': DiscreteOrdinal([512, 1024, 2048]),
        'dropout_rate': ContinuousRange(0.1, 0.5),
    }
    model_space = [
        {
            'name': 'create_generators',
            'train': {
                'func': create_generators,
                'args': [
                    'batch_size',
                    'max_text_length',
                ],
                'kwargs': {
                    'avg_close': 'avg_close',
                    'avg_volume': 'avg_volume',
                    'std_dev': 'std_dev',
                    'n_news': 'num_news',
                    'news_relative_age_threshold': 'news_relative_age_threshold'
                },
                'outputs': ['train_generator', 'test_generator'],
                'run_in_parent_process': True,
            }
        },
        {
            'name': 'load_train_batch',
            'train': {
                'func': 'train_generator.load_batch',
                'args': 0,
                'outputs': ['x_train', 'y_train'],
                'run_in_parent_process': True,
            }
        },
        {
            'name': 'get_structured_input_dim',
            'train': {
                'func': count_structured_input_dim,
                'args': ['x_train', 'num_news'],
                'outputs': 'structured_input_dim',
            }
        },
        {
            'name': 'create_model',
            'train': {
                'func': create_combined_model,
                'args': [
                    model_name,
                    'num_news',
                    'structured_input_dim',
                    'hidden_layer_dim',
                    1 # output dim
                ],
                'kwargs': {
                    'dropout_rate': 'dropout_rate',
                    'output_activation': None
                },
                'outputs': 'model'
            }
        },
        {
            'name': 'save_load_model',
            'train': {
                'func': save_model,
                'args': 'model',
                'outputs': 'model_path',
            },
            'inference': {
                'func': keras.models.load_model,
                'args': 'model_path',
                'outputs': ['model']
            }
        },
        {
            'name': 'fit_predict',
            'train': {
                'func': fit_model,
                'args': ['model', 'train_generator', 'test_generator', 10],  # 10 epochs
                'outputs': ['history'],
                'gpu': True
            },
            'inference': {
                'func': 'model.predict',
                'args': 'x_new',
                'outputs': 'y_pred'
            }
        },
        {
            'name': 'score',
            'train': {
                'func': get_score,
                'args': 'history',
                'outputs': 'score'
            }
        }
    ]
    mt = ModelTuner(model_space, hyperparams, None, 'price_change_perc', 1, 1)
    model = await mt.run()
    model.save(model_name)


def fit_model(model, train_generator, validation_generator, epochs):
    return model.fit(
        train_generator,
        validation_data=test_generator,
        epochs=epochs,  # Example number of epochs
        steps_per_epoch=len(train_generator),
        validation_steps=len(test_generator),
    )

def save_model(model, model_folder: int='models', model_name: str=None):
    if model_name is None:
        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        model_name = f"model_{dt}_{pid}.h5"
    model_path = os.path.join(model_folder, model_name)
    model.save(model_path)
    return model_path


def count_structured_input_dim(array, num_news):
    return array.shape[1] - 512 * 2 * num_news


def get_score(history):
    return history.history['val_loss'][-1]

# TODO
#  Normalization
#  de-couple statistics and news data from initial data load
#  OR
#  make a separate query to get only the necessary info for new_data