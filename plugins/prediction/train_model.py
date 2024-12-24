from data_access.dao_manager import dao_manager
from plugins.decorator import plugin

from .data_generator import create_generators
from .create_model import create_combined_model
import ez_model_tuner

data_dao = dao_manager.get_dao("DataAggregator")


@plugin()
async def train_model(model_name: str):
    model_space = [
        {
            'name': 'create_generators',
            'train': {
                'func': create_generators,
                'args': [
                    0,
                    [512, 1024], # max text length
                ],
                'kwargs': {
                    'avg_close': [True, False],
                    'avg_volume': [True, False],
                    'std_dev': [True, False],
                    'n_news': [0, 1, 2, 3, 4, 5],
                    'news_relative_age_threshold': ez_model_tuner.config_validation.ContinuousRange(6*60*60, 48*60*60)
                },
                'outputs': ['train_generator', 'test_generator'],
            }
        },
        {
            'name': 'load_train_batch',
            'train': {
                'func': 'train_generator.load_batch',
                'args': [[0]],
                'outputs': ['x_train', 'y_train'],
            }
        },
        {
            'name': 'load_test_batch',
            'train': {
                'func': 'test_generator.load_batch',
                'args': [[0]],
                'outputs': ['x_test', 'y_test'],
            }
        },
        {
            'name': 'create_model',
            'train': {
                'func': create_combined_model,
                'args': [
                    'test_model_untrained',

                ]
            }
        }

    ]
    # # TODO: Create model and train using the generators
    # model_name: str,
    # num_texts: str,
    # structured_input_dim: int,
    # combined_hidden_dim: int,
    # output_dim: int,
    # text_model_name: str = "bert-base-uncased",
    # output_activation: str = "sigmoid",
    # dropout_rate: float = 0.3,