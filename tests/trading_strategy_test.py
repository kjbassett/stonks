import unittest

import numpy as np
import pandas as pd
from src.prediction.trading_strategy import tune_ratio_cutoff


class TestTuneRatioCutoff(unittest.TestCase):

    def setUp(self):
        self.data = pd.read_csv("trading_strategy_test_data.csv")

    def test_zero_transaction_cost(self):
        best_cutoff, best_returns = tune_ratio_cutoff(self.data)
        self.assertAlmostEqual(best_cutoff, 1.390446, 5)
        self.assertEqual(best_returns, 3)

    def test_1_percent_transaction_cost(self):
        best_cutoff, best_returns = tune_ratio_cutoff(self.data, transaction_cost=1)
        self.assertEqual(best_cutoff, np.inf)
        self.assertEqual(best_returns, 0)
