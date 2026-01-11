import unittest
from unittest.mock import MagicMock

import pandas as pd
from src.simulation.simulator import MarketSimulator
from src.simulation.strategy import PredictionThresholdRule, StrategyPolicy

"""
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
"""

file_name = "trading_strategy_test_data.csv"
df_original = pd.read_csv(file_name)


class TestPredictionThresholdRule(unittest.TestCase):
    # See trading_strategy_test_data_worksheet.ods

    def setUp(self):
        self.rule = PredictionThresholdRule(
            threshold=1.0, aggressiveness=1.0, learning_rate=0.01
        )
        self.df = df_original[df_original["timestamp"] == 0]

    def test_apply(self):
        scores = self.rule.apply(self.df)
        assert scores.sum() == 1
        for i, val in enumerate(scores[scores > 0]):
            self.assertAlmostEqual(
                val, [0.152815534545059, 0.243128026681781, 0.60405643877316][i]
            )

    def test_apply_with_aggressiveness(self):
        rule = PredictionThresholdRule(
            threshold=1.0, aggressiveness=2.0, learning_rate=0.01
        )
        scores = rule.apply(self.df)

        assert scores.sum() == 1
        for i, score in enumerate(scores[scores > 0]):
            self.assertAlmostEqual(
                score, [0.0522022838499184, 0.132137030995106, 0.815660685154975][i]
            )

    def test_apply_none_past_threshold(self):
        self.rule.threshold = 99999
        scores = self.rule.apply(self.df)
        for score in scores:
            assert score == 0

    def test_adapt_lr0(self):
        rule = PredictionThresholdRule(
            threshold=1.0, aggressiveness=2.0, learning_rate=0
        )
        rule.adapt(-1, -1)
        assert rule.threshold == 1
        rule.adapt(-1, 1)
        assert rule.threshold == 1
        rule.adapt(1, -1)
        assert rule.threshold == 1
        rule.adapt(0, 0)
        assert rule.threshold == 1

    def test_adapt_no_change(self):
        # if we had good returns and used all of our money, no change
        self.rule.adapt(1, 1)
        assert self.rule.threshold == 1
        # if we had bad returns and didn't go all in, no change
        self.rule.adapt(-1, 0.9)
        assert self.rule.threshold == 1

    def test_adapt_increases_threshold(self):
        # we made money but didn't want to put too much money in too few stocks
        self.rule.adapt(1, 0.9)
        assert self.rule.threshold == 0.99

    def test_adapt_decreases_threshold(self):
        # we lost money and went all in, raise threshold for buying
        self.rule.adapt(-1, 1)
        assert self.rule.threshold == 1.01


class TestStrategyPolicy(unittest.TestCase):

    def setUp(self):
        rules = [
            PredictionThresholdRule(
                threshold=1.0, aggressiveness=1.0, learning_rate=0.01
            ),
            PredictionThresholdRule(
                threshold=1.0, aggressiveness=2.0, learning_rate=0.01
            ),
        ]
        self.policy = StrategyPolicy(rules, max_score=0.25, aggregation="sum")
        self.df = df_original[df_original["timestamp"] == 0]

    def test_apply_applies_all_rules(self):
        mock_rule_apply = MagicMock()
        mock_rule_apply.return_value = [0.25, 0.25, 0.5]
        for rule in self.policy.rules:
            rule.apply = mock_rule_apply
        self.policy.apply(self.df)
        assert mock_rule_apply.call_count == len(self.policy.rules)
        assert len(self.policy.rules) == 2

    def test_apply_no_cap_sum(self):
        self.policy.max_score = 999
        scores = self.policy.apply(self.df)
        for i, score in enumerate(scores[scores > 0]):
            self.assertAlmostEqual(
                score, [0.102508909197489, 0.187632528838444, 0.709858561964068][i]
            )

    def test_apply_no_cap_max(self):
        self.policy.max_score = 999
        self.policy.aggregation = "max"
        scores = self.policy.apply(self.df)
        for i, score in enumerate(scores[scores > 0]):
            self.assertAlmostEqual(
                score, [0.126126608586433, 0.200666205493938, 0.67320718591963][i]
            )

    def test_apply_cap(self):
        result = self.policy.apply(self.df)
        # only 3 scores passed threshold (rest came out of rule with 0 score).
        # We don't distribute money to companies we think shouldn't buy
        assert result.tolist() == [0] * (len(self.df.index) - 3) + [0.25, 0.25, 0.25]

    def test_no_adaption_with_too_few_iterations(self):
        min_iterations = 5
        for rule in self.policy.rules:
            rule.adapt = MagicMock()
        for _ in range(min_iterations - 1):
            self.policy.adapt(0, 0)
            for rule in self.policy.rules:
                assert rule.adapt.call_count == 0

    def test_adaption_at_min_iterations(self):
        min_iterations = 5
        for rule in self.policy.rules:
            rule.adapt = MagicMock()
        for _ in range(min_iterations):
            self.policy.adapt(0, 0)
        for rule in self.policy.rules:
            assert rule.adapt.call_count == 1

    def test_adapt_calls_rule_adapt_with_correct_args(self):
        for rule in self.policy.rules:
            rule.adapt = MagicMock()
        self.policy.return_history = [1, 2, 3, 4]
        self.policy.utilization_history = [0.1, 0.2, 0.3, 0.4]
        self.policy.adapt(5, 0.5)
        for rule in self.policy.rules:
            assert rule.adapt.call_count == 1
            rule.adapt.assert_called_once_with(3, 0.3)


class TestSimulator(unittest.TestCase):
    def setUp(self):
        rules = [
            PredictionThresholdRule(
                threshold=1.0, aggressiveness=1.0, learning_rate=0.01
            ),
            PredictionThresholdRule(
                threshold=1.0, aggressiveness=2.0, learning_rate=0.01
            ),
        ]
        self.sim = MarketSimulator(df_original, rules=rules)

    def test_sim_applies_executes_the_right_stuff(self):
        self.sim.policy.apply = MagicMock()
        self.sim.policy.apply.return_value = 0
        self.sim.policy.adapt = MagicMock()
        self.sim.portfolio.total_equity = MagicMock()
        self.sim.portfolio.total_equity.return_value = 1
        self.sim._apply_trade = MagicMock()

        self.sim.run()

        expected_iterations = len(df_original["timestamp"].unique())
        assert self.sim.policy.apply.call_count == expected_iterations
        assert self.sim.policy.adapt.call_count == expected_iterations - 1
        assert self.sim.portfolio.total_equity.call_count == expected_iterations
        assert self.sim._apply_trade.call_count == len(df_original.index)

    def test_sim_gives_the_right_result(self):
        result = self.sim.run()
        assert result["total_equity"] == 175000
        expected_values = {
            "AS": 0.25 * 175000,
            "AT": 0.25 * 175000,
            "AU": 0.25 * 175000,
        }
        for symbol, value in expected_values.items():
            # price should be $2 each at end of simulation
            self.assertAlmostEqual(
                result["portfolio"].positions[symbol].shares * 2, value
            )
