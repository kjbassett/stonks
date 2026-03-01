import unittest
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
from src.trading.trading_engine import TradingEngine
from src.trading.strategy import (
    InformationRatioRule,
    PredictionMagnitudeRule,
    PredictionThresholdRule,
    StrategyPolicy,
)

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

file_name = "tests/trading_strategy_test_data.csv"
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

    def test_adapt_no_change_when_profitable_and_fully_deployed(self):
        # Profitable AND fully deployed — threshold stays exactly where it is
        self.rule.adapt(1, 1)
        assert self.rule.threshold == 1

    def test_adapt_tightens_on_any_negative_return(self):
        # Losing money regardless of utilization — always tighten
        self.rule.adapt(-1, 0.9)
        assert self.rule.threshold == 1.01
        # Same when fully deployed
        self.rule.threshold = 1.0
        self.rule.adapt(-1, 1.0)
        assert self.rule.threshold == 1.01

    def test_adapt_loosens_when_profitable_and_underutilized(self):
        # Profitable but capital is not fully deployed — relax filter
        self.rule.adapt(1, 0.9)
        assert self.rule.threshold == 0.99

    def test_adapt_tightens_when_losing_and_fully_deployed(self):
        # Classic case: lost money going all-in
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


class TestPredictionMagnitudeRule(unittest.TestCase):

    def setUp(self):
        self.rule = PredictionMagnitudeRule(aggressiveness=1.0, learning_rate=0.01)
        # Simple frame: one negative, two positive with a 1:3 prediction ratio
        self.df = pd.DataFrame({
            "prediction": [-0.5, 0.25, 0.75],
            "variance":   [0.10, 0.10, 0.10],
        })

    def test_apply_ignores_negative_predictions(self):
        scores = self.rule.apply(self.df)
        assert scores.iloc[0] == 0

    def test_apply_sums_to_1(self):
        scores = self.rule.apply(self.df)
        self.assertAlmostEqual(scores.sum(), 1.0)

    def test_apply_proportional_to_prediction(self):
        scores = self.rule.apply(self.df)
        # aggressiveness=1 → score ∝ prediction; ratio should be 0.25:0.75 = 1:3
        self.assertAlmostEqual(scores.iloc[1] / scores.iloc[2], 0.25 / 0.75)

    def test_apply_aggressiveness_changes_ratio(self):
        rule = PredictionMagnitudeRule(aggressiveness=2.0, learning_rate=0.01)
        scores = rule.apply(self.df)
        # aggressiveness=2 → score ∝ pred²; ratio should be 0.0625:0.5625 = 1:9
        self.assertAlmostEqual(scores.iloc[1] / scores.iloc[2], 0.0625 / 0.5625)

    def test_apply_all_negative_returns_zeros(self):
        df = pd.DataFrame({"prediction": [-1.0, -0.5], "variance": [0.1, 0.1]})
        scores = self.rule.apply(df)
        for s in scores:
            assert s == 0

    def test_adapt_lr0_no_change(self):
        rule = PredictionMagnitudeRule(aggressiveness=1.0, learning_rate=0)
        rule.adapt(1, 0.5)
        assert rule.aggressiveness == 1.0

    def test_adapt_increases_aggressiveness_when_profitable_underutilized(self):
        self.rule.adapt(0.1, 0.5)  # avg_return > 0, avg_util < 1
        assert self.rule.aggressiveness > 1.0

    def test_adapt_decreases_aggressiveness_when_losing(self):
        self.rule.adapt(-0.1, 0.8)  # avg_return < 0
        assert self.rule.aggressiveness < 1.0

    def test_adapt_no_change_when_profitable_and_fully_deployed(self):
        self.rule.adapt(0.1, 1.0)  # avg_return > 0, avg_util >= 1
        assert self.rule.aggressiveness == 1.0

    def test_adapt_aggressiveness_clamped_at_max_3(self):
        self.rule.aggressiveness = 2.99
        for _ in range(100):
            self.rule.adapt(1, 0)
        assert self.rule.aggressiveness <= 3.0

    def test_adapt_aggressiveness_clamped_at_min_0_1(self):
        self.rule.aggressiveness = 0.11
        for _ in range(100):
            self.rule.adapt(-1, 0)
        assert self.rule.aggressiveness >= 0.1


class TestInformationRatioRule(unittest.TestCase):

    def setUp(self):
        self.rule = InformationRatioRule(min_ratio=0.0, learning_rate=0.01)
        # A: negative prediction → excluded
        # B: pred²/var = 0.25/0.5  = 0.5
        # C: pred²/var = 1.0 /0.25 = 4.0
        self.df = pd.DataFrame({
            "prediction": [-0.5, 0.5,  1.0],
            "variance":   [0.10, 0.5, 0.25],
        })

    def test_apply_ignores_negative_predictions(self):
        scores = self.rule.apply(self.df)
        assert scores.iloc[0] == 0

    def test_apply_sums_to_1(self):
        scores = self.rule.apply(self.df)
        self.assertAlmostEqual(scores.sum(), 1.0)

    def test_apply_higher_ratio_gets_higher_score(self):
        scores = self.rule.apply(self.df)
        # C has ratio 4.0 vs B's 0.5
        assert scores.iloc[2] > scores.iloc[1]

    def test_apply_scores_proportional_to_ratio(self):
        scores = self.rule.apply(self.df)
        # scores ∝ ratio; B=0.5, C=4.0 → B/C = 0.5/4.0 = 1/8
        self.assertAlmostEqual(scores.iloc[1] / scores.iloc[2], 0.5 / 4.0)

    def test_apply_min_ratio_filters_low_confidence(self):
        rule = InformationRatioRule(min_ratio=1.0, learning_rate=0.01)
        scores = rule.apply(self.df)
        # B's ratio=0.5 < 1.0 → filtered out; C's ratio=4.0 → included
        assert scores.iloc[1] == 0
        assert scores.iloc[2] > 0

    def test_apply_all_below_min_ratio_returns_zeros(self):
        rule = InformationRatioRule(min_ratio=999.0)
        scores = rule.apply(self.df)
        for s in scores:
            assert s == 0

    def test_adapt_lr0_no_change(self):
        rule = InformationRatioRule(min_ratio=1.0, learning_rate=0)
        rule.adapt(-1, 1)
        assert rule.min_ratio == 1.0

    def test_adapt_increases_min_ratio_when_losing(self):
        rule = InformationRatioRule(min_ratio=1.0, learning_rate=0.01)
        rule.adapt(-0.1, 0.8)
        assert rule.min_ratio > 1.0

    def test_adapt_decreases_min_ratio_when_profitable_underutilized(self):
        rule = InformationRatioRule(min_ratio=1.0, learning_rate=0.01)
        rule.adapt(0.1, 0.5)
        assert rule.min_ratio < 1.0

    def test_adapt_no_change_when_profitable_and_fully_deployed(self):
        rule = InformationRatioRule(min_ratio=1.0, learning_rate=0.01)
        rule.adapt(0.1, 1.0)
        assert rule.min_ratio == 1.0

    def test_adapt_min_ratio_floored_at_zero(self):
        rule = InformationRatioRule(min_ratio=0.001, learning_rate=0.01)
        for _ in range(100):
            rule.adapt(1, 0)
        assert rule.min_ratio >= 0.0


class TestTradingEnging(unittest.IsolatedAsyncioTestCase):
    async def test_sim_applies_executes_the_right_stuff(self):
        policy = MagicMock()
        policy.apply = MagicMock(return_value=0)
        policy.adapt = MagicMock()
        # rebalance_interval_hours=0 ensures every timestamp triggers a rebalance,
        # matching the original behaviour where the simulator had no throttling.
        # allow_intraday=True because both test timestamps (0 and 1) map to the same
        # calendar date (1970-01-01), so without it PDT would block sells at ts=1.
        # stop_loss_pct=1.0 disables accidental stop-loss triggers (price would need
        # to drop 100% to fire, which never happens in the test data).
        sim = TradingEngine(
            df_original,
            policy=policy,
            rebalance_interval_hours=0.0,
            stop_loss_pct=1.0,
            allow_intraday=True,
        )
        sim.executor.get_equity = AsyncMock(return_value=1.0)
        sim.executor.execute_target_exposure = AsyncMock()
        sim.executor.check_stop_losses = AsyncMock(return_value=[])

        await sim.run()

        expected_iterations = len(df_original["timestamp"].unique())  # 2
        assert sim.policy.apply.call_count == expected_iterations
        assert sim.policy.adapt.call_count == expected_iterations - 1
        # get_equity is called twice per rebalancing iteration:
        # once inside the rebalance block (for sizing) and once for the adapt step
        assert sim.executor.get_equity.call_count == expected_iterations * 2
        assert sim.executor.execute_target_exposure.call_count == len(df_original.index)
        assert sim.executor.check_stop_losses.call_count == expected_iterations

    async def test_sim_gives_the_right_result(self):
        policy = StrategyPolicy([
            PredictionThresholdRule(
                threshold=1.0, aggressiveness=1.0, learning_rate=0.01
            ),
            PredictionThresholdRule(
                threshold=1.0, aggressiveness=2.0, learning_rate=0.01
            ),
        ])
        # rebalance_interval_hours=0 ensures every timestamp triggers a rebalance.
        # allow_intraday=True because both test timestamps (0 and 1) map to the same date
        # calendar date (1970-01-01), so without it PDT would block sells at ts=1.
        self.sim = TradingEngine(
            df_original,
            policy,
            paper_trading=True,
            rebalance_interval_hours=0.0,
            allow_intraday=True
        )

        result = await self.sim.run()
        assert result["total_equity"] == 175000
        expected_values = {
            "AS": 0.25 * 175000,
            "AT": 0.25 * 175000,
            "AU": 0.25 * 175000,
        }
        for symbol, value in expected_values.items():
            # price should be $2 each at end of simulation
            self.assertAlmostEqual(
                result["executor"].broker.portfolio.positions[symbol].shares * 2, value
            )
