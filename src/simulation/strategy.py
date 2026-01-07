from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class StrategyPolicy:
    def __init__(self, rules, max_score=0.25, aggregation="sum"):
        self.rules = rules
        self.max_score = max_score
        self.aggregation = aggregation

        self.utilization_history = []
        self.return_history = []

    def apply(self, df_ts):
        # 1. Collect raw exposures from rules
        scores = np.array([rule.apply(df_ts) for rule in self.rules])

        # 2. Aggregate outputs per symbol
        if self.aggregation == "sum":
            scores = scores.sum(axis=0)
        elif self.aggregation == "max":
            scores = scores.max(axis=0)
        else:
            raise ValueError(f"Unknown aggregation: {self.aggregation}")

        # 3. Don't include negative scores unless we implement shorting in the future
        scores = np.clip(scores, 0, None)

        # 4. Normalized aggregated scores
        scores = normalize(scores)

        # 5. Cap score at max_score, redistribute the remaining scores from top to bottom score
        clipped = np.clip(scores, 0, self.max_score)
        remainder = (scores - clipped).sum()
        scores = clipped
        # get indeces in order of decreasing score without affecting the pd.Series itself
        order = np.argsort(-scores)
        for i in order:
            # no money left to redistribute OR no companies past the threshold.
            # This may not be the way for other kinds of rules
            if remainder <= 0 or scores[i] == 0:
                break
            cap = self.max_score - scores[i]
            if cap > 0:
                xfer = min(cap, remainder)
                scores[i] += xfer
                remainder -= xfer
        return scores

    def adapt(self, realized_return, utilization):
        self.return_history.append(realized_return)
        self.utilization_history.append(utilization)

        if len(self.return_history) < 5:
            return

        avg_return = sum(self.return_history[-5:]) / 5
        avg_util = sum(self.utilization_history[-5:]) / 5

        for rule in self.rules:
            rule.adapt(avg_return, avg_util)


class TradingRule(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.Series:
        """
        Input:
            df_ts: dataframe for a single timestamp
        Output:
            pd.Series indexed like df_ts.index with values in [0, 1]
            (exposure preferences)
        """
        pass

    @abstractmethod
    def adapt(self, avg_return, avg_utilization):
        pass


class PredictionThresholdRule(TradingRule):
    def __init__(
        self,
        threshold: float = 1.0,
        aggressiveness: float = 1.0,
        learning_rate: float = 0.01,
    ):
        self.threshold = threshold
        self.aggressiveness = aggressiveness
        self.learning_rate = learning_rate

    def apply(self, df):
        pred = df["prediction"]
        var = df["uncertainty"]

        scores = pred / (var.pow(0.5) + 1e-8)

        # Gate
        mask = scores >= self.threshold

        scores[mask] = scores[mask] ** self.aggressiveness
        scores[~mask] = 0

        scores = normalize(scores)

        return scores

    def adapt(self, avg_return, avg_util):
        if self.learning_rate == 0:
            return

        # Only loosen threshold if:
        # 1. returns are positive
        # 2. capital was underutilized
        # For util to be less than 1, there must be 1/max_score or fewer companies with a positive score
        if avg_return > 0 and avg_util < 1:
            self.threshold *= 1 - self.learning_rate

        # Tighten threshold if:
        # 1. returns are negative
        # 2. capital was heavily utilized
        elif (
            avg_return < 0 and avg_util == 1
        ):  # TODO should we adjust if avg_return is < something other than 0?
            self.threshold *= 1 + self.learning_rate

        self.threshold = max(0.0, self.threshold)


def normalize(x):
    s = x.sum()
    if s <= 0:
        return np.zeros_like(x)
    return x / s
