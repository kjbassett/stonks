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
    """
    Gate stocks by their signal-to-noise ratio (prediction / sqrt(variance)),
    then score the ones that pass by that same ratio raised to `aggressiveness`.

    adapt() logic:
      - Returns positive AND utilization < 1: loosen threshold (more stocks qualify,
        signal is working and there's capacity to deploy more capital)
      - Returns negative (any utilization): tighten threshold (signal is hurting us,
        be more selective regardless of how deployed we are)
      - Returns positive AND utilization >= 1: leave threshold alone (working well,
        fully deployed — don't fix what isn't broken)
    """

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
        var = df["variance"]

        scores = pred / (var.pow(0.5) + 1e-8)

        mask = scores >= self.threshold
        scores[mask] = scores[mask] ** self.aggressiveness
        scores[~mask] = 0

        return normalize(scores)

    def adapt(self, avg_return, avg_util):
        if self.learning_rate == 0:
            return

        if avg_return > 0 and avg_util < 1:
            # Signal is profitable but we're underutilising capital — relax the filter
            self.threshold *= 1 - self.learning_rate
            print(f"[PredictionThresholdRule] Loosened threshold → {self.threshold:.4f}")
        elif avg_return < 0:
            # Losing money regardless of utilisation — tighten the quality filter
            self.threshold *= 1 + self.learning_rate
            print(f"[PredictionThresholdRule] Tightened threshold → {self.threshold:.4f}")
        # avg_return > 0 and avg_util >= 1: profitable and fully deployed — leave it alone

        self.threshold = max(0.0, self.threshold)


class PredictionMagnitudeRule(TradingRule):
    """
    Score stocks by the raw magnitude of their predicted return (ignoring uncertainty).

    Complements PredictionThresholdRule: that rule gates by signal quality,
    this rule weights the survivors by how much upside they predict.

    adapt(): if returns are negative, dampen aggressiveness; if positive and
    underutilised, increase it.
    """

    def __init__(self, aggressiveness: float = 1.0, learning_rate: float = 0.01):
        self.aggressiveness = aggressiveness
        self.learning_rate = learning_rate

    def apply(self, df):
        pred = df["prediction"].copy()
        pred = pred.clip(lower=0)  # ignore negative predictions (no shorting)
        scores = pred ** self.aggressiveness
        return normalize(scores)

    def adapt(self, avg_return, avg_util):
        if self.learning_rate == 0:
            return
        if avg_return > 0 and avg_util < 1:
            self.aggressiveness = min(self.aggressiveness * (1 + self.learning_rate), 3.0)
        elif avg_return < 0:
            self.aggressiveness = max(self.aggressiveness * (1 - self.learning_rate), 0.1)


class InformationRatioRule(TradingRule):
    """
    Score by prediction² / variance — the squared information ratio.

    This rewards stocks that are simultaneously high-conviction (large prediction)
    AND low-uncertainty (small variance). Compared to PredictionThresholdRule's
    linear ratio, the squaring of the prediction makes it prefer larger moves more
    aggressively, while still discounting uncertain predictions.

    adapt(): tighten the minimum ratio required when returns are negative.
    """

    def __init__(self, min_ratio: float = 0.0, learning_rate: float = 0.01):
        self.min_ratio = min_ratio
        self.learning_rate = learning_rate

    def apply(self, df):
        pred = df["prediction"]
        var = df["variance"] + 1e-8

        ratios = (pred ** 2) / var
        ratios = ratios.where(pred > 0, 0)  # only positive predictions
        ratios = ratios.where(ratios >= self.min_ratio, 0)

        return normalize(ratios)

    def adapt(self, avg_return, avg_util):
        if self.learning_rate == 0:
            return
        if avg_return > 0 and avg_util < 1:
            self.min_ratio = max(0.0, self.min_ratio * (1 - self.learning_rate))
        elif avg_return < 0:
            self.min_ratio *= 1 + self.learning_rate


def normalize(x):
    s = x.sum()
    if s <= 0:
        return np.zeros_like(x)
    return x / s
