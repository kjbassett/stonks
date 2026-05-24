import unittest

import numpy as np
import pandas as pd

from src.ml_diagnostics.checks import (
    LOSS_SPIKE_FAIL_MULTIPLIER,
    LOSS_SPIKE_ROLLING_WINDOW,
    LOSS_SPIKE_WARN_MULTIPLIER,
    NEAR_CONSTANT_RANGE_FAIL,
    NEAR_CONSTANT_RANGE_WARN,
    OUTLIER_FRACTION_FAIL,
    OUTLIER_FRACTION_WARN,
    OUTLIER_Z_THRESHOLD,
    _check_outlier_fraction,
    _check_post_scale_range,
    check_features,
    check_loss_spikes,
    run_data_quality_checks,
)


class TestCheckPostScaleRange(unittest.TestCase):
    # --- happy path ---

    def test_normal_spread_passes(self):
        values = np.linspace(-3.0, 3.0, 1000)
        result = _check_post_scale_range("col", values)
        self.assertEqual(result["status"], "pass")
        self.assertGreater(result["value"], NEAR_CONSTANT_RANGE_WARN)

    # --- edge cases ---

    def test_near_constant_fails(self):
        values = np.full(500, 0.5) + np.random.default_rng(0).normal(0, 1e-6, 500)
        result = _check_post_scale_range("col", values)
        self.assertEqual(result["status"], "fail")
        self.assertLess(result["value"], NEAR_CONSTANT_RANGE_FAIL)

    def test_low_variance_warns(self):
        # spread deliberately between FAIL and WARN thresholds
        values = np.linspace(0.0, NEAR_CONSTANT_RANGE_WARN * 0.5, 1000)
        result = _check_post_scale_range("col", values)
        self.assertEqual(result["status"], "warn")

    def test_single_value_returns_pass(self):
        result = _check_post_scale_range("col", np.array([1.0]))
        self.assertEqual(result["status"], "pass")

    # --- invalid input ---

    def test_empty_array_returns_pass(self):
        result = _check_post_scale_range("col", np.array([]))
        self.assertEqual(result["status"], "pass")


class TestCheckOutlierFraction(unittest.TestCase):
    # --- happy path ---

    def test_no_outliers_passes(self):
        rng = np.random.default_rng(42)
        values = rng.normal(0, 1, 10000)
        result = _check_outlier_fraction("col", values)
        self.assertEqual(result["status"], "pass")

    # --- edge cases ---

    def test_high_outlier_fraction_fails(self):
        # force > OUTLIER_FRACTION_FAIL fraction above threshold
        n = 10000
        n_outliers = int(n * (OUTLIER_FRACTION_FAIL + 0.01))
        values = np.concatenate([
            np.zeros(n - n_outliers),
            np.full(n_outliers, OUTLIER_Z_THRESHOLD + 1.0),
        ])
        result = _check_outlier_fraction("col", values)
        self.assertEqual(result["status"], "fail")

    def test_moderate_outlier_fraction_warns(self):
        n = 10000
        mid = (OUTLIER_FRACTION_WARN + OUTLIER_FRACTION_FAIL) / 2
        n_outliers = int(n * mid)
        values = np.concatenate([
            np.zeros(n - n_outliers),
            np.full(n_outliers, OUTLIER_Z_THRESHOLD + 1.0),
        ])
        result = _check_outlier_fraction("col", values)
        self.assertEqual(result["status"], "warn")

    def test_empty_array_returns_pass(self):
        result = _check_outlier_fraction("col", np.array([]))
        self.assertEqual(result["status"], "pass")

    # --- invalid input ---

    def test_all_extreme_values_fails(self):
        values = np.full(100, OUTLIER_Z_THRESHOLD + 10.0)
        result = _check_outlier_fraction("col", values)
        self.assertEqual(result["status"], "fail")
        self.assertAlmostEqual(result["value"], 1.0)


class TestCheckFeatures(unittest.TestCase):
    def _make_df(self, **cols) -> pd.DataFrame:
        return pd.DataFrame(cols)

    # --- happy path ---

    def test_normal_columns_all_pass(self):
        rng = np.random.default_rng(0)
        df = self._make_df(
            a=rng.normal(0, 1, 1000),
            b=rng.normal(0, 1, 1000),
        )
        report = check_features(df)
        self.assertTrue(all(v["status"] == "pass" for v in report.values()))

    def test_report_contains_both_checks(self):
        df = self._make_df(x=np.linspace(-3, 3, 200))
        report = check_features(df)
        self.assertIn("x", report)
        self.assertIn("post_scale_range", report["x"])
        self.assertIn("outlier_fraction", report["x"])

    # --- edge cases ---

    def test_near_constant_column_fails(self):
        df = self._make_df(flat=np.full(500, 0.0))
        report = check_features(df)
        self.assertEqual(report["flat"]["status"], "fail")

    def test_ignore_cols_excluded(self):
        df = self._make_df(
            symbol=pd.Categorical(["A"] * 100),
            price=np.linspace(-2, 2, 100),
        )
        report = check_features(df, ignore_cols=["symbol"])
        self.assertNotIn("symbol", report)
        self.assertIn("price", report)

    def test_non_numeric_cols_skipped_automatically(self):
        df = self._make_df(
            label=["cat"] * 100,
            value=np.linspace(-2, 2, 100),
        )
        report = check_features(df)
        self.assertNotIn("label", report)

    # --- invalid input ---

    def test_empty_dataframe_returns_empty_report(self):
        report = check_features(pd.DataFrame())
        self.assertEqual(report, {})


class TestCheckLossSpikes(unittest.TestCase):
    def _smooth_loss(self, n: int = 500) -> list:
        """Decreasing loss with no spikes."""
        return list(np.linspace(10.0, 0.01, n) + np.random.default_rng(1).normal(0, 0.001, n))

    def _spiked_loss(self, n: int = 500, spike_at: int = 300) -> list:
        """Decreasing loss with one massive spike."""
        arr = np.linspace(10.0, 0.01, n)
        arr[spike_at] = arr[spike_at - 1] * (LOSS_SPIKE_FAIL_MULTIPLIER + 50)
        return list(arr)

    # --- happy path ---

    def test_smooth_loss_passes(self):
        result = check_loss_spikes(self._smooth_loss())
        self.assertEqual(result["train"]["status"], "pass")

    def test_val_loss_included_when_provided(self):
        result = check_loss_spikes(self._smooth_loss(), val_loss_history=self._smooth_loss())
        self.assertIn("train", result)
        self.assertIn("val", result)

    # --- edge cases ---

    def test_spike_in_train_fails(self):
        result = check_loss_spikes(self._spiked_loss())
        self.assertEqual(result["train"]["status"], "fail")

    def test_spike_ratio_recorded_in_value(self):
        result = check_loss_spikes(self._spiked_loss())
        self.assertGreater(result["train"]["value"], LOSS_SPIKE_FAIL_MULTIPLIER)

    def test_too_few_batches_passes(self):
        short = [1.0] * (LOSS_SPIKE_ROLLING_WINDOW - 1)
        result = check_loss_spikes(short)
        self.assertEqual(result["train"]["status"], "pass")

    def test_empty_train_history_returns_empty_dict(self):
        result = check_loss_spikes([])
        self.assertNotIn("train", result)

    # --- invalid input ---

    def test_none_val_history_ignored(self):
        result = check_loss_spikes(self._smooth_loss(), val_loss_history=None)
        self.assertNotIn("val", result)

    def test_warn_multiplier_in_warn_range(self):
        n = 500
        arr = np.full(n, 1.0)
        # spike at 400, just above warn but below fail
        arr[400] = LOSS_SPIKE_WARN_MULTIPLIER * 5 + 1
        result = check_loss_spikes(list(arr))
        self.assertIn(result["train"]["status"], ("warn", "fail"))


class TestRunDataQualityChecks(unittest.TestCase):
    def _good_data(self) -> pd.DataFrame:
        rng = np.random.default_rng(7)
        return pd.DataFrame({
            "feature_a": rng.normal(0, 1, 500),
            "feature_b": rng.normal(0, 1, 500),
            "symbol": ["X"] * 500,
            "timestamp": list(range(500)),
            "target": rng.normal(0, 1, 500),
        })

    def _good_loss(self, n: int = 500) -> list:
        return list(np.linspace(5.0, 0.01, n))

    # --- happy path ---

    def test_good_data_overall_pass(self):
        report = run_data_quality_checks(self._good_data(), self._good_loss())
        self.assertEqual(report["overall_status"], "pass")

    def test_report_has_required_keys(self):
        report = run_data_quality_checks(self._good_data(), self._good_loss())
        for key in ("overall_status", "feature_checks", "loss_spike_check", "summary"):
            self.assertIn(key, report)

    def test_ignored_cols_absent_from_feature_checks(self):
        report = run_data_quality_checks(self._good_data(), self._good_loss())
        feature_cols = set(report["feature_checks"].keys())
        self.assertNotIn("symbol", feature_cols)
        self.assertNotIn("timestamp", feature_cols)
        self.assertNotIn("target", feature_cols)

    # --- edge cases ---

    def test_near_constant_feature_yields_fail_overall(self):
        df = self._good_data()
        df["feature_a"] = 0.0
        report = run_data_quality_checks(df, self._good_loss())
        self.assertEqual(report["overall_status"], "fail")

    def test_loss_spike_escalates_overall_status(self):
        n = 500
        arr = np.linspace(5.0, 0.01, n)
        arr[400] = arr[399] * (LOSS_SPIKE_FAIL_MULTIPLIER + 50)
        report = run_data_quality_checks(self._good_data(), list(arr))
        self.assertEqual(report["loss_spike_check"]["train"]["status"], "fail")
        self.assertEqual(report["overall_status"], "fail")

    # --- invalid input ---

    def test_custom_ignore_cols_respected(self):
        df = self._good_data()
        df["my_id"] = 0.0  # near-constant numeric, should be ignored
        report = run_data_quality_checks(
            df, self._good_loss(), ignore_cols=["symbol", "timestamp", "target", "my_id"]
        )
        self.assertNotIn("my_id", report["feature_checks"])


if __name__ == "__main__":
    unittest.main()
