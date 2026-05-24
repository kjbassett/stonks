import unittest

from src.data_access.DataCompiler import construct_calculated_columns


class TestConstructCalculatedColumns(unittest.TestCase):
    def _get_col_names(self, cols):
        return [c.split(" AS ")[-1] for c in cols]

    def _get_expr(self, cols, alias):
        for c in cols:
            if c.endswith(f"AS {alias}"):
                return c
        return None

    # --- happy path ---

    def test_returns_expected_column_count_all_enabled(self):
        cols = construct_calculated_columns(
            "hour", max_window=4, num_windows=2,
            include_close_ratio=True,
            include_cv_close_ratio=True,
            include_avg_volume_ratio=True,
            include_cv_volume_ratio=True,
        )
        # 2 windows × 4 columns = 8
        self.assertEqual(len(cols), 8)

    def test_cv_close_uses_case_when_floor(self):
        cols = construct_calculated_columns(
            "hour", max_window=2, num_windows=1,
            include_cv_close_ratio=True,
        )
        expr = self._get_expr(cols, "cv_close_ratio_lag_2")
        self.assertIsNotNone(expr)
        self.assertIn("CASE WHEN", expr)
        self.assertIn("0.0001", expr)
        self.assertIn("NULL", expr)

    def test_cv_volume_uses_case_when_floor(self):
        cols = construct_calculated_columns(
            "hour", max_window=2, num_windows=1,
            include_cv_volume_ratio=True,
        )
        expr = self._get_expr(cols, "cv_volume_ratio_lag_2")
        self.assertIsNotNone(expr)
        self.assertIn("CASE WHEN", expr)
        self.assertIn("0.01", expr)

    def test_close_ratio_uses_plain_division(self):
        cols = construct_calculated_columns(
            "hour", max_window=2, num_windows=1,
            include_close_ratio=True,
            include_cv_close_ratio=False,
            include_avg_volume_ratio=False,
            include_cv_volume_ratio=False,
        )
        self.assertEqual(len(cols), 1)
        expr = cols[0]
        self.assertNotIn("CASE WHEN", expr)
        self.assertIn("close_ratio_lag_2", expr)

    # --- include flag behaviour ---

    def test_disabled_cv_close_ratio_excluded(self):
        cols = construct_calculated_columns(
            "hour", max_window=2, num_windows=1,
            include_close_ratio=False,
            include_cv_close_ratio=False,
            include_avg_volume_ratio=False,
            include_cv_volume_ratio=False,
        )
        self.assertEqual(cols, [])

    def test_only_close_ratio_included(self):
        cols = construct_calculated_columns(
            "hour", max_window=4, num_windows=2,
            include_close_ratio=True,
            include_cv_close_ratio=False,
            include_avg_volume_ratio=False,
            include_cv_volume_ratio=False,
        )
        names = self._get_col_names(cols)
        self.assertTrue(all("close_ratio_lag" in n for n in names))
        self.assertFalse(any("cv_" in n for n in names))
        self.assertFalse(any("volume" in n for n in names))

    # --- edge cases ---

    def test_zero_max_window_returns_empty(self):
        cols = construct_calculated_columns("hour", max_window=0, num_windows=5)
        self.assertEqual(cols, [])

    def test_zero_num_windows_returns_empty(self):
        cols = construct_calculated_columns("hour", max_window=10, num_windows=0)
        self.assertEqual(cols, [])

    def test_cv_close_disabled_for_minute_aggregation(self):
        cols = construct_calculated_columns(
            "minute", max_window=2, num_windows=1,
            include_cv_close_ratio=True,
        )
        names = self._get_col_names(cols)
        self.assertFalse(any("cv_close" in n for n in names))

    # --- invalid input ---

    def test_unsupported_aggregation_raises(self):
        with self.assertRaises(ValueError):
            construct_calculated_columns("daily", max_window=2, num_windows=1)

    def test_lag_window_partitioned_by_company_id(self):
        cols = construct_calculated_columns(
            "hour", max_window=2, num_windows=1,
            include_close_ratio=True,
            include_cv_close_ratio=False,
            include_avg_volume_ratio=False,
            include_cv_volume_ratio=False,
        )
        self.assertIn("PARTITION BY t.company_id", cols[0])


if __name__ == "__main__":
    unittest.main()
