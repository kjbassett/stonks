import unittest

from src.data_access.DataCompiler import (
    construct_calculated_columns,
    construct_news_columns,
    construct_query,
)


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


class TestConstructNewsColumns(unittest.TestCase):
    """The RankedNews CTE must stay scoped to the requested companies/time
    window — otherwise it joins News/NewsCompanyLink against all history for
    every company on every call (confirmed: this took a query from 26s to 58
    minutes in production before this scoping was added)."""

    def test_returns_empty_when_num_news_zero(self):
        result = construct_news_columns("hour", 0, 86400)
        self.assertEqual(result, ([], [], [], ()))

    def test_cte_scopes_to_symbols_when_given(self):
        cte, _cols, _joins, params = construct_news_columns(
            "hour", 1, 86400, symbols=["AAPL", "MSFT"]
        )
        cte_text = cte[0]
        self.assertIn("JOIN Company c2 ON t.company_id = c2.id", cte_text)
        self.assertIn("c2.symbol IN (?,?)", cte_text)
        self.assertEqual(params, ("AAPL", "MSFT"))

    def test_cte_omits_company_scope_when_no_symbols(self):
        cte, _cols, _joins, params = construct_news_columns("hour", 1, 86400)
        self.assertNotIn("Company c2", cte[0])
        self.assertEqual(params, ())

    def test_cte_scopes_to_min_timestamp(self):
        cte, _cols, _joins, _params = construct_news_columns(
            "hour", 1, 86400, min_timestamp=1700000000
        )
        self.assertIn("t.end >= 1700000000", cte[0])

    def test_cte_scopes_to_max_timestamp(self):
        cte, _cols, _joins, _params = construct_news_columns(
            "hour", 1, 86400, max_timestamp=1800000000
        )
        self.assertIn("t.end <= 1800000000", cte[0])

    def test_cte_omits_time_bounds_when_unset(self):
        cte, _cols, _joins, _params = construct_news_columns("hour", 1, 86400)
        self.assertNotIn("t.end >=", cte[0])
        self.assertNotIn("t.end <=", cte[0])

    def test_columns_and_joins_scale_with_num_news(self):
        _cte, cols, joins, _params = construct_news_columns("hour", 2, 86400)
        self.assertEqual(len(joins), 2)
        col_text = " ".join(cols)
        self.assertIn("news1_id", col_text)
        self.assertIn("news2_id", col_text)
        self.assertIn("n1.rn = 1", joins[0])
        self.assertIn("n2.rn = 2", joins[1])

    def test_minute_aggregation_uses_trading_data(self):
        cte, _cols, _joins, _params = construct_news_columns("minute", 1, 86400)
        self.assertIn("FROM TradingData t", cte[0])
        self.assertIn("t.timestamp AS trade_ts", cte[0])

    def test_hour_aggregation_uses_trading_data_aggregation(self):
        cte, _cols, _joins, _params = construct_news_columns("hour", 1, 86400)
        self.assertIn("FROM TradingDataAggregation t", cte[0])
        self.assertIn("t.end AS trade_ts", cte[0])

    def test_unsupported_aggregation_raises(self):
        with self.assertRaises(ValueError):
            construct_news_columns("daily", 1, 86400)


class TestConstructQueryNewsParamOrdering(unittest.TestCase):
    """The CTE's ? placeholders appear before the outer query's in the final
    SQL text, so params must be ordered to match — a mismatch here silently
    binds the wrong values to the wrong placeholders."""

    def test_news_params_precede_outer_symbol_params(self):
        symbols = ["AAPL", "MSFT"]
        query, params = construct_query(
            "hour",
            min_timestamp=1700000000,
            num_news=1,
            news_history_threshold=86400,
            symbols=symbols,
        )
        # Both the CTE's Company-scoping join and the outer c.symbol IN (...)
        # filter bind the same two symbols, so params should contain them twice.
        self.assertEqual(params, ("AAPL", "MSFT", "AAPL", "MSFT"))
        # Sanity: the CTE text (containing the first placeholder pair) really
        # does appear before the outer SELECT's filter in the final query.
        cte_pos = query.find("RankedNews")
        outer_filter_pos = query.find("c.symbol IN")
        self.assertGreater(outer_filter_pos, cte_pos)

    def test_no_news_params_when_num_news_zero(self):
        query, params = construct_query(
            "hour", min_timestamp=1700000000, num_news=0, symbols=["AAPL"]
        )
        self.assertEqual(params, ("AAPL",))
        self.assertNotIn("RankedNews", query)


if __name__ == "__main__":
    unittest.main()
