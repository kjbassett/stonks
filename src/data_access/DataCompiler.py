import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_log = logging.getLogger("data_access.data_compiler")


class DataCompiler(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")

    async def get_data(
        self,
        aggregation_interval: str = "minute",
        min_timestamp: int = 0,
        max_timestamp: int = 0,
        max_window: int = 0,
        num_windows: int = 0,
        num_news: int = 0,
        news_history_threshold: int = 24 * 60 * 60,
        include_target: bool = True,
        target_offset: any = "next_close",
        include_close_ratio: bool = True,
        include_cv_close_ratio: bool = True,
        include_avg_volume_ratio: bool = True,
        include_cv_volume_ratio: bool = True,
        keep_latest_only: bool = False,
        print_query: bool = False,
        symbols: Optional[List[str]] = None,
        include_after_hours: bool = True,
    ) -> pd.DataFrame:
        query, params = construct_query(
            aggregation_interval,
            min_timestamp,
            max_timestamp,
            max_window,
            num_windows,
            num_news,
            news_history_threshold,
            include_target,
            target_offset,
            include_close_ratio,
            include_cv_close_ratio,
            include_avg_volume_ratio,
            include_cv_volume_ratio,
            keep_latest_only=keep_latest_only,
            symbols=symbols if isinstance(symbols, list) else None,
            include_after_hours=include_after_hours,
        )
        if print_query:
            _log.debug("Query: %s", query)
        data = await self.db.execute_query(
            query, params, query_type="SELECT", return_type="DataFrame"
        )
        for col in data.columns:
            if col[:2] == "rn":
                data = data.drop(columns=[col])
        _log.debug("Data dtypes: %s", data.dtypes.to_dict())
        return data


def construct_query(
    aggregation_interval: str,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 86400,
    include_target: bool = True,
    target_offset: any = "next_close",
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
    keep_latest_only: bool = False,
    symbols: Optional[List[str]] = None,
    include_after_hours: bool = True,
) -> tuple[str, tuple]:
    inner_query, params = construct_inner_query(
        aggregation_interval,
        min_timestamp,
        max_timestamp,
        max_window,
        num_windows,
        num_news,
        news_history_threshold,
        include_target,
        target_offset,
        include_close_ratio,
        include_cv_close_ratio,
        include_avg_volume_ratio,
        include_cv_volume_ratio,
        symbols=symbols,
    )
    query = construct_full_query(
        inner_query, keep_latest_only, include_target, target_offset, include_after_hours
    )
    return query, params


def construct_inner_query(
    aggregation_interval: str,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 86400,
    include_target: bool = True,
    target_offset: any = "next_close",
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
    symbols: Optional[List[str]] = None,
) -> tuple[str, tuple]:
    ctes = []  # common table expressions
    columns = ["c.symbol"]
    joins = [
        "JOIN Company c ON t.company_id = c.id",
        "LEFT JOIN Exchange e ON c.primary_exchange = e.market_id",
        "LEFT JOIN TickerType tt ON c.ticker_type_id = tt.id",
    ]
    filters = ["c.enabled = 1", "e.active IS NOT 0", "tt.enabled IS NOT 0"]

    if aggregation_interval == "minute":
        table = "TradingData"
        _log.warning("Minute aggregations are untested")
        start_col = end_col = "t.timestamp"
        columns += [
            start_col,
            "t.vw_average",
            "i.name AS industry",
            "io.name AS industry_office",
        ]
    else:
        table = "TradingDataAggregation"
        start_col = "t.start"
        end_col = "t.end"
        columns += [
            f"{end_col} AS timestamp",  # bar end — when all bar features are available
            "t.open",
            "t.low",
            "t.close",
            "t.avg_close",
            "t.cv_close",
            "t.price_change",
            "t.avg_volume",
            "t.cv_volume",
            "t.row_count",
            "i.name AS industry",
            "io.name AS industry_office",
        ]
        filters.append("t.row_count > 5")
    joins += [
        "JOIN Industry i ON c.industry_id = i.id",
        "JOIN IndustryOffice io ON i.office_id = io.id",
    ]

    # target column
    if include_target:
        target_cols, target_joins, target_filters = construct_target_column(
            aggregation_interval, target_offset
        )
        columns += target_cols
        joins += target_joins
        filters += target_filters

    # market timing columns
    timing_columns, timing_joins = construct_market_timing_columns(aggregation_interval)
    columns += timing_columns
    joins += timing_joins

    # hour, day of week, and month of year
    columns += construct_dt_columns(aggregation_interval)

    # news
    news_cte, news_cols, news_joins, news_params = construct_news_columns(
        aggregation_interval,
        num_news,
        news_history_threshold,
        min_timestamp,
        max_timestamp,
        symbols,
    )
    ctes += news_cte
    columns += news_cols
    joins += news_joins

    # Add statistics for each window
    columns += construct_calculated_columns(
        aggregation_interval,
        max_window,
        num_windows,
        include_close_ratio,
        include_cv_close_ratio,
        include_avg_volume_ratio,
        include_cv_volume_ratio,
    )

    # Add filters
    if min_timestamp != 0:
        filters.append(f"{end_col} >= {min_timestamp}")
    if max_timestamp > 0:
        filters.append(f"{end_col} <= {max_timestamp}")
    if aggregation_interval != "minute":
        filters.append(f"t.interval = '{aggregation_interval}'")

    # news_params (if any) come from the CTE, which is prepended before this
    # SELECT in the final query text, so its ? placeholders must bind first.
    params: tuple = news_params
    if symbols:
        placeholders = ",".join("?" * len(symbols))
        filters.append(f"c.symbol IN ({placeholders})")
        params = params + tuple(symbols)

    # format query parts
    ctes = "WITH " + ",\n".join(ctes) + "\n" if ctes else ""
    columns = ",\n".join(columns)
    joins = "\n".join(joins)
    filters = f"WHERE {' AND '.join(filters)}" if filters else " "

    # Construct the query
    query = f"""
{ctes}
SELECT {columns}
FROM {table} t
{joins}
{filters}
ORDER BY {end_col}
"""

    return query, params


def construct_target_column(aggregation_interval, offset):
    if aggregation_interval == "minute":
        raise NotImplementedError(
            "construct_target_column not implemented for minute aggregations"
        )

    elif aggregation_interval == "hour":
        if offset == "next_close":
            columns = ["CAST((tt.close - t.close) / t.close AS REAL) as target"]
            # join on close of next market day according to market calendar table
            joins = [
                """
LEFT JOIN TradingDataAggregation tt
    ON t.company_id = tt.company_id
    AND t.interval = tt.interval
    AND (
        SELECT mc.last_market_hour
        FROM MarketCalendar mc
        WHERE mc.close_ts > t.end + 3600 -- next closing timestamp, but final hour should look at next day
        ORDER BY mc.close_ts
        LIMIT 1
    ) = tt.hour
    AND date(datetime((
        SELECT mc.close_ts
        FROM MarketCalendar mc
        WHERE mc.close_ts > t.end
        ORDER BY mc.close_ts
        LIMIT 1
    ), 'unixepoch')) = tt.date
                """
            ]
            filters = ["tt.interval = 'hour'"]
        elif isinstance(offset, int):
            columns = [
                "(t2.close - t.close) / t.close as target",
                "ROW_NUMBER() OVER (PARTITION BY t.company_id, t.end ORDER BY ABS(t2.end - t.end)) as rn_target",
            ]
            joins = [
                f"""
LEFT JOIN TradingDataAggregation t2
    ON t2.company_id = t.company_id
    AND t.interval = t2.interval
    AND t2.end >= t.end + 3600 * {offset * 0.9}
    AND t2.end <= t.end + 3600 * {offset * 1.1}"""
            ]
            filters = []
        else:
            raise ValueError('target offset must be int or "next_close"')

        return columns, joins, filters


def construct_market_timing_columns(aggregation_interval):
    if aggregation_interval == "minute":
        raise NotImplementedError(
            "construct_market_timing_columns not implemented for minute aggregations"
        )
    # Uses MarketCalendar (pandas_market_calendars-derived, real UTC open/close
    # timestamps per trading day) instead of a hardcoded UTC offset — correct
    # across DST transitions and market holidays/half-days, unlike the '-5 hours'
    # CASE expression this replaces.
    joins = ["LEFT JOIN MarketCalendar mkt_cal ON mkt_cal.date = t.date"]
    columns = [
        """
        (
            SELECT mc2.close_ts FROM MarketCalendar mc2
            WHERE mc2.close_ts > t.end
            ORDER BY mc2.close_ts LIMIT 1
        ) - t.end AS seconds_to_next_close
        """,
        """
        CASE
            WHEN mkt_cal.date IS NULL THEN NULL
            WHEN t.end > mkt_cal.open_ts AND t.end <= mkt_cal.close_ts THEN 0
            ELSE 1
        END AS is_after_hours
        """,
    ]
    return columns, joins


def construct_dt_columns(aggregation_interval):
    columns = []
    weekday_str = """
    WHEN '0' THEN 'Sunday'
    WHEN '1' THEN 'Monday'
    WHEN '2' THEN 'Tuesday'
    WHEN '3' THEN 'Wednesday'
    WHEN '4' THEN 'Thursday'
    WHEN '5' THEN 'Friday'
    WHEN '6' THEN 'Saturday'
    END AS day_of_week
    """
    month_str = """
    WHEN '01' THEN 'January'
    WHEN '02' THEN 'February'
    WHEN '03' THEN 'March'
    WHEN '04' THEN 'April'
    WHEN '05' THEN 'May'
    WHEN '06' THEN 'June'
    WHEN '07' THEN 'July'
    WHEN '08' THEN 'August'
    WHEN '09' THEN 'September'
    WHEN '10' THEN 'October'
    WHEN '11' THEN 'November'
    WHEN '12' THEN 'December'
    END AS month_name"""

    # Add hour and weekday name, if hourly, we already have date and hour in TradingDataAggregation
    if aggregation_interval == "minute":
        columns.append(
            "CAST(strftime('%H', datetime(t.timestamp, 'unixepoch')) AS INTEGER) AS hour"
        )
        columns.append(
            f"CASE strftime('%w', datetime(t.timestamp, 'unixepoch')) {weekday_str}"
        )
        columns.append(
            f"CASE strftime('%m', datetime(t.timestamp, 'unixepoch')) {month_str}"
        )
        # TODO need cos & sin for minute agg
    elif aggregation_interval == "hour":
        columns.append("t.hour")
        columns.append("COS(2 * PI() * t.hour / 24) as hour_cos")
        columns.append("SIN(2 * PI() * t.hour / 24) as hour_sin")
        columns.append(f"CASE strftime('%w', t.date) {weekday_str}")
        columns.append(f"CASE strftime('%m', t.date) {month_str}")

    return columns


def construct_news_columns(
    aggregation_interval: str,
    num_news: int,
    news_history_threshold: int,
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    symbols: Optional[List[str]] = None,
) -> tuple:
    """Build CTEs, columns, joins, and params for news ID, sentiment, and age features.

    Args:
        aggregation_interval: Aggregation level ('minute' or 'hour').
        num_news: Number of most-recent news articles to include per row.
        news_history_threshold: Maximum news age in seconds.
        min_timestamp: Same lower bound the outer query applies — restricts the
            CTE's own driving scan to the requested window instead of all history.
        max_timestamp: Same upper bound the outer query applies.
        symbols: Same symbol filter the outer query applies — restricts the CTE's
            driving scan to the requested companies instead of every company.

    Returns:
        A 4-tuple of (ctes, columns, joins, params).
    """
    if num_news < 1:
        return [], [], [], ()

    if aggregation_interval == "minute":
        t_col = "t.timestamp"
        table = "TradingData"
    elif aggregation_interval == "hour":
        t_col = "t.end"
        table = "TradingDataAggregation"
    else:
        raise ValueError(f"Unsupported aggregation interval: {aggregation_interval}")

    # The CTE is its own independently-scoped FROM {table} t — it does not
    # inherit the outer query's company/time filters (SQLite can't push
    # predicates through the ROW_NUMBER() window function). Without these,
    # it joins News/NewsCompanyLink against the *entire* history for *every*
    # company on every call, regardless of how narrow the actual request is
    # (confirmed: this alone took a 26s query to 58 minutes at ~100 symbols /
    # 14 days against 22M+ TradingDataAggregation rows). Mirroring the same
    # bounds the outer query already applies keeps this CTE's driving scan
    # limited to what's actually requested.
    cte_joins = [
        "JOIN NewsCompanyLink ncl ON t.company_id = ncl.company_id",
        "JOIN News n ON ncl.news_id = n.id",
    ]
    cte_filters = [
        f"n.timestamp <= {t_col}",
        f"n.timestamp >= {t_col} - {news_history_threshold}",
    ]
    params: tuple = ()
    if min_timestamp != 0:
        cte_filters.append(f"{t_col} >= {min_timestamp}")
    if max_timestamp > 0:
        cte_filters.append(f"{t_col} <= {max_timestamp}")
    if symbols:
        placeholders = ",".join("?" * len(symbols))
        cte_joins.append(
            f"JOIN Company c2 ON t.company_id = c2.id AND c2.symbol IN ({placeholders})"
        )
        params = tuple(symbols)

    cte_joins_str = "\n    ".join(cte_joins)
    cte = [
        f"""
    RankedNews AS (
    SELECT
        n.id AS news_id,
        ncl.sentiment,
        n.timestamp,
        t.company_id,
        {t_col} AS trade_ts,
        ROW_NUMBER() OVER (PARTITION BY t.company_id, {t_col} ORDER BY n.timestamp DESC) AS rn
    FROM {table} t
    {cte_joins_str}
    WHERE {' AND '.join(cte_filters)}
    )"""
    ]
    columns = []
    joins = []
    for i in range(1, num_news + 1):
        columns.append(f"n{i}.news_id AS news{i}_id")
        columns.append(
            f"CASE n{i}.sentiment "
            f"WHEN 'positive' THEN 1.0 "
            f"WHEN 'negative' THEN -1.0 "
            f"ELSE 0.0 END AS news{i}_sentiment"
        )
        columns.append(f"{t_col} - n{i}.timestamp AS news{i}_age")
        joins.append(
            f"LEFT JOIN RankedNews n{i} ON t.company_id = n{i}.company_id"
            f" AND {t_col} = n{i}.trade_ts AND n{i}.rn = {i}"
        )
    return cte, columns, joins, params


def construct_calculated_columns(
    aggregation_interval,
    max_window,
    num_windows,
    include_close_ratio=True,
    include_cv_close_ratio=True,
    include_avg_volume_ratio=True,
    include_cv_volume_ratio=True,
):
    # guard clause
    if max_window == 0 or num_windows == 0:
        return []

    # determine settings
    # choose timestamp column
    if aggregation_interval == "minute":
        ts_col = "t.timestamp"
        if include_cv_close_ratio:
            _log.warning("CV calculation not supported for minute data — disabling include_cv_close_ratio")
            include_cv_close_ratio = False
    elif aggregation_interval == "hour":
        ts_col = "t.end"
    else:
        raise ValueError("Unsupported aggregation interval")

    # iterate through this and create all relative (ratio) columns
    flag_columns = {
        "close": include_close_ratio,
        "cv_close": include_cv_close_ratio,
        "avg_volume": include_avg_volume_ratio,
        "cv_volume": include_cv_volume_ratio,
    }
    # create each relative column for each window
    windows = np.linspace(0, max_window, num_windows + 1, dtype=int)
    # ensure we don't have duplicate window sizes. dtype=int could cause duplicates
    windows = list(dict.fromkeys(windows))
    # first window is always 0, which is just the current row
    windows = windows[1:]

    # Minimum lagged value for cv columns before treating the ratio as NULL.
    # cv_close can suffer catastrophic cancellation (sqrt(a²-a²) ≈ 1e-8 instead
    # of 0.0), producing ratios in the hundreds of thousands.  A floor of 1e-4
    # (well above float noise, well below the typical cv value of 0.001–0.01)
    # converts those pathological rows to NULL so the missing-data filter drops
    # them rather than letting them corrupt z-score normalization.
    _cv_ratio_floor = {
        "cv_close": 1e-4,
        "cv_volume": 1e-2,
    }

    # create columns which are comparisons between current data and past data
    calc_columns = []
    for offset in windows:
        for col, include in flag_columns.items():
            if not include:
                continue
            lag = (
                f"LAG(t.{col}, {offset}) OVER "
                f"(PARTITION BY t.company_id ORDER BY {ts_col})"
            )
            floor = _cv_ratio_floor.get(col, 0.0)
            if floor > 0.0:
                expr = f"(CASE WHEN {lag} < {floor} THEN NULL ELSE t.{col} / {lag} END)"
            else:
                expr = f"(t.{col} / {lag})"
            calc_columns.append(f"{expr} AS {col}_ratio_lag_{offset}")
    return calc_columns


def construct_full_query(
    inner_query, keep_latest_only, include_target, target_offset, include_after_hours=True
):
    # Both conditions filter on columns the inner query already computes
    # (rn_target from construct_target_column's row-numbering, is_after_hours
    # from construct_market_timing_columns) — they must stay outer-query
    # conditions, never pushed into construct_inner_query's own filters, since
    # the inner query's window functions (LAG-based lag features, the target's
    # forward join) need the complete, unfiltered per-company row sequence to
    # produce correct values. Excluding after-hours rows only means they don't
    # become training examples themselves — an in-hours row whose target or lag
    # legitimately depends on an after-hours timestamp still needs that row
    # present while the inner query runs.
    conditions = []
    if isinstance(target_offset, int) and include_target:
        conditions.append("rn_target = 1")
    if not include_after_hours:
        conditions.append("is_after_hours = 0")

    if not keep_latest_only:
        if conditions:
            return f"SELECT * FROM ({inner_query}) WHERE {' AND '.join(conditions)}"
        return inner_query

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY timestamp DESC
               ) AS rn_latest_only
        FROM ({inner_query})
        {where_clause}
    )
    WHERE rn_latest_only = 1
    """
    return query


# TODO
#  Verify hour is correct (and day of week and month with same fix if needed). Just put timestamp into data and convert it online.

"""
Developer notes
You can include the following data:
data from current hour
data from current hour standardized relative to current company
data from n hours ago
data from n hours ago standardized relative to current company
data from n hours ago compared to current hour
data from n hours ago compared to next offset

You probably want data from now for the model to be able to compare the business to others. vertical
Then the past data should be relative to itself currently. horizontal
Current is always the "control group" (=1) relative to past to maintain comparability.
"""
