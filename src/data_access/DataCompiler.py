import numpy as np
import pandas as pd

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase


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
    ) -> pd.DataFrame:
        query = construct_query(
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
        )
        if print_query:
            print(query)
        data = await self.db.execute_query(
            query, query_type="SELECT", return_type="DataFrame", print_query=print_query
        )
        if "rn" in data.columns:
            data = data.drop(columns=["rn"])
        data.to_csv("data.csv", index=False)
        print(data.dtypes)
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
) -> str:
    inner_query = construct_inner_query(
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
    )
    query = construct_full_query(inner_query, keep_latest_only)
    return query


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
) -> str:
    ctes = []  # common table expressions
    columns = ["c.symbol"]
    joins = ["JOIN Company c ON t.company_id = c.id"]
    filters = []

    if aggregation_interval == "minute":
        table = "TradingData"
        print("minute aggregations are untested!")
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
            f"{start_col} AS timestamp",  # pipeline expects column called timestamp
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
    columns += construct_market_timing_columns(aggregation_interval)

    # hour, day of week, and month of year
    columns += construct_dt_columns(aggregation_interval)

    # news
    news_cte, news_cols, news_joins = construct_news_columns(
        aggregation_interval,
        num_news,
        news_history_threshold,
        get_ids=aggregation_interval == "minute",
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
        filters.append(f"{start_col} >= {min_timestamp}")
    if max_timestamp > 0:
        filters.append(f"{end_col} <= {max_timestamp}")
    if aggregation_interval != "minute":
        filters.append(f"t.interval = '{aggregation_interval}'")

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

    return query


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
            columns = ["(t2.close - t.close) / t.close as target"]
            joins = [
                f"""
LEFT JOIN TradingDataAggregation t2
    ON t2.company_id = t.company_id
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
    return [
        """
        (
            CASE
                WHEN time(datetime(t.end, 'unixepoch', '-5 hours')) < '16:00:00'
                THEN strftime('%s',
                     date(datetime(t.end, 'unixepoch', '-5 hours')) || ' 16:00:00',
                     '+5 hours')
                ELSE strftime('%s',
                     date(datetime(t.end, 'unixepoch', '-5 hours'), '+1 day') || ' 16:00:00',
                     '+5 hours')
            END
        ) - t.end AS seconds_to_next_close
        """,
        """
        CASE
            WHEN time(datetime(t.end, 'unixepoch', '-5 hours')) >= '16:00:00'
                 OR time(datetime(t.end, 'unixepoch', '-5 hours')) < '09:30:00'
            THEN 1 ELSE 0
        END AS is_after_hours
        """,
    ]


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
        columns.append("COS(2 * PI() * t.hour) as hour_cos")
        columns.append("SIN(2 * PI() * t.hour) as hour_sin")
        columns.append(f"CASE strftime('%w', t.date) {weekday_str}")
        columns.append(f"CASE strftime('%m', t.date) {month_str}")

    return columns


def construct_news_columns(
    aggregation_interval,
    num_news,
    news_history_threshold,
    get_ids=False,
):
    if num_news < 1:
        return [], [], []

    # define which tables and columns to use
    if aggregation_interval == "minute":
        t_col = "t.timestamp"
        table = "TradingData"
    elif aggregation_interval == "hour":
        t_col = "t.end"
        table = "TradingDataAggregation"
    else:
        raise ValueError("Unsupported aggregation interval")

    cte = [
        f"""
    RankedNews AS (
    SELECT
        {'n.id AS news_id' if get_ids else 'n.body AS body'},
        n.timestamp,
        t.company_id,
        {t_col} AS trade_ts,
        ROW_NUMBER() OVER (PARTITION BY t.company_id, {t_col} ORDER BY n.timestamp DESC) AS rn
    FROM {table} t
    JOIN NewsCompanyLink ncl ON t.company_id = ncl.company_id
    JOIN News n ON ncl.news_id = n.id
    WHERE n.timestamp <= {t_col} -- TODO might want to simulate the time between news and trading data irl
    AND n.timestamp >= {t_col} - {news_history_threshold}
    )"""
    ]
    columns = []
    joins = []
    for i in range(1, num_news + 1):
        if get_ids:
            columns.append(f"n{i}.news_id AS news{i}_id")
        else:
            columns.append(
                f"c.symbol || ' ' || c.name || ' ' || n{i}.body as news{i}"
            )  # sqlite string concat
        joins.append(
            f"LEFT JOIN RankedNews n{i} ON t.company_id = n{i}.company_id AND {t_col} = n{i}.trade_ts AND n{i}.rn = {i}"
        )
        columns.append(f"{t_col} - n{i}.timestamp AS news{i}_age")
    return cte, columns, joins


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
            print(
                "Coefficient of Variation calculation not supported for data by minute. Turning off cv flag"
            )
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

    # create columns which are comparisons between current data and past data
    calc_columns = []
    for offset in windows:
        for col, include in flag_columns.items():
            # current price / past price for current company in the past window
            # "normalize" past data relative to current data
            calc_columns.append(
                f"(t.{col} / LAG(t.{col}, {offset}) OVER (PARTITION BY t.company_id ORDER BY {ts_col})) AS {col}_ratio_lag_{offset}"
            )
    return calc_columns


def construct_full_query(inner_query, keep_latest_only):
    if not keep_latest_only:
        return inner_query
    query = f"""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY timestamp DESC
               ) AS rn
        FROM ({inner_query})
    )
    WHERE rn = 1;
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
