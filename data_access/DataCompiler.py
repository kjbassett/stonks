import numpy as np
import pandas as pd

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class DataCompiler(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")

    async def get_data(
        self,
        aggregation_interval: str = "minute",
        price_change_offset: int = 86400 * 5,
        min_timestamp: int = 0,
        max_timestamp: int = 0,
        max_window: int = 0,
        num_windows: int = 0,
        num_news: int = 0,
        news_history_threshold: int = 24 * 60 * 60,
        include_close_ratio: bool = True,
        include_cv_close_ratio: bool = True,
        include_avg_volume_ratio: bool = True,
        include_cv_volume_ratio: bool = True,
        print_query: bool = False,
    ) -> pd.DataFrame:
        query = construct_query(
            aggregation_interval,
            price_change_offset,
            min_timestamp,
            max_timestamp,
            max_window,
            num_windows,
            num_news,
            news_history_threshold,
            include_close_ratio,
            include_cv_close_ratio,
            include_avg_volume_ratio,
            include_cv_volume_ratio,
        )
        data = await self.db.execute_query(
            query, query_type="SELECT", return_type="DataFrame", print_query=print_query
        )
        data = data[~data["target"].isnull()]
        print(data.dtypes)
        return data


def construct_query(
    aggregation_interval: str,
    price_change_offset: int = 86400 * 5,  # 1 day in seconds x 5
    min_timestamp: int = 0,
    max_timestamp: int = 0,
    max_window: int = 0,
    num_windows: int = 0,
    num_news: int = 0,
    news_history_threshold: int = 86400,
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
) -> str:
    ctes = []  # common table expressions
    columns = []
    joins = []
    filters = []
    if aggregation_interval == "minute":
        start_col = "t.timestamp"
        end_col = "t.timestamp"
        columns += ["t.close", "t.vw_average", "i.name", "io.name"]
    elif aggregation_interval == "hour":
        start_col = "t.start"
        end_col = "t.end"
        columns += [
            "t.open",
            "t.low",
            "t.close",
            "t.avg_close",
            "t.cv_close",
            "t.price_change",
            "t.avg_volume",
            "t.cv_volume",
            "t.row_count",
            "i.name",
            "io.name",
        ]
        filters.append("t.row_count > 5")
    joins += [
        "JOIN Company c ON t.company_id = c.id",
        "JOIN Industry i ON c.industry_id = i.id",
        "JOIN IndustryOffice io ON i.office_id = io.id",
    ]

    # target column
    columns.append(construct_target_column(aggregation_interval, price_change_offset))

    # hour, day of week, and month of year
    columns += construct_dt_columns(aggregation_interval)

    # news
    news_cte, news_cols, news_joins = construct_news_columns(
        aggregation_interval,
        num_news,
        news_history_threshold,
        get_ids=aggregation_interval == "minute",
    )
    ctes.append(news_cte)
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
    if min_timestamp > 0:
        filters.append(f"{start_col} >= {min_timestamp}")
    if max_timestamp > 0:
        filters.append(f"{end_col} <= {max_timestamp}")
    if aggregation_interval != "minute":
        filters.append(f"interval = '{aggregation_interval}'")

    # format query parts
    ctes = "WITH " + ",\n".join(ctes) + "\n" if ctes else ""
    columns = ",\n".join(columns)
    joins = "\n".join(joins)
    filters = f"WHERE {' AND '.join(filters)}" if filters else " "

    # Construct the query
    query = f"""
{ctes} 
SELECT {columns}
FROM TradingData{"Aggregation" if aggregation_interval != "minute" else ""} t
{joins}
{filters}
ORDER BY {end_col}
"""

    return query


def construct_target_column(aggregation_interval, price_change_offset):
    if aggregation_interval == "minute":
        # TODO Does not account for hours that the market isn't open
        # target column
        pco_min = price_change_offset - 0.02 * price_change_offset
        pco_max = price_change_offset + 0.02 * price_change_offset
        return f"""
CAST(((
    SELECT AVG(t2.close)
    FROM TradingData t2 
    WHERE
        t2.company_id = t.company_id
        AND t2.timestamp >= t.timestamp + {pco_min}
        AND t2.timestamp <= t.timestamp + {pco_max}
) - t.close) / t.close AS REAL) AS target"""

    elif aggregation_interval == "hour":
        # Grab the price_change_offset-th row after the current row
        # Would it be faster to use a window function here?
        return f"""
CAST(((
    SELECT AVG(close)
    FROM (
        SELECT t2.close
        FROM TradingDataAggregation t2
        WHERE
            t2.company_id = t.company_id
            AND t2.interval = 'hour'
            AND (t2.date > t.date OR (t2.date = t.date AND t2.hour > t.hour))
            AND t2.row_count > 5
        ORDER BY t2.date, t2.hour
        LIMIT 1 OFFSET {price_change_offset - 1}
    )
) - t.close) / t.close AS REAL) AS target"""


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
    elif aggregation_interval == "hour":
        columns.append("t.hour")
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
        return "", [], []

    # define which tables and columns to use
    if aggregation_interval == "minute":
        t_col = "t.timestamp"
        table = "TradingData"
    elif aggregation_interval == "hour":
        t_col = "t.end"
        table = "TradingDataAggregation"
    else:
        raise ValueError("Unsupported aggregation interval")

    cte = f"""
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
    columns = []
    joins = []
    for i in range(1, num_news + 1):
        if get_ids:
            columns.append(f"n{i}.news_id AS news{i}_id")
        else:
            columns.append(f"c.symbol || ' ' || c.name || ' ' || n{i}.body as news{i}")
        joins.append(
            f"LEFT JOIN RankedNews n{i} ON t.company_id = n{i}.company_id AND {t_col} = n{i}.trade_ts AND n{i}.rn = {i}"
        )

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
    if aggregation_interval == "minute":
        ts_col = "t.timestamp"
        if include_cv_close_ratio:
            print(
                "Coefficient of Variation calculation not supported for data by minute. Turning off cv flag"
            )
            include_cv_close_ratio = False
    elif aggregation_interval in ("hour"):
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
    windows = np.linspace(0, max_window, num_windows + 1, dtype=int)[1:]
    windows = set(
        windows
    )  # ensure we don't have duplicate window sizes. dtype=int could cause duplicates

    # create columns which are comparisons between current data and past data
    calc_columns = []
    for offset in windows:
        for col, include in flag_columns.items():
            # current price / past price for current company in the past window
            calc_columns.append(
                f"(t.{col} / LAG({col}, {offset}) OVER (PARTITION BY t.company_id ORDER BY {ts_col})) AS {col}_ratio_lag_{offset}"
            )
    return calc_columns


# TODO
#  Get diff of current timestamp and news timestamps
#  Verify hour is correct (and day of week and month with same fix if needed). Just put timestamp into data and convert it online.
#  How to tokenize company in text?

"""
Developer notes
You can include the following data:
data from current hour
data from current hour standardized to current company
data from n hours ago
data from n hours ago standardized to current company
data from n hours ago compared to current hour
data from n hours ago compared to next offset

You probably want data from now for the model to be able to compare the business to others. vertical
Then the past data should be relative to itself currently. horizontal
Current is always "in the middle" relative to past to maintain comparability.
"""
