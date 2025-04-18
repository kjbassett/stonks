import time

import numpy as np
import pandas as pd

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class DataCompiler(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")

    async def get_data(
        self,
        price_change_offset: int = 0,
        min_timestamp: int = 0,
        max_timestamp: int = 0,
        max_window: int = 0,
        num_windows: int = 0,
        num_news: int = 0,
        news_history_threshold: int = 24 * 60 * 60,
        include_price_change: bool = False,
        include_volume_change: bool = False,
        include_coeff_var: bool = False,
        include_price_over_average: bool = False,
        include_volume_over_average: bool = False,
        print_query: bool = False
    ) -> pd.DataFrame:
        query = construct_query(
            price_change_offset,
            min_timestamp,
            max_timestamp,
            max_window,
            num_windows,
            num_news,
            news_history_threshold,
            include_price_change,
            include_volume_change,
            include_coeff_var,
            include_price_over_average,
            include_volume_over_average,
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
    include_price_change: bool = False,
    include_volume_change: bool = False,
    include_coeff_var: bool = False,
    include_price_over_average: bool = False,
    include_volume_over_average: bool = False,
) -> str:
    ctes = []  # common table expressions
    columns = []
    joines = []
    if aggregation_interval == "minute":
        columns += ["t.close", "t.vw_average", 'i.name', 'io.name']
    elif aggregation_interval == "hour":
        columns += ['t.open', 't.low', 't.close', 't.avg_close', 't.cv_close',
                    't.price_change', 't.avg_volume', 't.cv_volume', 't.row_count', 'i.name', 'io.name']
    joins += ["JOIN Company c ON t.company_id = c.id",
              "JOIN Industry i ON c.industry_id = i.id",
              "JOIN IndustryOffice io ON i.office_id = io.id"]


    # target column
    columns.append(construct_target_column(aggregation_interval, price_change_offset))

    # hour, day of week, and month of year
    columns += construct_dt_columns(aggregation_interval)

     # news ids
    news_cte, news_cols, news_joins = construct_news_columns(aggregation_interval, num_news)
    ctes.append(news_cte)
    columns += news_cols
    joins += news_joins

    # Add statistics for each window
    if max_window > 0 and num_windows > 0:
        window_sizes = np.linspace(0, max_window, num_windows + 1, dtype=int)[1:]
        window_sizes = set(window_sizes)  # ensure we don't have duplicate window sizes
        for offset in window_sizes:
            # current price / past price
            if include_price_change:
                columns.append(
                    f"(t.close / LAG(t.close, {offset}) OVER (PARTITION BY t.company_id ORDER BY t.timestamp)) AS price_ratio_window_{offset}"
                )
            # current volume / past volume
            if include_volume_change:
                columns.append(
                    f"(t.volume / LAG(t.volume, {offset}) OVER (PARTITION BY t.company_id ORDER BY t.timestamp)) AS volume_ratio_window_{offset}"
                )
            # coefficient of variation of price over window
            if include_coeff_var:
                columns.append(
                    f"""
    SQRT(
        AVG(t.close * t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW) - 
        (
            AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW) * 
            AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW)
        )
    ) / AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW) as coeff_var_window_{offset}"""
                )
            # current price / average price over window
            if include_price_over_average:
                columns.append(
                    f"(t.close / AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW)) AS price_avg_ratio_window_{offset}"
                )
            # current volume / average volume over window
            if (
                include_volume_over_average
            ):  # current volume / average volume over window
                columns.append(
                    f"(t.volume / AVG(t.volume) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {offset} PRECEDING AND CURRENT ROW)) AS volume_avg_ratio_window_{offset}"
                )

    # format query parts
    ctes = "WITH " + ",\n".join(ctes) + "\n" if ctes else ""
    columns = ",\n".join(columns)
    joins = "\n".join(joins)

    # Construct the query
    query = f"""
{ctes} 
SELECT {columns}
FROM TradingData{"Aggregations" if aggregation_interval != "minute" else ""} t
{joins}
WHERE t.timestamp >= {min_timestamp} and t.timestamp <= {max_timestamp if max_timestamp else time.time()}
ORDER BY t.timestamp
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
        return f"""
CAST(((
    SELECT AVG(t2.close)
    FROM (
        SELECT t2.close
        FROM TradingDataAggregation t2
        WHERE
            t2.company_id = t.company_id
            AND t2.interval = 'hourly'
            AND (t2.date > t.date OR (t2.date = t.date AND t2.hour > t.hour))
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
        columns.append("CAST(strftime('%H', datetime(t.timestamp, 'unixepoch')) AS INTEGER) AS hour")
        columns.append(f"CASE strftime('%w', datetime(t.timestamp, 'unixepoch')) {weekday_str}")
        columns.append(f"CASE strftime('%m', datetime(t.timestamp, 'unixepoch')) {month_str}")
    elif aggregation_interval == "hour":
        columns.append("t.hour")
        columns.append(f"CASE strftime('%w', t.date) {weekday_str}")
        columns.append(f"CASE strftime('%m', t.date) {month_str}")


def construct_news_columns(aggregation_interval, num_news, news_history_threshold, get_ids=False):
    if num_news < 1:
        return "", [], []

    if aggregation_interval == "minute":
        t_col = "t.timestamp"
        table = "TradingData"
    elif aggregation_interval == "hour":
        t_col = "t.end"
        table = "TradingDataAggregation"
    else:
        raise ValueError("Unsupported aggregation interval")

    if get_ids:
        formula = "n.id"
        data_col = "news_id"
    elif not get_ids:
        formula = "CONCAT(t.symbol, ' ', t.name, ' ', n.body)"
        data_col = "news_text"
    else:
        raise ValueError("Unsupported value for variable 'get_ids'")

    cte = f"""
    RankedNews AS (
    SELECT
        {formula} AS {data_col},
        n.timestamp,
        t.company_id,
        {t_col} AS trade_ts,
        ROW_NUMBER() OVER (PARTITION BY t.company_id, t.timestamp ORDER BY n.timestamp DESC) AS rn
    FROM {table} t
    JOIN NewsCompanyLink ncl ON t.company_id = ncl.company_id
    JOIN News n ON ncl.news_id = n.id
    WHERE n.timestamp <= {t_col} -- TODO might want to simulate the time between news and trading data irl
    AND n.timestamp >= {t_col} - {news_history_threshold}
    )"""
    columns = []
    joins = []
    for i in range(1, num_news + 1):
        columns.append(f"n{i}.news_id AS news{i}_id")
        joins.append(
            f"LEFT JOIN RankedNews n{i} ON t.company_id = n{i}.company_id AND {t_col} = n{i}.trade_ts AND n{i}.rn = {i}"
        )

    return cte, columns, joins

# TODO
#  Get diff of current timestamp and news timestamps
#  Verify hour is correct (and day of week and month with same fix if needed). Just put timestamp into data and convert it online.
#  How to tokenize company in text?
