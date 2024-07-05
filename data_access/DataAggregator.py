import pandas as pd

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class DataAggregator(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "")

    async def get_data(
        self,
        company_id: int = None,
        min_timestamp: int = None,
        max_timestamp: int = None,
        avg_close: bool = True,
        avg_volume: bool = True,
        std_dev: bool = True,
        windows: iter = None,
        n_news: int = 3,
        news_relative_age_threshold: int = 24 * 60 * 60,
    ) -> pd.DataFrame:
        if windows is None:
            windows = [4, 19, 59, 389]

        query = construct_query(
            company_id,
            min_timestamp,
            max_timestamp,
            avg_close,
            avg_volume,
            std_dev,
            windows,
            news_relative_age_threshold,
            n_news,
        )
        print(query)
        data = await self.db.execute_query(
            query, query_type="SELECT", return_type="DataFrame"
        )

        if min_timestamp:
            data = data[data["timestamp"] >= min_timestamp]
        if max_timestamp:
            data = data[data["timestamp"] <= max_timestamp]

        return data


def construct_query(
    company_id,
    min_timestamp,
    max_timestamp,
    avg_close,
    avg_volume,
    std_dev,
    windows,
    news_relative_age_threshold,
    n_news,
):
    CTEs = []
    columns = [
        "t.company_id",
        "t.timestamp",
        "t.open",
        "t.high",
        "t.low",
        "t.close",
        "t.vw_average",
        "t.volume",
        "c.symbol",
        "c.industry",
    ]
    # Get various statistics over each window
    for window in windows:
        if avg_close:
            columns.append(
                f"AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) AS ma_{window}"
            )
        if avg_volume:
            columns.append(
                f"AVG(t.volume) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) AS avg_volume_{window}"
            )
        if std_dev:
            columns.append(
                f"""SQRT(AVG(t.close * t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) - 
                    (AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) * 
                    AVG(t.close) OVER (PARTITION BY t.company_id ORDER BY t.timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW))) AS volatility_{window}"""
            )
    # get n_news columns of the last n news articles before TradingData.timestamp
    ranked_news_joins = []
    for idx in range(1, n_news + 1):
        columns.append(f"n{idx}.news_id AS news{idx}_id")
        columns.append(f"n{idx}.timestamp AS news{idx}_timestamp")
        ranked_news_joins.append(
            f"LEFT JOIN RankedNews n{idx} ON t.company_id = n{idx}.company_id AND t.timestamp = n{idx}.trading_timestamp AND n{idx}.rn = {idx}"
        )
    if ranked_news_joins:
        CTEs.append(
            f"""
            RankedNews AS (
                SELECT
                    n.id AS news_id,
                    n.timestamp,
                    t.company_id,
                    t.timestamp AS trading_timestamp,
                    ROW_NUMBER() OVER (PARTITION BY t.company_id, t.timestamp ORDER BY n.timestamp DESC) AS rn
                FROM
                    TradingData t
                JOIN
                    NewsCompanyLink ncl
                ON
                    t.company_id = ncl.company_id
                JOIN
                    News n
                ON
                    ncl.news_id = n.id
                AND
                    n.timestamp <= t.timestamp
                AND 
                    n.timestamp >= t.timestamp - {news_relative_age_threshold}
            )"""
        )
    # Apply filters if necessary
    where_clause = []
    if company_id:
        where_clause.append(f"t.company_id = {company_id}")
    if min_timestamp:
        max_window = max(windows) if windows else 0
        adjusted_min_timestamp = (
            min_timestamp - (max_window * 60 + 28800) if min_timestamp else None
        )
        where_clause.append(f"t.timestamp >= {adjusted_min_timestamp}")
    if max_timestamp:
        where_clause.append(f"t.timestamp <= {max_timestamp}")
    CTEs = "WITH " + "\n".join(CTEs) + "\n" if CTEs else ""
    columns = ",\n".join(columns)
    ranked_news_joins = "\n".join(ranked_news_joins) if ranked_news_joins else ""
    where_clause = "WHERE " + " AND ".join(where_clause) if where_clause else ""
    query = f"""
    {CTEs}
    SELECT 
        {columns}
    FROM 
        TradingData t
    JOIN 
        Company c
    ON 
        t.company_id = c.id
    {ranked_news_joins}
    {where_clause}
    ORDER BY 
        t.company_id, t.timestamp;
    """
    return query


# TODO
#  Industry one-hot encoding
#  Reddit data
#  How to tokenize company in text?
