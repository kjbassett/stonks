import pandas as pd

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class TradingData(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")

    async def get_data(
        self,
        company_id: int = None,
        symbol: str = None,
        start_timestamp: int = None,
        end_timestamp: int = None,
    ) -> pd.DataFrame:
        query = f"SELECT * FROM {self.table_name}"

        where_clause = []
        params = []
        if company_id is not None:
            where_clause.append("company_id = ?")
            params.append(company_id)
        if symbol is not None:
            where_clause.append("symbol = ?")
            params.append(symbol)
        if start_timestamp is not None:
            where_clause.append("timestamp >= ?")
            params.append(start_timestamp)
        if end_timestamp is not None:
            where_clause.append("timestamp <= ?")
            params.append(end_timestamp)

        if len(where_clause) > 0:
            query += " WHERE " + " AND ".join(where_clause)
        query += " ORDER BY timestamp ASC;"
        return await self.db.execute_query(
            query, (start_timestamp, end_timestamp), return_type="DataFrame"
        )

    async def get_timestamps_by_company(
        self, company_id: int, min_timstamp: int = 0
    ) -> pd.DataFrame:
        query = f"SELECT timestamp FROM {self.table_name} WHERE t.company_id =? AND timestamp >=? ORDER BY timestamp ASC;"
        return await self.db.execute_query(
            query, (company_id, min_timstamp), return_type="DataFrame"
        )

    async def get_data_with_statistics(
        self,
        company_id: int = None,
        min_timestamp: int = None,
        max_timestamp: int = None,
        avg_close: bool = True,
        avg_volume: bool = True,
        std_dev: bool = True,
        row_windows: iter = None,
    ) -> pd.DataFrame:
        # Calculate the maximum window size to adjust the timestamp range
        if row_windows is None:
            row_windows = [4, 19, 59, 389]
        max_window = max(row_windows) if row_windows else 0
        # Assuming 60 seconds per measurement and 28800 seconds between trading windows
        adjusted_min_timestamp = (
            min_timestamp - (max_window * 60 + 28800) if min_timestamp else None
        )

        columns = [
            "t.company_id",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "vw_average",
            "volume",
        ]

        # Add moving averages and other statistics if requested
        for window in row_windows:

            if avg_close:
                columns.append(
                    f"AVG(close) OVER (PARTITION BY t.company_id ORDER BY timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) AS ma_{window}"
                )
            if avg_volume:
                columns.append(
                    f"AVG(volume) OVER (PARTITION BY t.company_id ORDER BY timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) AS avg_volume_{window}"
                )
            if std_dev:
                columns.append(
                    f"""SQRT(AVG(close * close) OVER (PARTITION BY t.company_id ORDER BY timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) - 
                    (AVG(close) OVER (PARTITION BY t.company_id ORDER BY timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW) * 
                    AVG(close) OVER (PARTITION BY t.company_id ORDER BY timestamp ROWS BETWEEN {window} PRECEDING AND CURRENT ROW))) AS volatility_{window}"""
                )

        # Base query
        query = f"""
        -- Declare variables for optional parameters
        WITH Params AS (
            SELECT
                ? AS company_id,
                ? AS min_timestamp,
                ? AS max_timestamp
        )
        SELECT 
            {", ".join(columns)}
        FROM 
            TradingData t, Params p
        WHERE 
            (p.company_id IS NULL OR t.company_id = p.company_id) AND 
            (p.min_timestamp IS NULL OR t.timestamp >= p.min_timestamp) AND 
            (p.max_timestamp IS NULL OR t.timestamp <= p.max_timestamp)
        ORDER BY 
            t.company_id, t.timestamp;
        """

        # Execute the query
        data = await self.db.execute_query(
            query,
            (company_id, adjusted_min_timestamp, max_timestamp),
            return_type="DataFrame",
            query_type="SELECT",
        )

        # Filter the data using pandas
        if min_timestamp:
            data = data[data["timestamp"] >= min_timestamp]
        if max_timestamp:
            data = data[data["timestamp"] <= max_timestamp]

        return data
