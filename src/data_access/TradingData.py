import logging
import pandas as pd

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_log = logging.getLogger("data_access.trading_data")


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
        query = f"SELECT timestamp FROM {self.table_name} WHERE company_id =? AND timestamp >=? ORDER BY timestamp ASC;"
        return await self.db.execute_query(
            query, (company_id, min_timstamp), return_type="DataFrame"
        )

    async def clean_data(self, min_timestamp: int) -> None:
        # delete old data and data on companies with disabled ticker types
        query = f"""
        DELETE FROM {self.table_name} 
          WHERE timestamp <=? 
          OR company_id in (
            SELECT Company.id
            FROM Company
            LEFT JOIN TickerType ON Company.ticker_type_id = TickerType.id
            LEFT JOIN Exchange ON Company.primary_exchange = Exchange.market_id
            WHERE TickerType.enabled <> 1 OR Company.enabled <> 1 OR Exchange.active IS 0
          );
        """
        _log.debug("Delete query: %s", query)
        await self.db.execute_query(query, (min_timestamp,), query_type="DELETE")
        # delete old attempted queries and queries on companies with disabled ticker types
        query = f"""
        DELETE FROM TradingDataAttemptedQueries
        WHERE end <=?
        OR company_id in (
            SELECT Company.id
            FROM Company
            LEFT JOIN TickerType
            ON Company.ticker_type_id = TickerType.id
            WHERE TickerType.enabled <> 1 OR Company.enabled <> 1
          );
        """
        _log.debug("Delete query: %s", query)
        await self.db.execute_query(query, (min_timestamp,), query_type="DELETE")
