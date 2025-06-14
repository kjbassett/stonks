import pandas as pd

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class News(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "News")

    async def get_timestamps_by_company(
        self, company_id: int, min_timestamp: int = 0
    ) -> pd.DataFrame:
        query = f"""
        SELECT timestamp
        FROM News INNER JOIN NewsCompanyLink
        ON News.id = NewsCompanyLink.news_id
        WHERE NewsCompanyLink.company_id =? AND News.timestamp >?;
        """

        data = await self.db(
            query, (company_id, min_timestamp), return_type="DataFrame"
        )
        return data

    async def clean_data(self, min_timestamp: int):
        # Delete items from linking table for companies with ticker type having enabled = 0
        query = """DELETE FROM NewsCompanyLink WHERE company_id IN (
            SELECT Company.id 
            FROM Company 
            LEFT JOIN TickerType
            ON Company.ticker_type_id = TickerType.id
            WHERE TickerType.enabled <> 1
        )"""
        await self.db.execute_query(query)
        # Delete old news items and ones with no rows in NewsCompanyLink table
        query = "DELETE FROM News WHERE timestamp < ? OR id NOT IN (SELECT news_id FROM NewsCompanyLink)"
        await self.db.execute_query(query, (min_timestamp,))
        query = f"""
        DELETE FROM NewsAttemptedQueries 
        WHERE company_id IN (
            SELECT Company.id 
            FROM Company 
            LEFT JOIN TickerType
            ON Company.ticker_type_id = TickerType.id
            WHERE TickerType.enabled <> 1
        )
        OR end < ?"""
        await self.db.execute_query(query, (min_timestamp,))
