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

    async def get_n_most_recent_news(
        self, company_id: int, n: int, min_timestamp: int = 0
    ) -> pd.DataFrame:
        query = f"""
        SELECT *
        FROM News INNER JOIN NewsCompanyLink
        ON News.id = NewsCompanyLink.news_id
        WHERE NewsCompanyLink.company_id =?
        AND News.timestamp >?
        ORDER BY News.timestamp DESC
        LIMIT ?;
        """

        data = await self.db(
            query, (company_id, min_timestamp, n), return_type="DataFrame"
        )
        return data
