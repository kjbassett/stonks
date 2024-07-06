from async_lru import alru_cache

from .base_dao import BaseDAO


class Industry(BaseDAO):
    def __init__(self, db):
        super().__init__(db, "Industry")

    @alru_cache(maxsize=500)
    async def get_or_create_industry_id(self, name: str = None):
        if not name:
            raise ValueError("Please provide the name of the industry.")
        industry = await self.get(name=name)

        # if not found, create a new company if symbol is provided
        if industry.empty:
            await self.insert({"name": name})
            industry = await self.get(industry=industry)

        return industry.loc[0, "id"]
