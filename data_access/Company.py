import pandas as pd
from async_lru import alru_cache

from .base_dao import BaseDAO


class Company(BaseDAO):
    def __init__(self, db):
        super().__init__(db, "Company")

    @alru_cache(maxsize=500)
    async def get_or_create_company(
        self, symbol: str = None, name: str = None, industry_id: str = None
    ):
        # TODO This should not be on the data access layer, but on the service layer. It should query polygon if no company data
        if not symbol and not name:
            raise ValueError("Please provide either a symbol or a name.")
        company = None
        if symbol:
            company = await self.get(symbol=symbol)
        elif name:
            company = await self.get(name=name)

        # if not found, create a new company if symbol is provided
        if (isinstance(company, pd.DataFrame) and company.empty) or company is None:
            if symbol:
                await self.insert(
                    {"symbol": symbol, "name": name, "industry_id": industry_id}
                )
                company = await self.get(symbol=symbol)
            else:
                raise ValueError(
                    "No company found, and no symbol provided to create new company."
                )

        return int(company.loc[0, "id"])
