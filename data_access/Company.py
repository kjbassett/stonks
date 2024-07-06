from async_lru import alru_cache

from .base_dao import BaseDAO


class Company(BaseDAO):
    def __init__(self, db):
        super().__init__(db, "Company")

    @alru_cache(maxsize=500)
    async def get_or_create_company(
        self, symbol: str = None, name: str = None, industry: str = None
    ):
        if not symbol and not name:
            raise ValueError("Please provide either a symbol or a name.")
        company = None
        if symbol:
            company = await self.get(symbol=symbol)
        elif name:
            company = await self.get(name=name)

        # if not found, create a new company if symbol is provided
        if not company:
            if symbol:
                await self.insert({"symbol": symbol, "name": name, "industry": industry})
                company = await self.get(symbol=symbol)
            else:
                raise ValueError(
                    "No company found, and no symbol provided to create new company."
                )

        return company[0]
