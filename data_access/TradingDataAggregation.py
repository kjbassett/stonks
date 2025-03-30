from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class TradingDataAggregation(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")