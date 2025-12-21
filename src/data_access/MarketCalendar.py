import pandas as pd
import pandas_market_calendars as mcal
from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase


class MarketCalendar(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "MarketCalendar")

    async def populate_market_calendar(self, start_date, end_date):
        calendar = build_market_calendar(start_date, end_date)
        await self.insert(calendar)


def build_market_calendar(start_date, end_date):
    cal = mcal.get_calendar("NYSE")

    schedule = cal.schedule(start_date=start_date, end_date=end_date)

    rows = []
    for i, row in schedule.iterrows():
        rows.append(
            {
                "date": i.date(),
                "open_ts": row["market_open"].timestamp(),
                "close_ts": row["market_close"].timestamp(),
                "last_market_hour": row["market_close"].hour - 1,
            }
        )

    return pd.DataFrame(rows)
