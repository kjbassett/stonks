import datetime
from functools import cache

import pandas as pd
import pandas_market_calendars
from src.utils.project_utilities import config
from webrock.decorator import plugin


def get_min_date():
    earliest = datetime.date.today() - datetime.timedelta(days=config["max_data_age"])
    return market_date_delta(earliest)


def is_open(date):
    if date.weekday() > 4:
        return False
    cal = pandas_market_calendars.get_calendar("NYSE")
    cal = cal.schedule(start_date=date, end_date=date)
    if len(cal.index) == 0:
        return False
    return True


def is_currently_open() -> bool:
    """
    Returns True if the NYSE is open for trading right now.

    Unlike is_open(), this checks both the calendar date (holiday/weekend)
    and whether the current wall-clock time falls within today's session.
    """
    now = pd.Timestamp.now(tz="UTC")
    schedule = pandas_market_calendars.get_calendar("NYSE").schedule(
        start_date=now.date(), end_date=now.date()
    )
    if schedule.empty:
        return False
    return schedule.iloc[0]["market_open"] <= now <= schedule.iloc[0]["market_close"]


@cache
def market_date_delta(date: datetime.datetime, n: int = 0):
    """
    Calculates the date that is n days of the market being open after date.
    If n is 0, return the next day that the market is open, including date.
    """
    if n == 0:
        # If n is 0, then we get the next day that the market is open.
        date = date - datetime.timedelta(days=1)
        n = 1

    direction = abs(n) / n
    cal = pandas_market_calendars.get_calendar("NYSE")

    start = date + datetime.timedelta(days=direction)
    days = max(abs(2 * n), 7)  # safe upper and lower bounds ?
    end = date + datetime.timedelta(days=days * direction)
    if start < end:
        cal = cal.schedule(start_date=start, end_date=end)
        cal = cal.head(n)
        return cal.index[-1]
    else:
        cal = cal.schedule(start_date=end, end_date=start)
        cal = cal.tail(abs(n))
        return cal.index[0]


def last_open_date():
    if datetime.datetime.now().hour < 4:
        return market_date_delta(datetime.datetime.today(), -1)
    return market_date_delta(datetime.datetime.today() + datetime.timedelta(days=1), -1)


def get_open_dates(start, end):
    cal = pandas_market_calendars.get_calendar("NYSE")
    cal = cal.schedule(start_date=start, end_date=end)
    cal = pd.Series(cal.index).dt.date
    return cal


def earliest_market_time():
    min_date = get_min_date()
    min_time = datetime.datetime.combine(min_date, datetime.time(4, 0, 0))
    return int(min_time.timestamp())


def latest_market_time(delay=900):
    # data delayed by 15 minutes, extra 5 minutes buffer
    lmt1 = datetime.datetime.now().timestamp() - delay - 300

    lmt2 = last_open_date()
    lmt2 = datetime.datetime.combine(lmt2, datetime.time(hour=20))
    lmt2 = lmt2.timestamp()
    return int(min(lmt1, lmt2))


@plugin()
async def update_market_calendar():
    from src.data_access.dao_manager import dao_manager

    earliest = datetime.date.today() - datetime.timedelta(days=config["max_data_age"])
    latest = datetime.date.today() + datetime.timedelta(days=7)
    await dao_manager.get_dao("MarketCalendar").populate_market_calendar(
        earliest, latest
    )


if __name__ == "__main__":
    print(get_open_dates(datetime.date(2023, 8, 1), datetime.datetime.today()))
    print(latest_market_time())
    calendar = pandas_market_calendars.get_calendar("NYSE")
    calendar = calendar.schedule(
        start_date=datetime.date(2021, 11, 22), end_date=datetime.date(2021, 11, 22)
    )
    print(calendar)
