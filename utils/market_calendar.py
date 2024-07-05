import datetime
from functools import cache

import pandas as pd
import pandas_market_calendars
from config import CONFIG


def is_open(date):
    if date.weekday() > 4:
        return False
    cal = pandas_market_calendars.get_calendar("NYSE")
    cal = cal.schedule(start_date=date, end_date=date)
    if len(cal.index) == 0:
        return False
    return True


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


def latest_market_time(delay=900):
    # data delayed by 15 minutes, extra 5 minutes buffer
    lmt1 = datetime.datetime.now().timestamp() - delay - 300

    lmt2 = last_open_date()
    lmt2 = datetime.datetime.combine(
        lmt2, datetime.time(hour=20)
    )
    lmt2 = lmt2.timestamp()
    return int(min(lmt1, lmt2))


all_open_dates = get_open_dates(CONFIG["min_date"], datetime.datetime.today())


@cache
def filter_open_dates(start_date, end_date):
    return all_open_dates[(all_open_dates >= start_date) & (all_open_dates <= end_date)]


if __name__ == "__main__":
    print(get_open_dates(datetime.date(2023, 8, 1), datetime.datetime.today()))
    print(latest_market_time())
    cal = pandas_market_calendars.get_calendar("NYSE")
    cal = cal.schedule(
        start_date=datetime.date(2021, 11, 22), end_date=datetime.date(2021, 11, 22)
    )
    print(cal)
