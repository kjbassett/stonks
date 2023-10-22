import datetime
import os
import pandas as pd
import pandas_market_calendars
from config import CONFIG
from functools import cache


def is_open(date):
    if date.weekday() > 4:
        return False
    cal = pandas_market_calendars.get_calendar("NYSE")
    cal = cal.schedule(start_date=date, end_date=date)
    if len(cal.index) == 0:
        return False
    return True


def get_api_key(api_name):
    with open("../keys.txt", "r") as f:
        for line in f:
            name, key = line.strip().split("=")
            if name == api_name:
                return key
    raise ValueError(f"No key found for API: {api_name}")


@cache
def market_date_delta(date, n=0):
    """
    Calculates the date that is n days of the market being open after date.
    If n is 0, return the next day that the market is open.
    """
    if n == 0:
        # If n is 0, then we get the next day that the market is open.
        date = date - datetime.timedelta(days=1)
        n = 1

    direction = abs(n) / n
    cal = pandas_market_calendars.get_calendar("NYSE")

    start = date + datetime.timedelta(days=direction)
    days = max(abs(2*n), 7)  # safe upper and lower bounds ?
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


def latest_market_time():
    # lmt = latest market time
    # todo check all apis info for latest possible
    # Todo convert milliseconds to seconds
    lmt1 = (datetime.datetime.now().timestamp() - 60 * 20)  # 20 minutes ago

    lmt2 = last_open_date()
    lmt2 = datetime.datetime.combine(lmt2, datetime.time(hour=20))  # todo check all apis info for latest open hours
    lmt2 = lmt2.timestamp()
    return min(lmt1, lmt2)


all_open_dates = get_open_dates(CONFIG['min_date'], datetime.datetime.today())


@cache
def filter_open_dates(start_date, end_date):
    return all_open_dates[(all_open_dates >= start_date) & (all_open_dates <= end_date)]


# Yeah, I know this isn't pep8, but I couldn't waste 4 lines on something so simple. Plus this is cooler 😎
load_progress = lambda path: pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()


if __name__ == "__main__":
    print(get_open_dates(datetime.date(2023, 8, 1), datetime.datetime.today()))
    print(latest_market_time())
