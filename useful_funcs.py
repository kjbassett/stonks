import datetime
import os
import pandas as pd
import pandas_market_calendars


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


def market_date_delta(date, n):
    """
    Calculates the date that is n days of the market being open after date
    :param date:
    :param n:
    :return:
    """
    if n == 0:
        # If n is 0, then we get the next day that the market is open.
        date = date - datetime.timedelta(days=1)
        n = 1

    direction = abs(n) / n
    cal = pandas_market_calendars.get_calendar("NYSE")

    start = date + datetime.timedelta(days=direction)
    days = max(abs(2*n), 7)# safe upper and lower bounds ?
    end = date + datetime.timedelta(days=days * direction)
    if start < end:
        cal = cal.schedule(start_date=start, end_date=end)
        cal = cal.head(n)
        return cal.index[-1]
    else:
        cal = cal.schedule(start_date=end, end_date=start)
        cal = cal.tail(abs(n))
        return cal.index[0]


# Yeah, I know this isn't pep8, but I couldn't waste 4 lines on something so simple. Plus this is cooler 😎
load_progress = lambda path: pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
