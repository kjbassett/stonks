from urllib3.exceptions import MaxRetryError, NewConnectionError
from requests.exceptions import SSLError as SSLError_, ConnectionError
from OpenSSL.SSL import SysCallError
from ssl import SSLError
from dateutil.easter import easter
from useful_funcs import calc_date, get_cred
import numpy as np
import pandas as pd
import holidays
import requests
import json
import os
import datetime
import time


def get_price_data(ticker, start, end):
    # Currently calling this function with 9:30 as start and end
    print(ticker)
    api_key = get_cred('ameritrade_api', 'api_key')
    url = f'https://api.tdameritrade.com/v1/marketdata/{ticker}/pricehistory?apikey={api_key}' \
          f'&endDate={end + 3600000}&startDate={start - 3600000}&needExtendedHoursData=false '
    for tries in range(5):
        try:
            query = requests.get(url)
            data = json.loads(query.content.decode())

            if 'candles' in data.keys() and len(data['candles']) > 0:
                data = {d['datetime']: d['open'] for d in data['candles']}
                if end in data.keys() and start in data.keys():
                    return (data[end] - data[start]) / data[start]
                else:
                    a = np.array(list(data.keys()))
                    a1 = np.abs(a - start)
                    a2 = np.abs(a - end)
                    i1 = np.argmin(a1)
                    i2 = np.argmin(a2)
                    if a1[i1] <= 1800000 and a2[i2] <= 1800000:
                        t1 = a[i1]
                        t2 = a[i2]
                        return (data[t2] - data[t1]) / data[t1]
                    else:
                        print('No Result')
                        print(f'Target times: {start}, {end}')
                        print(data)
                        return np.nan

            elif 'error' in data.keys():
                if 'transactions per second' in data['error']:
                    time.sleep(0.5)
                    continue
                else:
                    print(data)
            elif 'empty' in data.keys() and data['empty']:
                return np.nan
            else:
                print('UNHANDLED RESULT')
                print(query)
                print(data)
                return np.nan
        except (SysCallError, SSLError, SSLError_, MaxRetryError,
                TimeoutError, NewConnectionError, ConnectionError) as e:
            print('Connection error.')
            print(e)
        time.sleep(0.5)
    else:
        return np.nan


def fix_data(days):
    for file in os.listdir(str(days)):
        print(file)
        df = pd.read_csv(str(days) + '\\' + file)
        date = datetime.datetime.strptime(file[2:12], '%Y-%m-%d').date()
        if date < datetime.date.today() - datetime.timedelta(days=30):
            continue

        nans = df[np.isnan(df['y'])]
        if not nans.empty:
            new_ys = get_changes(nans, date, days, query_only=True)
            print(new_ys[(new_ys['y'] != 0) & np.logical_not(np.isnan(new_ys['y']))])

            df.update(new_ys)
            print(f'Reduced {len(nans.index)} incomplete data points down to {len(df[np.isnan(df["y"])].index)}')
        else:
            print('Good data!')

        df.to_csv(str(days) + '\\' + file)


def get_changes(df, start, days, save=False, load_progress=True, query_only=False):
    """

    :param df: dataframe onto which a column containing the price change as a % will be added
    :param start: beginning date
    :param days: Number of days in the future to
    :param save: Boolean, save results for future use
    :param load_progress: If True, loads progress if process ended before get_changes could finish
    :param query_only: Only query Ameritrade API instead of getting result from saved file if True
    :return: df with results in 'y' column
    """

    path = f'{days}\\{days}-' + start.strftime('%Y-%m-%d') + '.csv'
    ppath = f'{days}\\{days}-' + start.strftime('%Y-%m-%d') + 'PART.csv'
    print(f'Gathering Data for {start}')

    # Check if changes have already been found and stored in a csv
    if not query_only and os.path.exists(path):
        df = df.merge(pd.read_csv(path)[['Ticker', 'y']], how='left', on='Ticker')
        return df

    # Check if get_changes started but didn't finish
    if os.path.exists(ppath) and load_progress:
        df = df.merge(pd.read_csv(ppath)[['Ticker', 'y']], how='left', on='Ticker')
    else:
        df['y'] = np.nan
    print(df)
    end = calc_date(start, days)

    t1 = int(datetime.datetime(start.year, start.month, start.day, 9, 30).timestamp() * 1000)
    t2 = int(datetime.datetime(end.year, end.month, end.day, 9, 30).timestamp() * 1000)

    # 30 days is limit on Ameritrade.
    # Next day's recs must be available for data points to be valid in the first place
    if datetime.datetime.now().date() - datetime.timedelta(days=30) <= start\
            and calc_date(start, 1).strftime('%Y-%m-%d') in os.listdir('DailyRecs'):
        print(start)
        t_col = df.columns.get_loc('Ticker')
        y_col = df.columns.get_loc('y')
        for i in range(len(df.index)):
            if pd.isnull(df.iloc[i]['y']):
                time.sleep(0.55)
                df.iat[i, y_col] = get_price_data(df.iat[i, t_col], t1, t2)
                df[['Ticker', 'y']].to_csv(ppath, index=False)
    else:
        save = False

    if save:
        if not os.path.exists(str(days)):
            os.makedirs(str(days))
        if not os.path.exists(path):
            df[['Ticker', 'y']].to_csv(path, index=False)

    if os.path.exists(ppath):
        os.remove(ppath)

    return df


def is_open(date):
    us_holidays = holidays.US()

    # If a holiday
    if date in us_holidays and us_holidays[date] not in ['Veterans Day', 'Columbus Day']:
        return False

    # Good Friday not included above, but stock market is closed
    if date == easter(date.year) - datetime.timedelta(days=2):
        return False

    # If it's a weekend
    if date.weekday() > 4:
        return False

    return True
