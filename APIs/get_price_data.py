import time
import queue
import threading
import pandas as pd
import numpy as np
import os
import importlib
import datetime
import pytz
from useful_funcs import latest_market_time, get_open_dates, market_date_delta
from functools import partial


def load_apis():
    apis = []
    subfolders = [
        f.path for f in os.scandir(os.getcwd()) if f.is_dir() and f.name[0] != "_"
    ]
    for fol in subfolders:
        file = os.path.split(fol)[1]
        file = file + "." + file
        api = importlib.import_module(file, package="APIs")
        if api.DISABLED:
            continue
        apis.append(api.API())
    return apis


def load_saved_data(symbol):
    """Load saved data for a symbol if it exists, otherwise return None."""
    filepath = os.path.join("../Data", f"{symbol}.parquet")
    if os.path.exists(filepath):
        return pd.read_parquet(filepath, engine='fastparquet')
    else:
        return None


def save_new_data(symbol, df):
    """Save a DataFrame to a Parquet file, appending if the file already exists."""
    # TODO remove duplicates
    if df.empty:
        return
    filepath = os.path.join("../Data", f"{symbol}.parquet")
    if os.path.exists(filepath):
        df.to_parquet(filepath, engine='fastparquet', append=True)
    else:
        df.to_parquet(filepath, engine='fastparquet')


def choose_best_api(start, end, threads):
    # The below 2 lines may not work if run outside the US/Eastern time zone
    start = datetime.datetime.fromtimestamp(start)
    end = datetime.datetime.fromtimestamp(end)

    # delete this. It's just here for testing
    for t in threads:
        print(f"{t['api'].name}: {t['api'].covered_hours(start, end)}")

    ts = sorted(threads, key=lambda t: (
        -t['api'].covered_hours(start, end),
        t['input_queue'].qsize()
    ))
    if ts[0]['api'].covered_hours(start, end) > 0:
        return ts[0]


def find_gap_in_data(df):
    df.timestamp = df.timestamp / 1000  # TODO convert milliseconds to seconds

    # add in beginning and end of time range
    ends = [datetime.datetime.combine(min_date, datetime.time(4)).timestamp(),
            latest_market_time()]

    df = pd.concat([df, pd.DataFrame({'timestamp': ends})])
    df = df.sort_values(by='timestamp')

    # Previous timestamp
    df['previous'] = df['timestamp'].shift(1)

    # Convert timestamp and previous to datetime columns
    df['date'] = pd.to_datetime(df.timestamp, unit='s', utc=True).dt.tz_convert('US/Eastern').dt.normalize()
    df['prev_date'] = pd.to_datetime(df.previous, unit='s', utc=True).dt.tz_convert('US/Eastern').dt.normalize()
    df['days_apart'] = (df['date'] - df['prev_date']).dt.days

    df['gap'] = df['timestamp'] - df['previous']
    # if previous timestamp is a previous day, don't include closed hours in the gap
    # 28800 is the time at the tail ends of the gap. 4 hrs after 8 pm and 4 hours before 4am is 8 hours * 3600
    open_dates = get_open_dates(df['date'].dt.date.min(), datetime.date.today())
    df = df.iloc[1:, :]

    # these two columns are needed for the adjust gap function
    df['gap'] = df.apply(partial(adjust_gap, open_dates), axis=1)
    # TODO gap adjustment treats all days in between as closed days.
    #  open days should only remove 8 hours from gap

    gaps = df[df['gap'] > 1800]  # gaps > 30 minutes are counted
    # check if gap has been tried before for each api
    if len(gaps.index) == 0:
        return
    return gaps.iloc[0]


def adjust_gap(open_dates, row):
    # only adjust if the data is on two separate days
    if row['days_apart'] > 0:
        # Don't count the time outside the market hours
        # Only count days the market is open. Gap could cover an open and a closed day.
        d1 = row['prev_date'].replace(tzinfo=None)
        d2 = row['date'].replace(tzinfo=None)
        i1 = open_dates.searchsorted(d1)
        i2 = open_dates.searchsorted(d2)
        if i1 == len(open_dates) or open_dates[i1] != d1:
            raise ValueError(f"{d1} is not in {open_dates}")
        if i2 == len(open_dates) or open_dates[i2] != d2:
            raise ValueError(f"{d2} is not in {open_dates}")
        open = i2 - i1
        closed = row['days_apart'] - open
        row['gap'] = row['gap'] - 28800 * open - 86400 * closed
    return row['gap']


def build_request(symbol, threads, min_date):
    """
    Finds gaps in available data. Chooses an api best suited to fill in the first gap.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to find gaps in.
    Returns
    -------
    start : starting timestamp of first gap
    end : ending timestamp of first gap
    """
    # min date is the earliest day since min_date that is covered by api limits
    min_date = max(min_date, min([t['api'].info['date_range']['min'] for t in threads]))
    # the day also has to be open
    min_date = market_date_delta(min_date)

    df = load_saved_data(symbol)
    if df is None:
        start = datetime.datetime.combine(min_date, datetime.time(4)).timestamp()
        end = latest_market_time()
    else:
        gap = find_gap_in_data(df)
        if gap is None:
            return
        start, end = gap['previous'], gap['timestamp']

    api = choose_best_api(start, end, threads)
    if api is None:
        return

    return api['input_queue'], start, end


def process_symbols(api, input_queue, result_queue):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame
    # Remember this function is what is parallelized

    while True:  # TODO replace with signal handling
        # Wait for api rate limit
        print(f'Waiting for {max(0, api.next_available_call_time() - time.time())} seconds')
        time.sleep(max(0, api.next_available_call_time() - time.time()))

        # Get next symbol to process
        try:
            symbol, start, end = input_queue.get(timeout=10)
        except queue.Empty:  # No more symbols to process
            return

        # get result from the API
        print(symbol, api.name)
        result = api.api_call(symbol, start*1000, end*1000)

        result_queue.put((symbol, result))


def distribute_requests(ticker_symbols, min_date):
    # Create a shared queue for symbols before they are assigned to an api's
    assign_queue = queue.Queue()
    # Create a shared queue for incoming results from other threads
    result_queue = queue.Queue()

    # Add symbols to the queue for assignment to an api
    for symbol in ticker_symbols:
        assign_queue.put(symbol)

    # Create and start a thread for each API
    threads = []
    for api in load_apis():
        input_queue = queue.Queue()
        thread = threading.Thread(
            target=process_symbols, args=(api, input_queue, result_queue)
        )
        thread.start()
        threads.append({'thread': thread, 'input_queue': input_queue, 'api': api})

    print('Threads started')

    # While threads are running
    while any(t['thread'].is_alive() for t in threads):
        # Try to get a symbol from the queue
        try:
            symbol = assign_queue.get_nowait()
            # Evaluate data and api limitations to determine which one should be used and for what time period
            request = build_request(symbol, threads, min_date)
            if request is None: # No request possible
                continue
            input_queue, start, end = request
            input_queue.put((symbol, start, end))
        except queue.Empty:
            pass
        # Try to get a result from the queue
        try:
            symbol, result = result_queue.get_nowait()
            save_new_data(symbol, result)
            assign_queue.put(symbol)  # It will be checked for gaps again. If no gaps, it won't be assigned.
        except queue.Empty:
            pass

# TODO

#  All timestamps should be seconds
#  Adjust ameritrade's min to be opening time of last open day so that the other APIs with better extended hours will be used for historical data
#  Figure out why APIs rate limits are not being respected (noticed on polygon)
#  hit run and fix until it works
#  Explore other API calls
#   https://polygon.io/docs/stocks/get_v2_reference_news
# Keep in mind that this will be run every day


if __name__ == "__main__":
    print(f'Latest market data at: {datetime.datetime.fromtimestamp(latest_market_time() / 1000)}')
    tcks = pd.read_csv("../tickers.csv")["Ticker"].unique().tolist()
    # tcks = ['AAPL', 'NVDA']

    min_date = (datetime.datetime.now() - datetime.timedelta(days=730)).date()  # data cutoff

    distribute_requests(tcks, min_date)