import time
import queue
import threading
import pandas as pd
import numpy as np
import os
import importlib
import datetime
import pytz
from useful_funcs import last_open_date


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


def find_data_gaps(df):
    """
    Find gaps in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to find gaps in.
    Returns
    -------
    gaps : pd.DataFrame
        A DataFrame of gaps.
    """
    df = df.sort_values(by='timestamp')
    df.timestamp = df.timestamp / 1000  # TODO convert milliseconds to seconds

    # Previous timestamp
    df['previous'] = df['timestamp'].shift(1)

    # Add columns with datetimes of timestamp and previous columns
    df['datetime'] = pd.to_datetime(df.timestamp, unit='s', utc=True).dt.tz_convert('US/Eastern')
    df['previous_dt'] = pd.to_datetime(df.previous, unit='s', utc=True).dt.tz_convert('US/Eastern')

    # dt4 is date of timestamp column + 4pm
    est = pytz.timezone('US/Eastern')
    df['dt4'] = df.apply(
        lambda row: est.localize(datetime.datetime.combine(row['datetime'].date(), datetime.time(4))),
        axis=1
    ).astype(np.int64) // 10**9

    # Previous is the last timestamp if last timestamp is on the same day
    # Otherwise previous is at 4am
    # In other words, don't count gaps if their on different days
    df['previous'] = np.where(
        df['previous_dt'].dt.date == df['datetime'].dt.date,
        df['previous'],
        df['dt8']
    )

    df['gap'] = df['timestamp'] - df['previous']
    df = df[df['gap'] > 1800]  # gaps > 30 minutes are counted
    gaps = df[['previous', 'timestamp']]
    gaps.columns = ['start', 'end']
    return gaps


def latest_market_time():
    # lmt = latest market time
    # todo check all apis info for latest possible
    # Todo convert milliseconds to seconds
    lmt1 = (datetime.datetime.now().timestamp() - 60 * 20) * 1000  # 20 minutes ago

    lmt2 = last_open_date()
    lmt2 = datetime.datetime.combine(lmt2, datetime.time(hour=20))  # todo check all apis info for latest open hours
    lmt2 = lmt2.timestamp() * 1000
    return min(lmt1, lmt2)


def process_symbols(api, assign_queue, input_queue, result_queue):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame
    # Remember this function is what is parallelized

    while True:  # TODO replace with signal handling
        # Wait for api rate limit
        print(f'Waiting for {max(0, api.next_available_call_time() - time.time())} seconds')
        time.sleep(max(0, api.next_available_call_time() - time.time()))

        # Get next symbol to process
        try:
            symbol, start_time = input_queue.get(timeout=10)
        except queue.Empty:  # No more symbols to process
            return

        # Check if the symbol is in the time range for the API
        if datetime.datetime.fromtimestamp(start_time//1000) < api.earliest_possible_time():
            print(f'{symbol} not in time range for {api.name}')
            assign_queue.put((symbol, start_time))
            continue

        # get result from the API
        print(symbol, api.name)
        result = api.api_call(symbol, start_time, int(datetime.datetime.now().timestamp()*1000))

        if result is None:
            print(f'No results from {symbol} + {api.name}')
            assign_queue.put((symbol, start_time))
            continue
        print(f'New data points: {len(result.index)}')

        result_queue.put((symbol, result))


def distribute_requests(ticker_symbols):
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
            target=process_symbols, args=(api, assign_queue, input_queue, result_queue)
        )
        thread.start()
        threads.append({'thread': thread, 'input_queue': input_queue, 'api': api})

    print('Threads started')

    # While threads are running
    while any(t['thread'].is_alive() for t in threads):
        # Try to get a symbol from the queue
        try:
            symbol = assign_queue.get(timeout=1)
            data = load_saved_data(symbol)  # TODO locks? or something
            for i, row in find_data_gaps(data):
                input_queue = choose_api(row['start'], row['end'], threads)
                input_queue.put((symbol, row['start'], row['end']))
        except queue.Empty:
            pass
        # Try to get a result from the queue
        try:
            symbol, result = result_queue.get(timeout=1)
            save_new_data(symbol, result)
            assign_queue.put(symbol)  # It will be checked for gaps again. If no gaps, it won't be assigned.
            # TODO other gap could already be in symbol queue!!!
        except queue.Empty:
            pass

# TODO
#  Re-adding timestamp to queue should have a better new time
#    Next available API time
#    Need ability to fill in timestamp gaps
#      Change to add_symbols_to_queue()
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

    distribute_requests(tcks)
