import time
import queue
import threading
import pandas as pd
import os
import importlib
import datetime
from useful_funcs import is_open, market_date_delta


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
    if df.empty:
        return
    filepath = os.path.join("../Data", f"{symbol}.parquet")
    if os.path.exists(filepath):
        df.to_parquet(filepath, engine='fastparquet', append=True)
    else:
        df.to_parquet(filepath, engine='fastparquet')


def add_symbols_to_queue(ticker_symbols, symbol_queue):
    """Add symbols to the queue, checking the last timestamp of saved data for each one."""
    for symbol in ticker_symbols:
        data = load_saved_data(symbol)
        if data is not None and len(data.index) > 0:
            last_timestamp = data.timestamp.max() + 1
            # You could add a check here to only add the symbol to the queue if the last timestamp
            # is more than a certain amount of time ago.
        else:
            last_timestamp = int(
                datetime.datetime(2023, 7, 31, 0, 0, 0).timestamp()*1000
            )
        print(symbol, last_timestamp)
        symbol_queue.put((symbol, last_timestamp))


def latest_market_time():
    # lmt = latest market time
    lmt1 = datetime.datetime.now().timestamp() * 1000 - 960000

    lmt2 = market_date_delta(datetime.datetime.today() + datetime.timedelta(days=1), -1)
    lmt2 = datetime.datetime.combine(lmt2, datetime.time(hour=20)) # (latest extended market hours data of all apis)
    lmt2 = lmt2.timestamp() * 1000  # Last open date
    return min(lmt1, lmt2)


def process_symbols(api, symbol_queue, result_queue):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame
    # Remember this function is what is parallelized

    while True:  # TODO replace with signal handling
        # Wait for api rate limit
        print(f'Waiting for {max(0, api.next_available_call_time() - time.time())} seconds')
        time.sleep(max(0, api.next_available_call_time() - time.time()))

        # Get next symbol to process
        try:
            symbol, start_time = symbol_queue.get(timeout=10)
        except queue.Empty:  # No more symbols to process
            return

        # Check if the symbol is in the time range for the API
        if datetime.datetime.fromtimestamp(start_time//1000) < api.earliest_possible_time():
            print(f'{symbol} not in time range for {api.name}')
            symbol_queue.put((symbol, start_time))
            continue

        # get result from the API
        print(symbol, api.name)
        result = api.api_call(symbol, start_time, int(datetime.datetime.now().timestamp()*1000))

        if result is None:
            print(f'No results from {symbol} + {api.name}')
            symbol_queue.put((symbol, start_time))
            continue
        print(f'New data points: {len(result.index)}')

        # If the last timestamp of the result is older that the timestamp of the latest available data
        # then add it back into to the queue
        # The latest possible timestamp is not this specific api's latest possible timestamp
        lmt = latest_market_time()
        if result['timestamp'].max() < lmt:
            # TODO this is causing ameritrade api to think the job is not in the available time range.
            #  Next time of open market could be hours or days later.
            #  Change here or at comparison with earliest_possible_time()
            symbol_queue.put((symbol, result['timestamp'].max() + 1))

        result_queue.put((symbol, result))


def distribute_requests(ticker_symbols):
    # Create a shared queue for symbols and a shared queue for results
    symbol_queue = queue.Queue()
    result_queue = queue.Queue()

    add_symbols_to_queue(ticker_symbols, symbol_queue)

    # Create and start a thread for each API
    threads = []
    for api in load_apis():
        thread = threading.Thread(
            target=process_symbols, args=(api, symbol_queue, result_queue)
        )
        thread.start()
        threads.append(thread)

    print('Threads started')

    # While threads are running
    while any(thread.is_alive() for thread in threads):
        # Try to get a result from the queue
        try:
            symbol, result = result_queue.get(timeout=1)  # Adjust timeout as needed
            save_new_data(symbol, result)
        except queue.Empty:
            continue

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
    distribute_requests(tcks)
