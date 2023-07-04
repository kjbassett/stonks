import time
import queue
import threading
import pandas as pd
import os
import importlib
import datetime


def load_apis():
    apis = []
    subfolders = [f.path for f in os.scandir(os.getcwd()) if f.is_dir() and f.name[0] != '_']
    for fol in subfolders:
        file = os.path.split(fol)[1]
        file = file + '.' + file
        apis.append(importlib.import_module(file, package='APIs').API())
    return apis


def load_saved_data(symbol):
    """Load saved data for a symbol if it exists, otherwise return None."""
    filepath = os.path.join('../Data', f"{symbol}.parquet")
    if os.path.exists(filepath):
        return pd.read_parquet(filepath)
    else:
        return None


def save_new_data(symbol, df):
    """Save a DataFrame to a Parquet file, appending if the file already exists."""
    filepath = os.path.join('../Data', f"{symbol}.parquet")
    if os.path.exists(filepath):
        df.to_parquet(filepath, append=True)
    else:
        df.to_parquet(filepath)


def add_symbols_to_queue(ticker_symbols, symbol_queue):
    """Add symbols to the queue, checking the last timestamp of saved data for each one."""
    for symbol in ticker_symbols:
        data = load_saved_data(symbol)
        if data is not None:
            last_timestamp = data.index[-1]
            # You could add a check here to only add the symbol to the queue if the last timestamp
            # is more than a certain amount of time ago.
        else:
            last_timestamp = None
        print(symbol,  last_timestamp)
        symbol_queue.put((symbol, last_timestamp))


def process_symbols(api, symbol_queue, result_queue):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame

    while True:  # TODO replace with signal handling
        # Wait for api rate limit
        time.sleep(max(0, api.next_available_time() - time.time()))
        try:
            # TODO If we end up putting things back into the symbol queue, this will need to wait
            symbol, start_time = symbol_queue.get_nowait()
        except queue.Empty:  # No more symbols to process
            return
        result = api.api_call(symbol, start_time, datetime.datetime.now())
        if result is not None:
            result_queue.put(symbol, result)
        else:
            symbol_queue.put(symbol, start_time)


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

    # Create an empty DataFrame to hold all results
    all_results = pd.DataFrame()

    # While threads are running
    while any(thread.is_alive() for thread in threads):
        # Try to get a result from the queue
        try:
            symbol, result = result_queue.get(timeout=1)  # Adjust timeout as needed
            save_new_data(symbol, result)
            all_results = all_results.append(result, ignore_index=True)
            save_new_data(symbol, result)
        except queue.Empty:
            continue  # No results available yet, continue waiting

    # After all threads have finished, make sure to process remaining results in the queue
    while not result_queue.empty():
        symbol, result = result_queue.get()
        all_results = all_results.append(result, ignore_index=True)
        save_new_data(symbol, result)

    return all_results

# TODO
# alpha vantage docs say "You can query both raw (as-traded) and split/dividend-adjusted (default) intraday data."
# hit run and fix until it works

# Keep in mind that this will be run every day

if __name__ == "__main__":
    tcks = pd.read_csv("../tickers.csv")["Ticker"].unique().tolist()
    distribute_requests(tcks)
