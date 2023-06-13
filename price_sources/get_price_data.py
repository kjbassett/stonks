import time
import queue
import threading
import pandas as pd
import os
import importlib
import datetime


def process_symbols(api_call, api_info, symbol_queue):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame
    try:
        api_log = pd.read_csv(f'{api_info["name"]}_log.csv')
    except FileNotFoundError:
        api_log = pd.DataFrame(columns=["api_name", "timestamp"])

    results = dict()
    while True:
        # Check rate limits for both per_second and per_minute
        for limit_type, limit_value in api_info["limits"].items():
            # Count the number of API calls made within the rate limit duration
            duration = 1 if limit_type == "per_second" else 60
            recent_calls = api_log[
                (api_log["api_name"] == api_info["name"])
                & (api_log["timestamp"] > time.time() - duration)
            ].shape[0]

            # If the rate limit has been met, pause for the remaining duration
            if recent_calls >= limit_value:
                time.sleep(duration)  # TODO smarter way to do this
            else:
                try:
                    symbol = symbol_queue.get_nowait()
                except queue.Empty:  # No more symbols to process
                    # Save API call log before exiting
                    api_log.to_csv("api_log.csv", index=False)
                    return
                else:
                    results[symbol] = api_call(
                        symbol,
                        datetime.datetime(2023, 6, 5, 0, 0),
                        datetime.datetime(2023, 6, 12, 0, 0),
                    )
                    # Add a new row to the log
                    api_log.loc[len(api_log.index)] = [api_info["name"], time.time()]
                    # Append new row to the csv file
                    with open("api_log.csv", "a") as f:
                        f.write(f"{api_info['name']},{time.time()}\n")


def distribute_requests(ticker_symbols):
    # Create a single queue for all symbols
    symbol_queue = queue.Queue()

    # Add the symbols to the queue
    for symbol in ticker_symbols:
        symbol_queue.put(symbol)

    # Create and start a thread for each API
    threads = []
    for api in load_apis():
        thread = threading.Thread(
            target=process_symbols, args=(api.api_call, api.api_info, symbol_queue)
        )
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
        print(thread)


def load_apis():
    directory = os.getcwd()
    this_file = os.path.split(__file__)[1]

    # Get a list of all .py files in the directory
    files = [
        f[:-3]
        for f in os.listdir(directory)
        if f.endswith(".py") and f != this_file and f != "__init__.py"
    ]

    # Import all the files
    modules = []
    for f in files:
        module = importlib.import_module(f)
        for attr in ("api_call", "api_info"):
            if not hasattr(module, attr):
                print(f"{f} does not have {attr}")
        modules.append(module)

    return modules


# TODO
# Add start and end times as args for each API call
# Convert responses to common format
# Come up with common format
# handling of request errors
# write to file
# Decide between intraday and intraday (Extended History)

# Keep in mind that this will be run every day

if __name__ == "__main__":
    tcks = pd.read_csv("../tickers.csv")["Ticker"].unique().tolist()
    distribute_requests(tcks)
