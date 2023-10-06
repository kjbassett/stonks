import time
import queue
import threading
import pandas as pd
import os
import importlib
import datetime
from useful_funcs import latest_market_time, get_open_dates, market_date_delta
from functools import partial
from icecream import ic
import tkinter as tk
from tkinter import ttk


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
        return pd.read_parquet(filepath, engine="fastparquet")
    else:
        return None


def save_new_data(symbol, df):
    """Save a DataFrame to a Parquet file, appending if the file already exists."""
    # TODO remove duplicates
    if df.empty:
        return
    filepath = os.path.join("../Data", f"{symbol}.parquet")
    if os.path.exists(filepath):
        df.to_parquet(filepath, engine="fastparquet", append=True)
    else:
        df.to_parquet(filepath, engine="fastparquet")


def choose_best_api(start, end, threads):
    # The below 2 lines may not work if run outside the US/Eastern time zone
    start = datetime.datetime.fromtimestamp(start)
    end = datetime.datetime.fromtimestamp(end)

    # delete this. It's just here for testing
    for th in threads:
        ic(th["api"].name, th["api"].covered_hours(start, end))

    ts = sorted(
        threads,
        key=lambda t: (
            -t["api"].covered_hours(
                start, end
            ),  # Sort first by covered hours, descending
            -t[
                "input_queue"
            ].qsize(),  # Then by queue size. TODO Would be better as queue time. Use Little's Law
        ),
    )
    if ts[0]["api"].covered_hours(start, end) > 0:
        return ts[0]


def find_gap_in_data(df, min_date):
    df.timestamp = df.timestamp / 1000  # TODO convert milliseconds to seconds

    # add in beginning and end of time range
    ends = [
        datetime.datetime.combine(min_date, datetime.time(4)).timestamp(),
        latest_market_time(),
    ]

    df = pd.concat([df, pd.DataFrame({"timestamp": ends})])
    df = df.sort_values(by="timestamp")

    # Previous timestamp
    df["previous"] = df["timestamp"].shift(1)

    # Convert timestamp and previous to datetime columns
    df["date"] = (
        pd.to_datetime(df.timestamp, unit="s", utc=True)
        .dt.tz_convert("US/Eastern")
        .dt.normalize()
    )
    df["prev_date"] = (
        pd.to_datetime(df.previous, unit="s", utc=True)
        .dt.tz_convert("US/Eastern")
        .dt.normalize()
    )
    df["days_apart"] = (df["date"] - df["prev_date"]).dt.days

    df["gap"] = df["timestamp"] - df["previous"]
    # if previous timestamp is a previous day, don't include closed hours in the gap
    # 28800 is the time at the tail ends of the gap. 4 hrs after 8 pm and 4 hours before 4am is 8 hours * 3600
    open_dates = get_open_dates(df["date"].dt.date.min(), datetime.date.today())
    df = df.iloc[1:, :]

    # these two columns are needed for the adjust gap function
    df["gap"] = df.apply(partial(adjust_gap, open_dates), axis=1)

    gaps = df[df["gap"] > 1800]  # gaps > 30 minutes are counted
    # check if gap has been tried before for each api
    if len(gaps.index) == 0:
        return
    return gaps.iloc[0]


def adjust_gap(open_dates, row):
    # only adjust if the data is on two separate days
    if row["days_apart"] > 0:
        # Don't count the time outside the market hours
        # Only count days the market is open. Gap could cover an open and a closed day.
        d1 = row["prev_date"].replace(tzinfo=None)
        d2 = row["date"].replace(tzinfo=None)
        i1 = open_dates.searchsorted(d1)
        i2 = open_dates.searchsorted(d2)
        if i1 == len(open_dates) or open_dates[i1] != d1:
            raise ValueError(f"{d1} is not in {open_dates}")
        if i2 == len(open_dates) or open_dates[i2] != d2:
            raise ValueError(f"{d2} is not in {open_dates}")
        open_days = i2 - i1
        closed = row["days_apart"] - open_days
        row["gap"] = row["gap"] - 28800 * open_days - 86400 * closed
    return row["gap"]


def build_request(symbol, threads, min_date):
    """
    Builds a request based on the data needed for a symbol and the api's ability to provide that data
    :param symbol: ticker symbol for a company
    :param threads: objects containing the api's and other relation objects
    :param min_date: The earliest date that we care about
    :return: the chosen api's input queue, the start time of the request, and the end time of the request
    """
    # min date is the earliest day since min_date that is covered by api limits
    min_date = max(min_date, min([t["api"].info["date_range"]["min"] for t in threads]))
    # the day also has to be open
    min_date = market_date_delta(min_date)

    df = load_saved_data(symbol)
    if df is None:
        start = datetime.datetime.combine(min_date, datetime.time(4)).timestamp()
        end = latest_market_time()
    else:
        gap = find_gap_in_data(df, min_date)
        if gap is None:
            return
        start, end = int(gap["previous"]), int(gap["timestamp"])
    print('building request for ' + symbol)
    api = choose_best_api(start, end, threads)
    if api is None:
        return

    return api["input_queue"], start, end


def process_symbols(api, input_queue, result_queue, stop_event):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame
    # Remember this function is what is parallelized

    while not stop_event.is_set():
        # Wait for api rate limit
        sleep_duration = max(0, api.next_available_call_time() - time.time())
        time.sleep(sleep_duration)

        # Get next symbol to process
        try:
            symbol, start, end = input_queue.get_nowait()
        except queue.Empty:
            time.sleep(1)
            continue

        # get result from the API
        ic(symbol, api.name)
        result = api.api_call(symbol, start * 1000, end * 1000)
        ic(len(result.index))
        result_queue.put((symbol, result))


def create_components():
    stop_event = threading.Event()

    # Create a shared queue for symbols before they are assigned to an api's
    assign_queue = []
    # Create a shared queue for incoming results from other threads
    result_queue = queue.Queue()

    threads = []
    for api in load_apis():
        input_queue = queue.Queue()
        thread = threading.Thread(
            target=process_symbols, args=(api, input_queue, result_queue, stop_event)
        )

        threads.append({"thread": thread, "input_queue": input_queue, "api": api})

    return {
        "assign_queue": assign_queue,
        "threads": threads,
        "result_queue": result_queue,
        "stop_event": stop_event,
    }


def distribute_requests(components, ticker_symbols, min_date):
    # Add symbols to the queue for assignment to an api
    assign_queue = components["assign_queue"]
    threads = components["threads"]
    result_queue = components["result_queue"]
    stop_event = components["stop_event"]
    for symbol in ticker_symbols:
        assign_queue.append(symbol)

    for t in threads:
        t["thread"].start()

    # While threads are running
    while not stop_event.is_set():
        # Try to get a symbol from the queue
        if assign_queue:
            symbol = assign_queue.pop()
            # Evaluate data and api limitations to determine which one should be used and for what time period
            request = build_request(symbol, threads, min_date)
            if request is None:  # No request covering new data possible
                continue
            input_queue, start, end = request
            input_queue.put((symbol, start, end))

        # Try to get a result from the queue
        try:
            symbol, result = result_queue.get_nowait()
            save_new_data(symbol, result)
            assign_queue.append(
                symbol
            )  # It will be checked for gaps again. If no gaps, it won't be assigned.
        except queue.Empty:
            pass
    for t in threads:
        t["thread"].join()


def main():
    ticker_symbols = pd.read_csv("../tickers.csv")["Ticker"].unique().tolist()
    min_date = datetime.date.today() - datetime.timedelta(days=730)

    # Create Queues and Threads
    components = create_components()
    dist_thread = threading.Thread(
        target=distribute_requests,
        args=(
            components,
            ticker_symbols,
            min_date,
        ),
    )

    # Create main window
    root = tk.Tk()
    root.title("Queue Monitor")

    # Frame to hold the labels
    frame = ttk.Frame(root, padding="10")
    frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    # Labels to display the queue lengths
    assign_queue_label = ttk.Label(frame, text="Assign Queue Length: 0")
    assign_queue_label.grid(row=0, column=0, sticky=tk.W, pady=5)

    input_queues_label = ttk.Label(frame, text="Input Queues Lengths: []")
    input_queues_label.grid(row=1, column=0, sticky=tk.W, pady=5)

    next_call_times_label = ttk.Label(frame, text="Next Call Times: []")
    next_call_times_label.grid(row=2, column=0, sticky=tk.W, pady=5)

    result_queue_label = ttk.Label(frame, text="Result Queue Length: 0")
    result_queue_label.grid(row=3, column=0, sticky=tk.W, pady=5)

    # Function to update the UI
    def update_ui():
        while not components['stop_event'].is_set():
            # Update the labels with queue lengths (use actual values here)
            assign_queue_label[
                "text"
            ] = f"Assign Queue Length: {len(components['assign_queue'])}"
            input_queues_label[
                "text"
            ] = f"Input Queues Lengths: {[t['input_queue'].qsize() for t in components['threads']]}"
            next_call_times_label[
                "text"
            ] = f"Next Call Times: {[round(t['api'].next_available_call_time() - time.time(), 1) for t in components['threads']]}"
            result_queue_label[
                "text"
            ] = f"Result Queue Length: {components['result_queue'].qsize()}"
            # Refresh every second (or another suitable interval)
            time.sleep(1)

    def stop_threads():
        components['stop_event'].set()
        # Optionally, you can wait for threads to finish using join, if needed
        for t in components['threads']:
            t['thread'].join()
        dist_thread.join()
        root.quit()

    # Button to stop the threads
    stop_button = ttk.Button(frame, text="Stop", command=stop_threads)
    stop_button.grid(row=4, column=0, pady=20)

    dist_thread.start()
    threading.Thread(target=update_ui).start()

    # Run the main loop
    root.mainloop()
    components['stop_event'].set()


# TODO
#  Fix stop button
#  covered_hours preferring alpha_vantage? Small chance it's correct. compare start and end of request to api time limits
#  All timestamps should be seconds
#  hit run and fix until it works
#  Explore other API calls
#   https://polygon.io/docs/stocks/get_v2_reference_news
# Keep in mind that this will be run every day


if __name__ == "__main__":
    print(
        f"Latest market data at: {datetime.datetime.fromtimestamp(latest_market_time() / 1000)}"
    )
    # tcks = ['AAPL', 'NVDA']
    # Clear error logs

    main()
