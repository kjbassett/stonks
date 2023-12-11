import time
import queue
import threading
import pandas as pd
import os
import importlib
import datetime
from useful_funcs import latest_market_time, market_date_delta, all_open_dates
from functools import partial
from icecream import ic
import tkinter as tk
from tkinter import ttk
from config import CONFIG
from db.database import Database

db = Database(CONFIG['db_folder'] + CONFIG['db_name'])
min_market_date = market_date_delta(CONFIG['min_date'])


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


def load_saved_data(company_id):
    """Load saved data for a symbol if it exists, otherwise return None."""
    min_ts = datetime.datetime.combine(CONFIG["min_date"], datetime.time.min).timestamp()
    query = f'SELECT * FROM TradingData WHERE company_id = ? AND timestamp > ? ORDER BY timestamp ASC'
    data = db(query, (company_id, min_ts), return_type='DataFrame')
    return data


def save_new_data(company_id, df):
    count_query = 'SELECT COUNT(company_id) FROM TradingData WHERE company_id = ?'
    old_n = db(count_query, (company_id,), return_type='DataFrame')['COUNT(company_id)'][0]

    df['company_id'] = company_id
    db.insert('TradingData', df)

    new_n = db(count_query, (company_id,), return_type='DataFrame')['COUNT(company_id)'][0]
    return new_n - old_n


def choose_best_api(request, apis):
    # The below 2 lines may not work if run outside the US/Eastern time zone
    start = datetime.datetime.fromtimestamp(request['start'])
    end = datetime.datetime.fromtimestamp(request['end'])

    best_api = None
    best_hours = 0
    for api in apis:
        if api['api'].name in request['excluded_apis']:
            print(f'skipping {api["api"].name} because it is excluded')
            continue
        hours = api["api"].covered_hours(start, end)
        if hours > best_hours:
            best_hours = hours
            best_api = api
        elif hours == best_hours > 0:
            if api['input_queue'].qsize() < best_api['input_queue'].qsize():
                best_hours = hours
                best_api = api

    return best_api


def find_gaps_in_data(df, gap_threshold=1800):
    # add dummy timestamps and end of time range to get all gaps
    ends = [
        int(datetime.datetime.combine(min_market_date, datetime.time(4)).timestamp()),
        latest_market_time(),
    ]

    # TODO is it faster to test if there are gaps on the ends before concat?
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

    # these two columns are needed for the adjust gap function
    df["prev_date"] = (
        pd.to_datetime(df.previous, unit="s", utc=True)
        .dt.tz_convert("US/Eastern")
        .dt.normalize()
    )
    df["days_apart"] = (df["date"] - df["prev_date"]).dt.days


    df["gap"] = df["timestamp"] - df["previous"]
    df = df.iloc[1:, ]

    # Todo filter > gap threshold here as well to speed up apply?
    df["gap"] = df.apply(partial(adjust_gap), axis=1)

    # gaps ranges are EXCLUSIVE except for dummy timestamps
    # This seems dangerous since we are saving timestamps and retrieving them later
    df.reset_index(drop=True, inplace=True)
    df.loc[1:, "previous"] += 60
    df.loc[:len(df)-1, "timestamp"] -= 60

    gaps = df[df["gap"] > gap_threshold]  # gaps > 30 minutes are counted
    # check if gap has been tried before for each api
    if len(gaps.index) == 0:
        return

    return gaps


def adjust_gap(row):
    # if previous timestamp is a previous day, don't include closed hours in the gap
    # only adjust if the data is on two separate days
    if row["days_apart"] == 0:
        return row["gap"]

    # Don't count the time outside the market hours
    # different by day depending on if the market was open or closed
    d1 = row["prev_date"].date()
    d2 = row["date"].date()
    i1 = all_open_dates.searchsorted(d1)  # todo memoize
    i2 = all_open_dates.searchsorted(d2)
    if i1 == len(all_open_dates) or all_open_dates[i1] != d1:
        raise ValueError(f"{d1} is not in {all_open_dates}")
    if i2 == len(all_open_dates) or all_open_dates[i2] != d2:
        raise ValueError(f"{d2} is not in {all_open_dates}")
    open_days = i2 - i1
    closed = row["days_apart"] - open_days
    # 28800 is the time between the end of one market day and the start of another.
    # 4 hrs after 8 pm and 4 hours before 4am is 8 hours * 3600 = 28800
    # 86400 is number of seconds in a day
    row["gap"] = row["gap"] - 28800 * open_days - 86400 * closed
    return row["gap"]


def build_request(company: pd.Series, apis: list):
    """
    Builds a request based on the data needed for a symbol and the api's ability to provide that data
    :param company: pd.Series of a company's information
    :param apis: objects containing the api's and other relation objects
    :return: the chosen api's input queue, the start time of the request, and the end time of the request
    """
    t = time.time()
    # min date is the earliest day since min_date that is covered by api limits
    min_date = max(CONFIG['min_date'], min([t["api"].info["date_range"]["min"] for t in apis]))
    # the day also has to be open
    min_date = market_date_delta(min_date)

    df = load_saved_data(company['id'])
    if df is None:
        start = datetime.datetime.combine(min_date, datetime.time(4)).timestamp()
        end = latest_market_time()
        # Could cause error if start and end in TradingDataGaps table
        return {'company': company, 'start': start, 'end': end, 'excluded_apis': []}
    else:
        gaps = find_gaps_in_data(df)
        if gaps is None or gaps.empty:
            return
        ptg = db('SELECT * FROM TradingDataGaps WHERE company_id = ?',
                 params=(company['id'], ), return_type='DataFrame')  # ptg = previously tried gaps
        for _, gap in gaps.iterrows():
            excluded_apis = list(ptg[(ptg['start'] == gap['previous']) & (ptg['end'] == gap['timestamp'])]['source'])
            # if 3 have tried, we can assume the other apis can't either
            if len(excluded_apis) < 3 and not all([api['api'].name in excluded_apis for api in apis]):
                start, end = int(gap["previous"]), int(gap["timestamp"])
                print(f'Build request in {time.time() - t} seconds')
                return {'company': company, 'start': start, 'end': end, 'excluded_apis': excluded_apis}
    print(f'Build request in {time.time() - t} seconds')


def process_requests(api, input_queue, result_queue, stop_event):
    # Load historical API call log if it exists, otherwise initialize an empty DataFrame
    # Remember this function is what is parallelized

    while not stop_event.is_set():
        # Wait for api rate limit
        sleep_duration = max(0, api.next_available_call_time() - time.time())
        if sleep_duration or input_queue.empty():
            time.sleep(1)
            continue

        request = input_queue.get_nowait()

        # get result from the API
        data = api.api_call(request['company']['symbol'], request['start'], request['end'])
        n = len(data) if data is not None else None
        ic(request['company']['symbol'], request['start'], request['end'], api.name, n)
        if n is not None and n <= 2:
            ic(data)

        result_queue.put(
            {**request, 'api_name': api.name, 'data': data}
        )


def create_components():
    stop_event = threading.Event()

    # Create a shared queue for symbols before they are assigned to an api's
    assign_queue = []
    # Create a shared queue for incoming results from other threads
    result_queue = queue.Queue()

    apis = []
    for api in load_apis():
        input_queue = queue.Queue()
        thread = threading.Thread(
            target=process_requests, args=(api, input_queue, result_queue, stop_event)
        )

        apis.append({"thread": thread, "input_queue": input_queue, "api": api})

    return {
        "assign_queue": assign_queue,
        "apis": apis,
        "result_queue": result_queue,
        "stop_event": stop_event,
    }


def distribute_requests(components: dict, companies: pd.DataFrame):
    # Add symbols to the queue for assignment to an api
    assign_queue = components["assign_queue"]
    apis = components["apis"]
    result_queue = components["result_queue"]
    stop_event = components["stop_event"]
    for _, cpy in companies.iterrows():
        assign_queue.append(cpy)
    for t in apis:
        t["thread"].start()

    # While threads are running
    while not stop_event.is_set():
        # Try to get a symbol from the queue
        if assign_queue:
            cpy = assign_queue.pop()
            # Evaluate data and api limitations to determine which one should be used and for what time period
            request = build_request(cpy, apis)
            if request or request['start'] is not None:  # No request covering new data possible
                api = choose_best_api(request, apis)
                if api:
                    api['input_queue'].put(request)

        # Try to get a result from the queue
        if result_queue.qsize() == 0:
            continue
        t = time.time()
        result = result_queue.get_nowait()
        # If empty or no new data, don't try that combo of api, symbol, start, and end again
        if result['data'] is None or result['data'].empty or not save_new_data(result['company']['id'], result['data']):
            print('No new data')
            query = f"INSERT INTO TradingDataGaps (source, company_id, start, end) VALUES (?, ?, ?, ?);"
            db(query, (result['api_name'], result['company']['id'], result['start'], result['end']))
        print(f'handled result in {time.time() - t} seconds')
        # Checked for gaps again. If no gaps, it won't be assigned again.
        assign_queue.append(result['company'])

    for api in apis:
        api["thread"].join()


def main():
    companies = db('SELECT * FROM Companies;', return_type='DataFrame')

    # Create Queues and Threads
    components = create_components()
    dist_thread = threading.Thread(
        target=distribute_requests,
        args=(
            components,
            companies
        ),
    )

    dist_thread.start()
    threading.Thread(target=update_ui).start()



# TODO
#  Explore other API calls
#   https://polygon.io/docs/stocks/get_v2_reference_news
# Keep in mind that this will be run every day


if __name__ == "__main__":
    print(
        f"Latest market data at: {datetime.datetime.fromtimestamp(latest_market_time())}"
    )
    main()
