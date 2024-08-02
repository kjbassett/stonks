import asyncio
import datetime
from functools import partial

import pandas as pd
from config import CONFIG
from data_access.dao_manager import dao_manager
from utils.market_calendar import (
    latest_market_time,
    market_date_delta,
    all_open_dates,
)

min_market_date = market_date_delta(CONFIG["min_date"])
min_market_ts = int(
    datetime.datetime.combine(min_market_date, datetime.time(4)).timestamp()
)
cmp = dao_manager.get_dao("Company")


async def find_gaps(current_data, min_gap_size):
    # add dummy timestamps and end of time range to get all gaps
    ends = [min_market_ts, latest_market_time()]
    # TODO is it faster to test if there are gaps on the ends before concat?
    current_data = pd.concat([current_data, pd.DataFrame({"timestamp": ends})])
    current_data = current_data.sort_values(by="timestamp")

    # Previous timestamp
    current_data["previous"] = current_data["timestamp"].shift(1)

    # Convert timestamp and previous to datetime columns
    current_data["date"] = (
        pd.to_datetime(current_data.timestamp, unit="s", utc=True)
        .dt.tz_convert("US/Eastern")
        .dt.normalize()
    )

    # these two columns are needed for the adjust gap function
    current_data["prev_date"] = (
        pd.to_datetime(current_data.previous, unit="s", utc=True)
        .dt.tz_convert("US/Eastern")
        .dt.normalize()
    )
    current_data["days_apart"] = (
        current_data["date"] - current_data["prev_date"]
    ).dt.days

    current_data["gap"] = current_data["timestamp"] - current_data["previous"]
    current_data = current_data.iloc[1:,]

    # Todo filter < gap threshold here as well to speed up apply? because adjusting gap can only make it smaller
    current_data["gap"] = current_data.apply(partial(adjust_gap), axis=1)

    # We choose to save a previously fetched time range in TradingDataGaps and NewsDataGaps table IF there were NO
    # results in the time range, so we can't include any row from current_data because we know there is data.
    # In other words, we might query the API and get no data BETWEEN t1 and t2, but we got data at timestamps t1 and t2.
    # So the gap isn't saved when it should have been.
    # ^ To fix this, we want to query for a time range of (t1 + 60, t2 - 60) unless either t1 or t2 are dummy timestamps

    current_data.reset_index(drop=True, inplace=True)
    current_data.loc[1:, "previous"] += 60  # see comments above
    current_data.loc[: len(current_data) - 1, "timestamp"] -= 60  # see comments above

    gaps = current_data[
        current_data["gap"] > min_gap_size
    ]  # gaps > 30 minutes are counted
    gaps = gaps[["previous", "timestamp"]].rename(
        columns={"previous": "start", "timestamp": "end"}
    )
    return gaps


def adjust_gap(row):
    if row["days_apart"] == 0:
        return row["gap"]

    # if previous timestamp is a previous day, don't count the time outside the market hours
    # different by day depending on if the market was open or closed
    d1 = row["prev_date"].date()
    d2 = row["date"].date()
    # Get index of previous and current day from array of all open dates
    i1 = all_open_dates.searchsorted(d1)  # todo memoize
    i2 = all_open_dates.searchsorted(d2)
    if i1 == len(all_open_dates) or all_open_dates[i1] != d1:
        raise ValueError(f"{d1} is not in {all_open_dates}")
    if i2 == len(all_open_dates) or all_open_dates[i2] != d2:
        raise ValueError(f"{d2} is not in {all_open_dates}")
    open_days = i2 - i1
    closed = row["days_apart"] - open_days
    # 28800 is the time between the end of one market day and the start of another. 8pm to 4am = 8 hours * 3600 = 28800
    # 86400 is number of seconds in a day
    row["gap"] = row["gap"] - 28800 * open_days - 86400 * closed
    return row["gap"]


async def filter_out_past_attempts(table, gaps, company_id):
    gap_table = table + "Gap"
    # Check if gap already in corresponding gap table
    # ptg = previously tried gaps
    ptg = await dao_manager.get_dao(gap_table).get(company_id=company_id)
    # left anti join gap and ptg
    gaps = pd.merge(gaps, ptg, on=["start", "end"], how="outer", indicator=True)
    gaps = gaps[gaps["_merge"] == "left_only"]
    gaps = gaps[["start", "end"]]
    return gaps


async def fill_gap(
    client,
    table,
    get_data_func,
    save_data_func: callable,
    cpy: pd.Series,
    gap: pd.Series,
):
    start, end = gap["start"], gap["end"]
    print("STARTING API CALL")
    data = await get_data_func(client, cpy["symbol"], int(start), int(end))

    # save_new_data returns the number of rows inserted, so if it's 0,
    #   we don't want to try this gap again. We save the record of our attempt here
    if not data or not await save_data_func(cpy["id"], data):
        gap_table = f"{table}Gap"
        await dao_manager.get_dao(gap_table).insert((cpy["id"], start, end))


async def fill_gaps(
    client,
    table: str,
    load_data_func: callable,
    get_data_func: callable,
    save_data_func: callable,
    companies: str,
    min_gap_size=1800,
):
    if not companies or companies == "all":
        companies = await cmp.get_all()
    else:
        companies = companies.replace(" ", "").split(",")
        companies = pd.DataFrame({"symbol": companies})
    # TODO some function that takes a list of tickers and gives a dataframe of companies, fetches new if necessary
    tasks = []
    for _, cpy in companies.iterrows():
        current_data = await load_data_func(cpy["id"], min_market_ts)
        gaps = await find_gaps(current_data, min_gap_size)
        gaps = await filter_out_past_attempts(table, gaps, cpy["id"])
        for _, gap in gaps.iterrows():
            # Create a task for each gap handling
            task = asyncio.create_task(
                fill_gap(client, table, get_data_func, save_data_func, cpy, gap)
            )
            tasks.append(task)
            print("TASK CREATED FOR GAP")
    # Wait for all tasks to complete
    print("WAITING FOR TASKS")
    await asyncio.gather(*tasks)
    print("TASKS COMPLETE")
