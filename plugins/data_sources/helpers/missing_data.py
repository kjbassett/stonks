import asyncio
import datetime
from functools import partial

import pandas as pd

from config import CONFIG
from utils.market_calendar import (
    latest_market_time,
    market_date_delta,
    all_open_dates,
)

min_market_date = market_date_delta(CONFIG["min_date"])
min_market_ts = int(
    datetime.datetime.combine(min_market_date, datetime.time(4)).timestamp()
)


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

    # Todo filter > gap threshold here as well to speed up apply?
    current_data["gap"] = current_data.apply(partial(adjust_gap), axis=1)

    # gaps ranges are EXCLUSIVE except for dummy timestamps
    # This seems dangerous since we are saving timestamps and retrieving them later
    current_data.reset_index(drop=True, inplace=True)
    current_data.loc[1:, "previous"] += 60
    current_data.loc[: len(current_data) - 1, "timestamp"] -= 60

    gaps = current_data[
        current_data["gap"] > min_gap_size
    ]  # gaps > 30 minutes are counted
    gaps = gaps[["previous", "timestamp"]].rename(
        columns={"previous": "start", "timestamp": "end"}
    )
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


async def filter_out_past_attempts(db, table, gaps, company_id):
    table += "Gaps"
    # Check if gap already in corresponding gaps table
    ptg = await db(
        f"SELECT * FROM {table} WHERE company_id =?",
        params=(company_id,),
        return_type="DataFrame",
    )
    # left anti join gaps and ptg
    gaps = pd.merge(gaps, ptg, on=["start", "end"], how="outer", indicator=True)
    gaps = gaps[gaps["_merge"] == "left_only"].drop("_merge", axis=1)
    return gaps


async def fill_gap(
    client,
    db,
    table,
    get_data_func,
    save_data_func: callable,
    cpy: pd.Series,
    gap: pd.Series,
):
    start, end = gap["start"], gap["end"]
    print("STARTING API CALL")
    data = await get_data_func(client, cpy["symbol"], int(start), int(end))

    # save_new_data returns the number of rows inserted, so if it's 0,...
    # we don't want to try the gap again. We save the record of our attempt here
    if not data or not await save_data_func(db, data):
        query = f"INSERT INTO {table}Gaps (company_id, start, end) VALUES (?, ?, ?);"
        await db(query, (cpy["id"], start, end))


async def fill_gaps(
    client,
    db,
    table: str,
    load_data_func: callable,
    get_data_func: callable,
    save_data_func: callable,
    companies: pd.DataFrame,
    min_gap_size=1800,
):
    if not companies:
        companies = await db("SELECT * FROM Companies;", return_type="DataFrame")
    tasks = []
    for _, cpy in companies.iterrows():
        current_data = await load_data_func(db, cpy["id"], min_market_ts)
        gaps = await find_gaps(current_data, min_gap_size)
        gaps = await filter_out_past_attempts(db, table, gaps, cpy["id"])
        for _, gap in gaps.iterrows():
            # Create a task for each gap handling
            task = asyncio.create_task(
                fill_gap(client, db, table, get_data_func, save_data_func, cpy, gap)
            )
            tasks.append(task)
            print("TASK CREATED FOR GAP")
            print(gap)
    # Wait for all tasks to complete
    print("WAITING FOR TASKS")
    await asyncio.gather(*tasks)
    print("TASKS COMPLETE")
