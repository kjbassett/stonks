from functools import partial
import datetime
import pandas as pd
from useful_funcs import latest_market_time, market_date_delta, all_open_dates
from config import CONFIG
import time
min_market_date = market_date_delta(CONFIG['min_date'])


async def load_saved_data(db, table, company_id, min_timestamp=0):
    """Load saved data for a symbol if it exists, otherwise return None."""
    query = f'SELECT * FROM {table} WHERE company_id = ? AND timestamp > ? ORDER BY timestamp ASC'
    data = await db(query, (company_id, min_timestamp), return_type='DataFrame')
    return data


async def save_new_data(db, table, company_id, df):
    count_query = f'SELECT COUNT(company_id) FROM {table} WHERE company_id = ?'
    old_n = await db(count_query, (company_id,), return_type='DataFrame')['COUNT(company_id)'][0]

    df['company_id'] = company_id
    await db.insert(table, df)

    new_n = await db(count_query, (company_id,), return_type='DataFrame')['COUNT(company_id)'][0]
    return new_n - old_n


async def find_gaps(db, table, company, min_gap_size):
    min_ts = int(datetime.datetime.combine(min_market_date, datetime.time(4)).timestamp())
    df = await load_saved_data(db, table, company['id'], min_timestamp=min_ts)
    # add dummy timestamps and end of time range to get all gaps
    ends = [min_ts, latest_market_time()]

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

    gaps = df[df["gap"] > min_gap_size]  # gaps > 30 minutes are counted
    gaps = gaps[["previous", "timestamp"]].rename(columns={"previous": "start", "timestamp": "end"})
    # check if gap has been tried before for each api
    gaps = await filter_out_past_attempts(db, table, gaps, company['id'])

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
    table += 'Gaps'
    # Check if gap already in corresponding gaps table
    ptg = await db(f'SELECT * FROM {table} WHERE company_id =?',
                   params=(company_id,),
                   return_type='DataFrame')
    # left anti join gaps and ptg
    gaps = pd.merge(gaps, ptg, on=['start', 'end'], how='outer', indicator=True)
    gaps = gaps[gaps['_merge'] == 'left_only'].drop('_merge', axis=1)
    return gaps


async def fill_gaps(db, table, data_func, companies: pd.DataFrame, min_gap_size=1800):
    for _, cpy in companies.iterrows():
        gaps = await find_gaps(db, table, cpy, min_gap_size)
        for _, gap in gaps.iterrows():
            t = time.time()
            start, end = gap['start'], gap['end']
            data = data_func(cpy['symbol'])
            if not data or not await save_new_data(db, table, cpy['id'], data):
                print('No new data')
                query = f"INSERT INTO {table}Gaps (company_id, start, end) VALUES (?, ?, ?);"
                await db(query, (cpy['id'], start, end))
            print(f'handled result in {time.time() - t} seconds')
