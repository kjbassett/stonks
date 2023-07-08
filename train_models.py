import datetime
import os

import numpy as np
import pandas as pd

from ameritrade_api import get_changes
from useful_funcs import calc_date

from EZMT import ModelTuner


def update_training_data(days):
    """
    Updates and returns all training data according to real world time.
    Unfiltered data is saved for faster load. Filtered (~y.isnull) data is returned
    :param days: The number days forward your model predicts
    :return: DataFrame of all up-to-date training data
    """
    max_date = datetime.datetime.today()  # latest possible date
    if max_date.hour < 9:
        max_date -= datetime.timedelta(days=1)
    max_date = max_date.date()
    max_date = calc_date(
        max_date, -days
    )  # Latest date of complete data (assuming ending price is collected at 9:00am)

    if os.path.exists(f"TrainingData\\{days}.csv"):
        tdata = pd.read_csv(f"TrainingData\\{days}.csv")
        tdata["Date"] = tdata.apply(
            lambda row: datetime.datetime.strptime(row["Date"], "%Y-%m-%d").date(),
            axis=1,
        )
        last_date = tdata["Date"].max()  # date last calculated
        if last_date >= max_date:
            return tdata
    else:
        tdata = None
        last_date = datetime.date(1900, 1, 1)

    # new for training data, not for today
    new_data = compile_recs(max_date, days, min_date=last_date)
    new_data = new_data[new_data["Date"] > last_date]
    if isinstance(tdata, pd.DataFrame):
        tdata = pd.concat([tdata, new_data], ignore_index=True)
    else:
        tdata = new_data.reset_index()
    # Even if price change isn't available on a certain date, ratings are used to find dratings for previous days.
    tdata.to_csv(f"TrainingData\\{days}.csv", index=False)
    return tdata


def compile_sources(one_hot=False):
    # Returns dataframe of date, ticker, and the source that recommended it on that day.
    df = pd.DataFrame(columns=["Date", "Ticker", "Source"])
    for fol in os.listdir("DailyRecs"):
        path = "DailyRecs/" + f"{fol}/" + "Symbols.csv"
        if os.path.exists(path):
            new = pd.read_csv(path)
            new["Date"] = datetime.datetime.strptime(fol, "%Y-%m-%d").date()
            df = pd.concat([df, new], ignore_index=True)

    if one_hot:
        df["val"] = 1
        df = df.pivot(index=["Date", "Ticker"], columns=["Source"], values=["val"])
        df = df.fillna(0)
        df.columns = ["Source_" + col[1] for col in df.columns]
    return df


def prefilter(data, date, days):
    # Cutoff invalid days in historical data
    data = data[data["Date"] <= calc_date(date, -days)]

    # Remove rows from data if y is unavailable
    data = data.replace([np.inf, -np.inf], np.nan)
    data = data[~data["y"].isna()]

    # Remove Outliers
    # historical = historical[np.abs(stats.zscore(historical['y'], nan_policy='omit')) < 3]

    return data


def missing_filter(data, row_limit, column_limit, rows_first=True):
    # Remove rows/columns of data from both datasets if it's nan% > row_or_column_limit%
    if rows_first:
        data = data[data.isnull().sum(axis=1) / len(data.columns) < row_limit]
        for col in data.columns:
            if data[col].isna().sum() > column_limit * len(data.index):
                data = data.drop(columns=[col])
    else:
        for col in data.columns:
            if data[col].isna().sum() > column_limit * len(data.index):
                data = data.drop(columns=[col])
        data = data[data.isnull().sum(axis=1) / len(data.columns) < row_limit]
    return data


def compile_recs(max_date, days, min_date=datetime.date(1900, 1, 1), change=True):
    df = pd.DataFrame()
    for fld in os.listdir("DailyRecs"):
        try:
            date = datetime.datetime.strptime(fld, "%Y-%m-%d").date()
            if date > max_date or date < min_date:
                continue
            next_ = pd.read_csv(f"DailyRecs\\{fld}\\main{fld}.csv")

            next_["Date"] = date
            if change:
                next_ = get_changes(
                    next_, date, days, save=True
                )  # changes in y variable
            print(f"Using Data from {date}")
            df = pd.concat([df, next_], ignore_index=True)
        except ValueError:
            continue

    if "index" in df.columns:
        df.drop("index", axis=1, inplace=True)
    return df


def main(date, prediction_range, pool=None):
    # Todo add update_training_data to ModelTuner as step to see which prediction range is best
    hist_data = update_training_data(
        prediction_range
    )  # update in case any past days are missing
    # Todo make sure Symbol is part of the index, no dates

    # Get SRC columns in one-hot format
    sources = compile_sources(one_hot=True)
    hist_data = hist_data.merge(sources, how="inner")

    # TODO use a binary variable indicating missing values for each feature if MNAR or MCAR

    hist_data = prefilter(hist_data, date, prediction_range)

    mt = ModelTuner(hist_data, "y", generations=1, pop_size=20, goal="min")

    X, y = hist_data.drop("y", axis=1), hist_data["y"]


if __name__ == "__main__":
    main()
