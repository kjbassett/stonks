import requests
from APIs.API import API
import pandas as pd
import os
import datetime

# https://www.alphavantage.co/documentation/#

DISABLED = True

name = os.path.split(__file__)[1].split(".")[0]
info = {
    "name": name,
    "limits": {"per_minute": 5, "per_day": 500},
    "time_range": {"min": datetime.date(2000, 1, 1), "max": datetime.date.today()},
    "hours": {"min": 4, "max": 20}
}
# TODO Convert (from start to end) into monthly calls


class API(API):
    def __init__(self):
        super().__init__(name, info)

    def _api_call(self, symbol, start, end):
        base_url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "1min",
            "outputsize": "full",
            "datatype": "json",
            "apikey": self.api_key,
        }
        response = requests.get(base_url, params=params)
        data = response.json()
        data = convert_to_df(data)
        data = data[(data["timestamp"] >= start) & (data["timestamp"] <= end)]
        data = convert_to_df(data)
        self.log_call()
        return data


def convert_to_df(data):
    price_data = data["Time Series (1min)"]
    df = pd.DataFrame.from_dict(price_data, orient="index")

    df = df.apply(pd.to_numeric)
    df.index = pd.to_datetime(df.index)

    # Convert the datetime index to Unix timestamp in milliseconds
    df.index = df.index.tz_localize(
        "US/Eastern"
    )  # replace 'US/Eastern' with the actual timezone if it's different
    df.index = df.index.tz_convert("UTC")
    df["timestamp"] = (df.index - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(
        "1ms"
    )

    df.reset_index(drop=True, inplace=True)
    df.columns = ["open", "high", "low", "close", "volume", "timestamp"]

    return df


if __name__ == "__main__":
    import datetime

    # start = int((datetime.datetime.now() - datetime.timedelta(days=0, hours=4)).timestamp() * 1000)
    # end = int((datetime.datetime.now() - datetime.timedelta(days=1, hours=4)).timestamp() * 1000)
    t1 = int(datetime.datetime(2023, 7, 3, 9, 30, 0, 0).timestamp() * 1000)
    t2 = int(datetime.datetime(2023, 7, 3, 10, 0, 0, 0).timestamp() * 1000)

    api = API()
    d = api.api_call(
        "AAPL",
        t1,
        t2,
    )

    print(d["timestamp"].min())
    print(d["timestamp"].max())
    print((t1 - d["timestamp"].min()) / 60000)
    print((t2 - d["timestamp"].max()) / 60000)
    print(
        f"{(d['timestamp'].max() - d['timestamp'].min()) / len(d.index) / 60000} minutes between timestamps"
    )
    # 1687939200000 04:00 AM ET
    # 1687996740000 07:59 PM ET