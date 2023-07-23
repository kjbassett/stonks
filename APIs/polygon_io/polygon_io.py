from polygon import RESTClient
from polygon.exceptions import NoResultsError
from APIs.API import API
import os
import pandas as pd
import datetime

DISABLED = True

name = os.path.split(__file__)[1].split(".")[0]
info = {
    "name": name,
    "limits": {"per_second": 100, "per_minute": 5},
    "time_range": {
        "min": datetime.datetime.today() - datetime.timedelta(days=730),
        "max": datetime.date.today(),
    },
    "hours": {"min": 4, "max": 20}
}


class PolygonIO(API):
    def __init__(self):
        super().__init__(name, info)
        self.client = RESTClient(api_key=self.api_key)
        self.latest_timestamps = {}

    def _api_call(self, params):
        # TODO Convert (from start to end) into multiple calls
        data = self.client.get_aggs(**params)
        data = convert_to_df(data)
        self.latest_timestamps[params['symbol']] = data['timestamp'].max()
        return data

    def get_params(self, symbol, start, end):
        if symbol in self.latest_timestamps:
            if start < self.latest_timestamps[symbol]:
                start = self.latest_timestamps[symbol] + 1
        if start > end:
            return

        params = {
            "ticker": symbol,
            "multiplier": 1,
            "timespan": "minute",
            "from_": start,
            "to": end,
        }
        yield params


def convert_to_df(data):
    data = [
        {
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": agg.volume,
            "timestamp": agg.timestamp,
        }
        for agg in data
    ]

    df = pd.DataFrame(data)
    return df


# 4AM EST to 8PM EST or 8AM GMT to 12AM GMT


if __name__ == "__main__":
    import datetime

    # start = int((datetime.datetime.now() - datetime.timedelta(days=0, hours=4)).timestamp() * 1000)
    # end = int((datetime.datetime.now() - datetime.timedelta(days=1, hours=4)).timestamp() * 1000)
    t1 = int(datetime.datetime(2023, 6, 28, 4, 0, 0, 0).timestamp() * 1000)
    t2 = int(datetime.datetime(2023, 6, 28, 20, 0, 0, 0).timestamp() * 1000)

    api = PolygonIO()
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
    print(d[d["timestamp"] == 1687950000000])
    # 1687939200000 04:00 AM ET
    # 1687996740000 07:59 PM ET
