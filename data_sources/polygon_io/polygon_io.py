from polygon import RESTClient
from data_sources.BaseAPI.BaseAPI import BaseAPI
import os
import datetime
from icecream import ic


class API(BaseAPI):
    def __init__(self):
        name = os.path.split(__file__)[1].split(".")[0]
        info = {
            "name": name,
            "limits": {"per_second": 100, "per_minute": 5},
            "date_range": {
                "min": datetime.date.today() - datetime.timedelta(days=730),
                "max": datetime.date.today() - datetime.timedelta(days=1)
            },
            "hours": {"min": 4, "max": 20},
            'delay': datetime.timedelta()
        }
        super().__init__(name, info)
        self.client = RESTClient(api_key=self.api_key)
        self.latest_timestamps = {}

    async def _api_call(self, params):
        print('API CALL ' + self.name)
        ic(params)
        data = self.client.get_aggs(**params)
        ic(data)
        if data:
            ic(dir(data[0]))
        data = [{
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": agg.volume,
            "timestamp": agg.timestamp
            } for agg in data]
        if data:
            self.latest_param_end = data['timestamp'].max() / 1000
        return data

    def get_params(self, symbol, start, end):
        self.latest_param_end = 0
        while True:
            if start < self.latest_param_end:
                start = self.latest_param_end + 1
            if start >= min(end, self.latest_possible_time().timestamp()):
                return
            params = {
                "ticker": symbol,
                "multiplier": 1,
                "timespan": "minute",
                "from_": int(start * 1000),
                "to": int(end * 1000),
                "limit": 50000
            }
            yield params


# 4AM EST to 8PM EST or 8AM GMT to 12AM GMT


if __name__ == "__main__":
    # start = int((datetime.datetime.now() - datetime.timedelta(days=0, hours=4)).timestamp() * 1000)
    # end = int((datetime.datetime.now() - datetime.timedelta(days=1, hours=4)).timestamp() * 1000)
    t1 = int(datetime.datetime(2023, 6, 28, 4, 0, 0, 0).timestamp() * 1000)
    t2 = int(datetime.datetime(2023, 6, 28, 20, 0, 0, 0).timestamp() * 1000)

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
    print(d[d["timestamp"] == 1687950000000])
    # 1687939200000 04:00 AM ET
    # 1687996740000 07:59 PM ET