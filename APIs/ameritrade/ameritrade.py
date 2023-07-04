import pandas as pd
import requests
import os
from APIs import API

# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory

DISABLED = False

name = os.path.split(__file__)[1].split(".")[0]
info = {
    "name": name,
    "limits": {"per_second": 2},
}


class API(API):
    def __init__(self):
        super().__init__(name, info)

    def api_call(self, symbol, start, end):
        url = f"https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory"
        params = {
            "apikey": self.api_key,
            "frequencyType": "minute",
            "frequency": 1,
            "startDate": start,
            "endDate": end,
            "needExtendedHoursData": "true",
        }
        try:
            response = requests.get(url, params=params)
            data = response.json()
            data = convert_to_df(data)
            self.log_call()
            return data
        except Exception as e:
            print(f'ERROR from {name} on api call')
            print(e)
            self.error_logger.error(
                f"Exception occurred for Ameritrade API on symbol {symbol} with url {url} and params {params}",
                exc_info=True,
            )


def convert_to_df(data):
    return pd.DataFrame(data["candles"]).rename(columns={'datetime': 'timestamp'})




if __name__ == "__main__":
    import datetime

    # start = int((datetime.datetime.now() - datetime.timedelta(days=0, hours=4)).timestamp() * 1000)
    # end = int((datetime.datetime.now() - datetime.timedelta(days=1, hours=4)).timestamp() * 1000)
    t1 = int(datetime.datetime(2023, 6, 28, 7, 0, 0).timestamp() * 1000)
    t2 = int(datetime.datetime(2023, 6, 28, 20, 0, 0).timestamp() * 1000)

    api = API()
    d = api.api_call(
        "AAPL",
        t1,
        t2,
    )

    print(d['timestamp'].min())
    print(d['timestamp'].max())
    print((t1 - d['timestamp'].min())/60000)
    print((t2 - d['timestamp'].max())/60000)
    print(f"{(d['timestamp'].max() - d['timestamp'].min())/len(d.index)/60000} minutes between timestamps")
    print(d[d['timestamp'] == 1687950000000])
    # 1688036400000 7:00 AM ET
    # 1687391940000 7:59 PM ET

