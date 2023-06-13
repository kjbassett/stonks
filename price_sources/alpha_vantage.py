import requests
import os
from price_sources import get_api_key

# https://www.alphavantage.co/documentation/#

name = os.path.split(__file__)[1].split('.')[0]
api_key = get_api_key(name)


def api_call(symbol, start, end):
    base_url = "https://www.alphavantage.co/query"
    function = "TIME_SERIES_INTRADAY"
    apikey = api_key
    payload = {
        "function": function,
        "symbol": symbol,
        "interval": "1min",
        "outputsize": "compact",
        "datatype": "json",
        "apikey": apikey,
    }
    response = requests.get(base_url, params=payload)
    data = response.json()
    print('ALPHA', data)
    return data


api_info = {
    "name": name,
    "limits": {"per_minute": 5, "per_day": 500},
}