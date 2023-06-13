from polygon import RESTClient
from price_sources import get_api_key
import os

name = os.path.split(__file__)[1].split('.')[0]
api_key = get_api_key(name)
client = RESTClient(api_key=api_key)


def api_call(symbol, start, end):
    start = start.strftime("%Y-%m-%d")
    end = end.strftime("%Y-%m-%d")
    data = client.get_aggs(
        ticker=symbol, multiplier=1, timespan="minute", from_=start, to=end
    )
    print('POLYGON', data)
    return data


api_info = {
    "name": name,
    "limits": {"per_second": 100, "per_minute": 5}
}
