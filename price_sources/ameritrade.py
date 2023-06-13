import requests
import os
from price_sources import get_api_key

# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory
name = os.path.split(__file__)[1].split('.')[0]
api_key = get_api_key(name)


def api_call(symbol, start, end):
    url = f"https://api.tdameritrade.com/v1/marketdata/{symbol}/quotes"
    params = {
        "apikey": api_key,
        "periodType": "day",
        "period": 10,
        "needExtendedHoursData": True,
    }
    response = requests.get(url, params=params)
    data = response.json()
    print('AMERITRADE', data)
    return data


api_info = {
    "name": name,
    "limits": {"per_second": 2},
}
