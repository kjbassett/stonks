import logging
import os
import pandas as pd
import time
from useful_funcs import get_api_key, market_date_delta
import datetime
from icecream import ic


durations = {"per_second": 1, "per_minute": 60, "per_hour": 3600, "per_day": 86400}


class BaseAPI:
    def __init__(self, name, info):
        self.name = name
        self.info = info
        self.api_key = get_api_key(name)
        self.error_logger = None
        self.call_log = None
        self.create_error_logger()
        self.load_call_log()

    def api_call(self, symbol, start, end):
        try:
            # split into multiple api calls due to api limits
            params = self.get_params(symbol, start, end)
            cols = ["open", "high", "low", "close", "volume", "timestamp"]
            all_data = pd.DataFrame(columns=cols)
            for param in params:
                ic(param)
                # determine if api needs to wait
                wait = self.next_available_call_time() - time.time()
                ic(wait)
                if wait < 10:
                    time.sleep(max(wait, 0))
                else:
                    break  # return data if waiting too long

                try:
                    self.log_call()
                    data = self._api_call(param)[cols]
                    all_data = pd.concat([all_data, data])
                except Exception as e:
                    print(f"ERROR from {self.name} on _api_call")
                    ic(e)
                    self.error_logger.error(
                        f"Exception occurred for {self.name}._api_call on symbol {symbol} with parameters {param}",
                        exc_info=True,
                    )
                    break

            all_data = all_data.drop_duplicates()
            all_data = all_data[
                (all_data["timestamp"] >= start) & (all_data["timestamp"] <= end)
            ]
            return all_data
        except Exception as e:
            print(f"ERROR from {self.name} on api_call")
            ic(e)
            self.error_logger.error(
                f"Exception occurred for {self.name}.api_call on symbol {symbol} with start {start} and end {end}",
                exc_info=True,
            )

    def earliest_possible_time(self):
        # Get the next open day AFTER the minimum day - 1
        ept = self.info["date_range"]["min"] - datetime.timedelta(days=1)
        ept = market_date_delta(ept, 1)
        ept = datetime.datetime.combine(ept, datetime.time(hour=self.info["hours"]["min"]))
        ic(ept)
        return ept

    def latest_possible_time(self):
        # Get the last open day BEFORE the maximum day + 1
        lpt = self.info["date_range"]["max"] + datetime.timedelta(days=1)
        lpt = market_date_delta(lpt, -1)
        lpt = datetime.datetime.combine(lpt, datetime.time(hour=self.info["hours"]["max"]))
        lpt = min(lpt, datetime.datetime.now() - self.info["delay"])
        ic(lpt)
        return lpt

    def create_error_logger(self):
        # Create a logger
        self.error_logger = logging.getLogger(__name__)
        self.error_logger.setLevel(logging.INFO)

        # Create handler and formatter for logger
        handler = logging.FileHandler(self.name + "/errors.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.error_logger.addHandler(handler)

    def load_call_log(self):
        if os.path.exists(self.name + "/call_log.csv"):
            self.call_log = pd.read_csv(self.name + "/call_log.csv", header=None).iloc[:, 0]
        else:
            self.call_log = pd.Series()

    def log_call(self):
        # Log the call
        ic('logging call')
        t = time.time()
        self.call_log.at[len(self.call_log)] = t

        # Filter out old logged calls
        longest_limit_type = max(self.info["limits"], key=self.info["limits"].get)  # Get key of the longest limit
        self.call_log = self.call_log[
            self.call_log >= t - durations[longest_limit_type]
        ]

        # Save the log
        self.call_log.to_csv(self.name + "/call_log.csv", index=False, header=False)

    def next_available_call_time(self):
        # update next_available_call_time
        latest = time.time()
        for limit_type, limit_value in self.info["limits"].items():
            recent_calls = self.call_log[
                self.call_log > time.time() - durations[limit_type]
            ]
            if recent_calls.size >= limit_value:
                latest = max(latest, recent_calls.min() + durations[limit_type])
        return latest

    def calculate_day_covered_hours(self, day, day_start_time, day_end_time):
        if not (self.info["date_range"]["min"] < day < self.info["date_range"]["max"]):
            return 0
        api_start_time = datetime.datetime.combine(day, datetime.time(self.info['hours']['min']))
        api_end_time = datetime.datetime.combine(day, datetime.time(self.info['hours']['max']))

        overlap_start_time = max(day_start_time, api_start_time)
        overlap_end_time = min(day_end_time, api_end_time)

        return max(0, (overlap_end_time - overlap_start_time).seconds / 3600)  # convert to hours

    def covered_hours(self, start, end):
        if start > end:
            start, end = end, start

        first_day = max(start.date(), self.info["date_range"]["min"])
        last_day = min(end.date(), self.info["date_range"]["max"])
        if first_day > last_day:
            return 0
        days = (last_day - first_day).days + 1
        hours = 0

        # Adjust for first day
        if first_day == start.date():
            hours += self.calculate_day_covered_hours(first_day, start, datetime.datetime.combine(first_day, datetime.time(23, 59)))
            days -= 1
        # Adjust for last day
        if last_day == end.date():
            hours += self.calculate_day_covered_hours(last_day, datetime.datetime.combine(last_day, datetime.time(0, 0)), end)
            days -= 1

        hours += days * (self.info['hours']['max'] - self.info['hours']['min'])

        return hours

    def _api_call(self, params):
        raise NotImplementedError("API._api_call not defined")

    @staticmethod
    def get_params(symbol, start, end):
        # splits the job into multiple jobs as a list of dicts of parameters for request
        raise NotImplementedError("API.split_job not defined")
