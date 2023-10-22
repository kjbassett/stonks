import logging
import os
import pandas as pd
import time
from useful_funcs import get_api_key, market_date_delta, filter_open_dates
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
        start, end = int(start), int(end)
        try:
            # split into multiple api calls due to api limits
            params = self.get_params(symbol, start, end)
            cols = ["open", "high", "low", "close", "volume", "timestamp"]
            all_data = pd.DataFrame(columns=cols)
            for param in params:
                ic(param)
                # determine if api needs to wait
                wait = self.next_available_call_time() - time.time()
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
        return ept

    def latest_possible_time(self):
        # Get the last open day BEFORE the maximum day + 1
        lpt = self.info["date_range"]["max"] + datetime.timedelta(days=1)
        lpt = market_date_delta(lpt, -1)
        lpt = datetime.datetime.combine(lpt, datetime.time(hour=self.info["hours"]["max"]))
        lpt = min(lpt, datetime.datetime.now() - self.info["delay"])
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
        if os.path.exists(self.name + "/calls.log"):
            self.call_log = pd.read_csv(self.name + "/calls.log", header=None).iloc[:, 0].tolist()
        else:
            self.call_log = []

    def log_call(self):
        t = time.time()

        # Log the call
        self.call_log.append(t)
        # Filter out old logged calls
        longest_limit_type = max(self.info["limits"], key=['per_second', 'per_minute', 'per_hour', 'per_day'].index)
        filter_threshold = t - durations[longest_limit_type]
        while self.call_log and self.call_log[0] <= filter_threshold:
            self.call_log.pop(0)
        # Save the log
        pd.DataFrame(self.call_log).to_csv(self.name + "/calls.log", index=False, header=False)

    def next_available_call_time(self):
        # update next_available_call_time
        nact = time.time() # next available call time
        for limit_type, limit_value in self.info["limits"].items():
            i = 0
            for i in range(len(self.call_log)):
                if self.call_log[i] >= time.time() - durations[limit_type]:
                    break
            recent_calls = self.call_log[i:]
            diff = len(recent_calls) - limit_value
            if diff >= 0:
                # must wait at least enough time for diff entries in log to expire
                nact = max(nact, recent_calls[diff] + durations[limit_type])
        return nact

    def covered_hours(self, start, end):
        if start > end:
            start, end = end, start

        # Get all open dates between dates of start and end
        first_day = max(start.date(), self.info["date_range"]["min"])
        last_day = min(end.date(), self.info["date_range"]["max"])

        # open dates should come from a cached function from
        open_dates = filter_open_dates(first_day, last_day)

        # Add days. Assuming all the same. Could be more accurate b/c the market sometimes has special hours
        days = len(open_dates.index)
        hours = days * (self.info['hours']['max'] - self.info['hours']['min'])

        # adjust if start is after the min hours for that day
        hours -= max(start.hour - self.info['hours']['min'], 0)

        # adjust if end is before the max hours for that day
        hours -= min(self.info['hours']['max'] - end.hour, 0)

        return hours

    def _api_call(self, params):
        raise NotImplementedError("API._api_call not defined")

    @staticmethod
    def get_params(symbol, start, end):
        # splits the job into multiple jobs as a list of dicts of parameters for request
        raise NotImplementedError("API.split_job not defined")
