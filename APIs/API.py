import logging
import os
import pandas as pd
import time


durations = {"per_second": 1, "per_minute": 60, "per_hour": 3600, "per_day": 86400}


class API:
    def __init__(self, name, info):
        self.name = name
        self.info = info
        self.api_key = self.get_api_key()
        self.error_logger = None
        self.call_log = None
        self.create_error_logger()
        self.load_call_log()

    def api_call(self, symbol, start, end):
        try:
            ranges = self.split_job(start, end)
            all_data = []
            for s, e in ranges:
                data = self._api_call(symbol, s, e)
                all_data.append(data)
            all_data = pd.concat(all_data)
            return all_data
        except Exception as e:
            print(f"ERROR from {self.name} on api call")
            print(e)
            self.error_logger.error(
                f"Exception occurred for {self.name} API on symbol {symbol} with start {start} and end {end}",
                exc_info=True,
            )

    def get_api_key(self):
        with open("../keys.txt", "r") as f:
            for line in f:
                name, key = line.strip().split("=")
                if name == self.name:
                    return key
        raise ValueError(f"No key found for API: {self.name}")

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
            self.call_log = pd.read_csv(self.name + "/call_log.csv", header=None)
            self.call_log = self.call_log[self.call_log.columns[0]]
        else:
            self.call_log = pd.Series()

    def log_call(self):
        # Log the call
        t = time.time()
        self.call_log.at[len(self.call_log)] = t

        # Filter the log
        longest_limit_type = max(self.info["limits"], key=self.info["limits"].get)
        self.call_log = self.call_log[
            self.call_log >= t - durations[longest_limit_type]
        ]

        # Save the log
        self.call_log.to_csv(self.name + "/call_log.csv")

    def next_available_time(self):
        # update next_available_time
        latest = 0
        for limit_type, limit_value in self.info["limits"].items():
            recent_calls = self.call_log[
                self.call_log > time.time() - durations[limit_type]
            ]
            if recent_calls.size >= limit_value:
                latest = max(latest, recent_calls.min() + durations[limit_type])

        return latest

    def _api_call(self, symbol, start, end):
        raise NotImplementedError("API._api_call not defined")

    @staticmethod
    def split_job(start, end):
        # default is to return start and end in an iterable
        return [(start, end)]
