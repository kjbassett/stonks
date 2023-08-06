import logging
import os
import pandas as pd
import time
from useful_funcs import get_api_key


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
            print(params)
            all_data = pd.DataFrame(
                columns=["open", "high", "low", "close", "volume", "timestamp"]
            )
            for param in params:
                print(param)
                # determine if api needs to wait
                wait = self.next_available_time() - time.time()
                print(wait)
                if wait < 10:
                    time.sleep(max(wait, 0))
                else:
                    break  # return data if waiting too long

                try:
                    self.log_call()
                    data = self._api_call(param)
                    all_data = pd.concat([all_data, data])
                except Exception as e:
                    print(f"ERROR from {self.name} on _api_call")
                    print(e)
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
            print(e)
            self.error_logger.error(
                f"Exception occurred for {self.name}.api_call on symbol {symbol} with start {start} and end {end}",
                exc_info=True,
            )



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
            print(recent_calls)
            if recent_calls.size >= limit_value:
                latest = max(latest, recent_calls.min() + durations[limit_type])

        return latest

    def _api_call(self, params):
        raise NotImplementedError("API._api_call not defined")

    @staticmethod
    def get_params(symbol, start, end) -> [dict]:
        # splits the job into multiple jobs as a list of dicts of parameters for request
        raise NotImplementedError("API.split_job not defined")

