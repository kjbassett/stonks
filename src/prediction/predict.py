import pandas as pd

from ezmt.organism import Organism
from src.utils.email import send_email
from webrock.decorator import plugin


@plugin()
async def predict_latest_data(
    name: str,
    version: str = "latest",
    log_states: bool = False,
    send_results: bool = False,
    recipients: str = None,
):
    model = Organism.load(name, version)
    predictions = await model.run(log_states=log_states, result_name="recommendations")
    if send_results:
        predictions = predictions[
            predictions["prediction"] / predictions["uncertainty"] > 2.5
        ]
        if not recipients:
            raise ValueError(
                "if send_recipients it truthy, recipients must have a value"
            )
        send_email("Stock Recommendations", prepare_message(predictions), recipients)


def prepare_message(df):
    # convert timestamp in seconds to readable string
    dt_str_col = pd.to_datetime(df["timestamp"], unit="s").dt.strftime(
        ", %Y-%m-%d %H:%M"
    )
    # add datetime string to symbol
    readable_data = df["symbol"].str.cat(dt_str_col)
    # concat all to 1 string, separated by new line characters
    return readable_data.str.cat(sep="\n")
