import datetime
import praw
import pandas as pd
import re
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk import word_tokenize
from useful_funcs import get_cred, load_progress
import os


"""
Effective July 1, 2023, the rate limits to use the Data AlphaVantage free of charge are 100 queries per minute per OAuth client
id if you are using OAuth authentication and ten queries per minute if you are not using OAuth authentication.
"""

client = get_cred("reddit_api", "client_id")
secret = get_cred("reddit_api", "client_secret")
username = get_cred("reddit_api", "username")
password = get_cred("reddit_api", "password")

subs = ["wallstreetbets", "stocks", "StockMarket", "investing"]
subs = sorted(subs)

buy_words = [
    "ape",
    "buy",
    "bull",
    "up",
    "increase",
    "green",
    "top",
    "safe",
    "yolo",
    "buying",
    "increasing",
]
sell_words = [
    "sell",
    "bear",
    "down",
    "decrease",
    "red",
    "pump",
    "dump",
    "selling",
    "decreasing",
    "pumping",
    "dumping",
]
negate_words = ["not", "never", "n't", "no"]


data_file = f'reddit{datetime.date.today().strftime("%Y-%m-%d")}.csv'


def post_eater():
    """
    Feed me text and I will track each ticker symbol I find as well as several attributes about that text.
    Is this the best way of doing things? Probably not but it was fun
    :return: list of dicts of text infos :)
    """
    sia = SentimentIntensityAnalyzer()

    post = None
    while True:
        data = []
        if not post:
            post = yield data
            continue
        for i, post in enumerate([post] + post.comments.list()):
            text = ""
            for attr in ["title", "body"]:
                if hasattr(post, attr):
                    text += getattr(post, attr)

            # Count buy_words and sell words
            words = [w.lower() for w in word_tokenize(text)]
            bw = 0  # counter for "buy" words
            sw = 0  # counter for "sell" words
            for j, word in enumerate(words):
                if word in buy_words:
                    if words[j - 1] in negate_words and j:
                        sw += 1
                    else:
                        bw += 1
                elif word in sell_words:
                    if words[j - 1] in negate_words and j:
                        bw += 1
                    else:
                        sw += 1

            for tck in re.findall(r"(?<=\$)\w+|[A-Z]{3,6}", text):  # magic
                data.append(
                    {
                        "ID": post.id,
                        "Ticker": tck,
                        "Sentiment": sia.polarity_scores(text)["compound"],
                        "Score": post.score,
                        "Buy Words": bw,
                        "Sell Words": sw,
                        "Timestamp": post.created,
                    }
                )
                # print(data[-1])

        post = yield data


def main(queue, save_path):
    # Load progress
    prog_path = save_path + "_" + data_file
    data = load_progress(prog_path)  # _ indicates a saved progress file

    reddit = praw.Reddit(
        client_id=client,
        client_secret=secret,
        password=password,
        user_agent="stonks",
        username=username,
    )

    pe = post_eater()
    next(pe)

    counter = 0
    for s in enumerate(subs):
        # Check if subreddit is already complete by checking if next sub has started
        if s in data["Subreddit"]:
            continue
        sub = reddit.subreddit(s)
        for post in sub.top(time_filter="day"):
            new = pd.DataFrame(pe.send(post))
            new["Subreddit"] = s
            data = pd.concat([data, new])
            counter += 1
            print(counter)
        data.to_csv(
            prog_path
        )  # Save here because we need to groupBy + aggregate before sending data to parent process

    # Todo map company name to ticker symbol.
    data = data.drop_duplicates(["ID", "Ticker"])
    data = data.groupby("Ticker").agg(
        {
            "Sentiment": "mean",
            "Score": "mean",
            "Buy Words": "mean",
            "Sell Words": "mean",
            "ID": "count",
        }
    )
    data = data.rename(
        columns={
            "Sentiment": "Avg Reddit Sentiment",
            "Score": "Avg Reddit Score",
            "Buy Words": "Avg Reddit Buy Words",
            "Sell Words": "Avg Reddit Sell Words",
            "ID": "Total Reddit Mentions",
        }
    )
    data["Source"] = "Reddit"
    data.to_csv(save_path + data_file)
    os.remove(prog_path)

    queue.put(data)
    queue.put("reddit complete")


if __name__ == "__main__":
    from timeit import default_timer as timer
    from datetime import timedelta
    from multiprocessing import Queue, Process

    start = timer()
    q = Queue()
    p = Process(
        target=main, args=(q, "C:\\Users\\Ken\\Dropbox\\Programming\\Stonks\\tmp\\")
    )
    p.start()
    msg = None
    while not isinstance(msg, str):
        msg = q.get()
        print(msg)
    stop = timer()
    print(timedelta(seconds=stop - start))  # 2:05


# TODO
#  In parent func: track if company is hasn't been mentioned recently
#  Filter out false positives for ticker symbol detection? Or leave that to the filtering step before analysis?
#  Save posts to hard drive and abstract out the encoding.
#  Convert to standard interface
