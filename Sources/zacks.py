from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from selenium import webdriver


def get_rating(driver, ticker):
    # -1, -0.5, 0, 0.5, 1
    driver.get("http://www.zacks.com/stock/quote/" + ticker)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    try:
        rec = int(soup.find('p', attrs={'class': 'rank_view'}).text.strip()[0])
        rec = ([0, 5, 4, 3, 2, 1].index(rec) - 3) / 2
    except IndexError:
        rec = np.nan
    except AttributeError:
        rec = np.nan

    return rec


def main(send_q, recv_q):
    driver = webdriver.Chrome()
    tickers = pd.Series()
    while True:

        for i in tickers.index:
            t = tickers.pop(i)
            print(f'Zacks\t{t}\t{len(tickers)}')
            send_q.put({'Ticker': t, 'Zacks': get_rating(driver, t)})

        if not recv_q.empty():
            r = recv_q.get()
            if isinstance(r, pd.Series):  # More tickers
                tickers = pd.concat([tickers, r])
            elif isinstance(r, str):
                if r == 'STOP':
                    break

    driver.quit()


if __name__ == '__main__':
    from timeit import default_timer as timer
    from datetime import timedelta
    from multiprocessing import Queue, Process

    start = timer()
    q1 = Queue()
    q2 = Queue()
    p = Process(target=main, args=(q1, q2))
    p.start()
    msg = None
    while not isinstance(msg, str):
        msg = q1.get()
        print(msg)
    stop = timer()
    print(timedelta(seconds=stop - start))
