from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import re


def daily_max_all(driver):
    # All free daily stock picks
    # -1, 0, 1
    driver.get("http://www.dailymaxoptions.com/free-stock-picks/")
    soup = BeautifulSoup(driver.page_source, features='lxml')
    if isinstance(soup, type(None)):
        print('No content for dailyMaxALL!!!!')
        return dict()
    table = soup.find('div', attrs={'class': 'table-responsive'})
    if isinstance(table, type(None)):
        print('No recommendations for dailyMaxALL!!!!')
        return dict()
    rows = table.find('tbody').find_all('tr', recursive=False)[:-1]

    results = []
    for row in rows:
        tck = row.find('a').text
        rec = ['down', None, 'up'].index(re.findall('images/(.+?)-arrow', row.find('img').attrs['src'])[0]) - 1
        results.append((tck, rec))
    if not results:
        print('dailyMaxALL did not find any tickers')
    return dict(results)


def daily_max_tslt10(driver):
    # Top Stocks Less Than $10
    # -1, -0.5, 0, 0.5, 1
    driver.get("http://www.dailymaxoptions.com/top-rated-under-10-stocks/")
    soup = BeautifulSoup(driver.page_source, "html.parser")
    if isinstance(soup, type(None)):
        print('No content for dailyMaxTSLT10!!!!')
        return dict()
    table = soup.find('table', attrs={'class': 'easy-table easy-table-default'})
    if isinstance(table, type(None)):
        print('No recommendations for dailyMaxTSLT10!!!!')
        return dict()
    table = table.find('tbody')
    rows = table.find_all('tr', recursive=False)
    results = []
    for row in rows:
        tck = row.find('a').text
        rec = (int(re.findall('images/(.)', row.find('img').attrs['src'])[0]) - 3) / 2
        results.append((tck, rec))
    if not results:
        print('dailyMaxTSLT10 did not find any tickers')
    return dict(results)


def daily_max_tts(driver):
    # Top Tech Stocks
    # -1, -0.5, 0, 0.5, 1
    # headers = {'user-agent': 'my-app/0.0.1'}
    driver.get("http://www.dailymaxoptions.com/top-rated-technology-stocks/")
    soup = BeautifulSoup(driver.page_source, "html.parser")
    if isinstance(soup, type(None)):
        print('No content for dailyMaxTTS!!!!')
        return dict()
    table = soup.find('table', attrs={'class': 'easy-table easy-table-default'})
    if isinstance(table, type(None)):
        print('No recommendations for dailyMaxTTS!!!!')
        return dict()
    table = table.find('tbody')
    rows = table.find_all('tr', recursive=False)
    results = []
    for row in rows:
        tck = row.find('a').text
        rec = (int(re.findall('images/(.)', row.find('img').attrs['src'])[0]) - 3) / 2
        results.append((tck, rec))
    if not results:
        print('dailyMaxTTS did not find any tickers')
    return dict(results)


def main(send_q):
    driver = webdriver.Chrome()
    daily = daily_max_all(driver)
    weekly = {**daily_max_tslt10(driver), **daily_max_tts(driver)}
    tickers = sorted(set(list(weekly.keys()) + list(daily.keys())))
    data = pd.DataFrame({'Ticker': tickers})
    data['DMDaily'] = data.apply(lambda row: daily[row['Ticker']] if row['Ticker'] in daily.keys() else 0, axis=1)
    data['DMWeekly'] = data.apply(lambda row: weekly[row['Ticker']] if row['Ticker'] in weekly.keys() else 0, axis=1)
    data['Source'] = 'DailyMax'

    send_q.put(data)
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
    q1.close()
    q2.close()
    p.join()
