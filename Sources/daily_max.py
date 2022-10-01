from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import re


def dm(self):
    data = self.load_progress('DailyMax')
    if all([col in data.columns for col in ['Ticker', 'DMDaily', 'DMWeekly']]):
        if (data['DMDaily'] != 0).any() and (data['DMWeekly'] != 0).any():
            return data
    driver = webdriver.Chrome(executable_path=self.driver_path)
    daily = self.dailyMaxALL(driver)
    weekly = {**self.dailyMaxTSLT10(driver), **self.dailyMaxTTS(driver)}
    tickers = sorted(set(list(weekly.keys()) + list(daily.keys())))
    data = pd.DataFrame({'Ticker': tickers})
    data['DMDaily'] = data.apply(lambda row: daily[row['Ticker']] if row['Ticker'] in daily.keys() else 0, axis=1)
    data['DMWeekly'] = data.apply(lambda row: daily[row['Ticker']] if row['Ticker'] in daily.keys() else 0,
                                  axis=1)

    self.save_progress(data, 'DailyMax')
    self.save_progress(pd.DataFrame({'Ticker': tickers, 'Source': 'DailyMax'}), 'Tickers')
    driver.quit()
    return data


def dailyMaxALL(driver):
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


def dailyMaxTSLT10(driver):
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


def dailyMaxTTS(driver):
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
