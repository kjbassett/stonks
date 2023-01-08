from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import re
import time
import numpy as np
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from useful_funcs import get_cred


def get_tickers(driver):
    while 'Trending Lists' not in driver.page_source:
        time.sleep(1)
    html = BeautifulSoup(driver.page_source, features='lxml')
    rh_lists = [li['href'] for li in html.find_all('a') if 'lists' in li['href'] and any(
        [t in li.text for t in ['100 Most Popular', 'Technology']])]
    tickers = []
    for link in rh_lists:
        for _try in range(3):
            try:
                driver.get('https://robinhood.com' + link)
                print('HERE')
                while len(driver.find_elements_by_tag_name('a')) < 30:
                    time.sleep(1)
                    btns = driver.find_elements_by_tag_name('button')
                    for p, btn in enumerate(btns):
                        if btn.text == 'Show More':
                            btns[p].click()
                for i in range(1, 11):
                    driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{i}/10);")
                    html = BeautifulSoup(driver.page_source, features='lxml')
                    rows = html.find_all('a', attrs={'role': 'row'})
                    for row in rows:
                        tickers.append(row.find_all('div', attrs={'role': 'cell'})[1].text)
                break
            except TimeoutException:
                continue

    return pd.Series(tickers).unique()


def get_rating(driver, ticker):
    # [-1, 1]
    driver.get("https://robinhood.com/stocks/" + ticker)
    if driver.title == 'Page not found | Robinhood':
        return 0
    try:
        section = WebDriverWait(driver, 2.5).until(ec.visibility_of_element_located((By.ID, "analyst-ratings")))
        chart = WebDriverWait(section, 3.5).until(ec.visibility_of_element_located((By.CLASS_NAME, "row")))
        buy, _, sell = [float(n) for n in re.findall('left: ([0-9]{1,2}.*?)%', chart.get_attribute('innerHTML'))]
        result = (buy - sell) / 100
        return round(result, 1)
    except TimeoutException:
        return np.nan
    except ValueError as e:
        print(e)
        return get_rating(ticker, driver)
    except Exception as e:
        print(e)
        return np.nan


def login(driver):
    driver.get('https://robinhood.com/login')
    user_field = WebDriverWait(driver, 10).until(ec.visibility_of_element_located((By.NAME, "username")))
    username = get_cred('robinhood', 'username')
    password = get_cred('robinhood', 'password')
    user_field.send_keys(username)
    for e in driver.find_elements_by_tag_name('input'):
        if e.get_attribute('type') == 'password':
            e.send_keys(password)
            break
    for e in driver.find_elements_by_tag_name('button'):
        if e.get_attribute('type') == 'submit':
            e.click()
            break


def main(send_q, recv_q):
    driver = webdriver.Chrome()
    login(driver)

    # Todo check if tickers already retrieved
    tickers = get_tickers(driver)
    send_q.put({'Ticker': tickers, 'Source': 'Robinhood'})

    while True:

        for i in tickers.index:
            t = tickers.pop(i)
            print(f'Robinhood\t{t}\t{len(tickers)}')
            send_q.put({'Ticker': t, 'Robinhood': get_rating(driver, t)})

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