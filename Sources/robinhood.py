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

def rh_tickers(self, driver):
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

    tickers = list(set(tickers))
    tickers = pd.DataFrame({'Ticker': tickers, 'Source': ['Robinhood'] * len(tickers)})
    self.save_progress(tickers, 'Tickers')

    tickers['Robinhood'] = np.nan

    return tickers.drop(columns=['Source'])


def _robinhood(row, driver):
    # [-1, 1]
    driver.get("https://robinhood.com/stocks/" + row['Ticker'])
    if driver.title == 'Page not found | Robinhood':
        row['Robinhood'] = 0
    try:
        section = WebDriverWait(driver, 2.5).until(ec.visibility_of_element_located((By.ID, "analyst-ratings")))
        chart = WebDriverWait(section, 3.5).until(ec.visibility_of_element_located((By.CLASS_NAME, "row")))
        buy, _, sell = [float(n) for n in re.findall('left: ([0-9]{1,2}.*?)%', chart.get_attribute('innerHTML'))]
        result = (buy - sell) / 100
        row['Robinhood'] = round(result, 1)
    except TimeoutException:
        row['Robinhood'] = np.nan
    except ValueError as e:
        print(e)
        return _robinhood(row, driver)
    except Exception as e:
        print(e)
        row['Robinhood'] = np.nan

    return row


def rh_login(driver):
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