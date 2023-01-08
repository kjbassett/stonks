from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
import re
import os
import time
import numpy as np
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchWindowException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from useful_funcs import get_cred


class_tags = {
    'newConstructs': 'new-constructs-rating',
    'cfra': 'cfra-rating',
    'ford': 'ford-rating',
    'theStreet': 'stree-rating',
    'researchTeam': 'research-team-rating'
}

srcs = {
    'newConstructs': 'New Constructs',
    'researchTeam': 'Research Team',
    'theStreet': 'The Street',
    'cfra': 'CFRA',
    'ford': 'Ford'
}


def get_tickers(driver):
    """
    Gets a list of tickers from the Ameritrade
    :param driver: webdriver.Chrome(), must already be logged in
    :return: pd.Series() of ticker symbols (str)
    """
    dl_path = os.path.expanduser("~") + '\\Downloads\\export-net.xlsx'
    if os.path.exists(dl_path):
        os.remove(dl_path)
    driver.get('https://invest.ameritrade.com/grid/p/site#r=MarketCalendar?eventType=ratings')
    time.sleep(3)

    for _ in range(3):  # Due to bug on Ameritrade's side, might need to try more than once
        driver.get(
            'https://invest.ameritrade.com/grid/p/site#r=jPage/https://research.ameritrade.com/grid/wwws/screener'
            '/stocks/results.asp?section=stocks&savedName=Strong%20Buys&recentNumber=0&c_name=invest_VENDOR')
        time.sleep(2)
        driver.get('https://research.ameritrade.com/grid/wwws/screener/stocks/export.asp')
        while not os.path.exists(dl_path):
            time.sleep(0.5)
        time.sleep(3)
        file = pd.read_excel(dl_path, sheet_name='Basic View', skiprows=1)
        os.remove(dl_path)
        if len(file.index) < 2000:  # Bug mentioned above pulls in nearly 10000 tickers
            return file['Symbol'].unique().rename('Ticker')


def get_rating(driver, ticker):
    url1 = 'https://invest.ameritrade.com/grid/p/site#r=jPage/'
    url2 = f'https://research.ameritrade.com/grid/wwws/research/stocks/analystreports?symbol={ticker}'
    url = url1 + url2
    tries = 0
    data = dict()

    while tries < 3:
        try:
            tries += 1
            driver.switch_to.default_content()
            driver.get(url)
            iframe = WebDriverWait(driver, 5).until(ec.visibility_of_element_located((By.ID, "main")))
            driver.switch_to.frame(iframe)
            WebDriverWait(driver, 5, ignored_exceptions=[NoSuchWindowException]).until(
                ec.visibility_of_all_elements_located((By.CLASS_NAME, "align-left")))  # ignore WebDriverException
            break
        except (TimeoutException, WebDriverException):
            if re.findall('/etfs/', driver.current_url) or 'symbolFailure' in driver.current_url:
                print('ETF or not found')
                return data
            continue

    if re.findall('/etfs/', driver.current_url) or tries == 3 or 'symbolFailure' in driver.current_url:
        print('Ameritrade: max tries exceeded or ETF or not found')
        return data

    for tries in range(3):
        try:
            if tries > 0:
                print(tries)
            time.sleep(0.3 * tries)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            ratings = soup.find('table', attrs={'class': ['ui-table', 'provider-detail']})
            ratings = ratings.find('tbody')
            ratings = ratings.find_all('tr')
            break
        except AttributeError as e:
            print(e)
    else:
        return data

    for rating in ratings:
        name = rating.find('td').attrs['data-value']
        if name not in srcs.keys() or rating.find_all('div', attrs={'class': 'no-rating-provided'}):
            continue
        rating_box = rating.find('div', attrs={'class': class_tags[name]})
        for rec, r in enumerate(rating_box.find_all('div')):
            if r.has_attr('class'):
                break
        else:
            continue
        try:
            if name in ['newConstructs', 'cfra']:
                rec = (-rec + 2) / 2
            elif name in ['researchTeam', 'theStreet']:
                rec = -rec + 1
            elif name == 'ford':
                rec = - 0.5 * rec + 1
            if rec > 1 or rec < -1:
                print(f'Check {name} for {ticker}. {rec}')
                rec = np.nan
        except ValueError:
            rec = np.nan
        except Exception as e:
            print(type(e), e)
            rec = np.nan
        data[srcs[name]] = rec

    return data


def login(driver):
    driver.get('https://invest.ameritrade.com/grid/p/login')

    btn = WebDriverWait(driver, 10).until(ec.element_to_be_clickable((By.CLASS_NAME, "cafeLoginButton")))
    time.sleep(2)
    btn.click()

    user = WebDriverWait(driver, 10).until(ec.visibility_of_element_located((By.NAME, "su_username")))
    pw = WebDriverWait(driver, 10).until(ec.visibility_of_element_located((By.NAME, "su_password")))
    username = get_cred('ameritrade', 'username')
    password = get_cred('ameritrade', 'password')
    while user.get_attribute('value') != username or pw.get_attribute('value') != password:
        user.clear()
        pw.clear()
        user.send_keys(username)
        time.sleep(0.2)
        pw.send_keys(password)
        time.sleep(0.2)

    pw.send_keys(Keys.ENTER)

    asq_btn = WebDriverWait(driver, 10).until(ec.visibility_of_element_located((By.NAME, "init_secretquestion")))
    asq_btn.click()

    try:
        ans = WebDriverWait(driver, 5).until(ec.visibility_of_element_located((By.NAME, "su_secretquestion")))
        sq = BeautifulSoup(driver.page_source, features='lxml')
        sq = sq.find('div', attrs={'class': 'row description'}).find_all('p')[1]

        time.sleep(0.2)
        qna = {
            get_cred('ameritrade', f'security_question_{i}'):
                get_cred('ameritrade', f'security_answer_{i}')
            for i in range(4)
        }

        for q in qna.keys():
            if q in sq.text:
                ans.send_keys(qna[q])
                time.sleep(0.1)
                ans.send_keys(Keys.ENTER)
                break
        else:
            print('Secret question: ', sq)
    except TimeoutException as e:
        print(e)

    time.sleep(1)
    driver.execute_script('document.getElementById("trustthisdevice0_0").click();')
    driver.execute_script('document.getElementById("accept").click();')

    while driver.current_url != 'https://invest.ameritrade.com/grid/p/site#r=home':
        pass
    time.sleep(1)


def main(send_q, recv_q):
    driver = webdriver.Chrome()
    login(driver)

    # Todo check if tickers already retrieved
    tickers = get_tickers(driver)
    send_q.put({'Ticker': tickers, 'Source': 'Ameritrade'})

    while True:

        for i in tickers.index:
            t = tickers.pop(i)
            print(f'Ameritrade\t{t}\t{len(tickers)}')
            send_q.put({'Ticker': t, **get_rating(driver, t)})

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

# TODO
#  Handle saving in parent process