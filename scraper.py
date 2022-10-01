# I'm not really sure why why I used OOP either, but it works.
# TODO Split into separate files
    # Search in a folder, each src is its own folder within. Needs a successful run first if needs column names
    # New Source Template

import re
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchWindowException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from useful_funcs import get_cred
import pandas as pd
import time
import os
import numpy as np
import datetime
import glob


class Scraper:

    def __init__(self, date, include=None, mimicUser=False):
        if include is None:
            include = []
        self.include = include

        self.date = date
        d = date.strftime('%Y-%m-%d')
        f = f"DailyRecs\\{d}\\"
        if not os.path.exists(f):
            os.mkdir(f)

        self.sources = {
            'Zacks': ['Zacks'],
            'Robinhood': ['Robinhood'],
            'Ameritrade': ['New Constructs', 'Research Team', 'The Street', 'CFRA', 'Ford'],
            'DailyMax': ['DMDaily', 'DMWeekly']
        }

        self.columns = ['Ticker']
        for grp in self.sources.values():
            for col in grp:
                self.columns.append(col)

        self.filepaths = {
            'Main': f"{f}main{d}.csv",
            'Tickers': f"{f}Tickers{d}.csv",
            **{k: f"{f}{k}{d}.csv" for k in self.sources.keys()}
        }

        self.all_tickers = self.load_progress('Tickers')

        self.class_tags = {'newConstructs': 'new-constructs-rating',
                           'cfra': 'cfra-rating',
                           'ford': 'ford-rating',
                           'theStreet': 'stree-rating',
                           'researchTeam': 'research-team-rating'}


        self.driver_path = r'C:\Users\Ken\Dropbox\Programming\chromedriver.exe'

        self.mimicUser = mimicUser

    def get_ratings(self, y, rating_func=None, login_func=None, ticker_func=None, extension_path=None):
        caps = DesiredCapabilities().CHROME
        #uBlock path
        chrome_options = Options()
        if extension_path:
            version = os.listdir(extension_path)[0]
            chrome_options.add_argument('load-extension=' + extension_path + version)

        if y == 'Ameritrade':
            rating_func = self._ameritrade
            login_func = self.amer_login
            ticker_func = self.amer_tickers
        elif y == 'Robinhood':
            rating_func = self._robinhood
            login_func = self.rh_login
            ticker_func = self.rh_tickers
        elif y == 'Zacks':
            rating_func = self._zacks
            #caps["pageLoadStrategy"] = "eager"
        elif y == 'DailyMax':
            return self.dm()
        else:
            raise Exception('Source Not implemented!')

        data = self.load_progress(y)

        driver = webdriver.Chrome(desired_capabilities=caps,
                                  executable_path=self.driver_path,
                                  chrome_options=chrome_options)
        driver.create_options()
        if login_func:
            login_func(driver)

        while True:
            self.all_tickers = self.load_progress('Tickers')
            if ticker_func and y not in self.all_tickers['Source'].to_list():
                data = ticker_func(driver)
                self.save_progress(data, y, finished=False)

            if not self.all_tickers.empty:
                data = data.merge(self.all_tickers.drop(columns=['Source']), on='Ticker', how='outer', sort=False)
            if not data.empty:
                data['all_null'] = data.drop(columns=['Ticker']).isnull().all(axis=1)
                data = data.sort_values(['all_null', 'Ticker'], ignore_index=True)
                start = np.argmax(~data['all_null'] * data.index) + 1
                data = data.drop('all_null', axis=1)
                for i in range(start, len(data.index)):
                    data.iloc[i] = rating_func(data.iloc[i], driver)
                    self.save_progress(data, y, finished=False)

            if all([src in self.all_tickers['Source'].unique().tolist() for src in
                    ['Robinhood', 'Ameritrade', 'DailyMax']]):
                self.save_progress(data, y, finished=True)
                break
            time.sleep(1)

        driver.quit()
        return data

    def amer_tickers(self, driver):
        dl_path = os.path.expanduser("~") + '\\Downloads\\export-net.xlsx'
        if os.path.exists(dl_path):
            os.remove(dl_path)
        tickers = []
        driver.get('https://invest.ameritrade.com/grid/p/site#r=MarketCalendar?eventType=ratings')
        time.sleep(3)

        for _ in range(3):
            driver.get(
                'https://invest.ameritrade.com/grid/p/site#r=jPage/https://research.ameritrade.com/grid/wwws/screener'
                '/stocks/results.asp?section=stocks&savedName=Strong%20Buys&recentNumber=0&c_name=invest_VENDOR')
            time.sleep(2)
            driver.get('https://research.ameritrade.com/grid/wwws/screener/stocks/export.asp')
            while not os.path.exists(dl_path):
                time.sleep(0.5)
            time.sleep(3)
            file = pd.read_excel(dl_path, sheet_name='Basic View', skiprows=1)
            # os.remove(dl_path)
            if len(file.index) < 2000: # Sometimes there is a bug that pulls in nearly 10000 tickers
                tickers = pd.DataFrame({'Ticker': file['Symbol'].unique(), 'Source': 'Ameritrade'})
                tickers = tickers[tickers['Ticker'].str.len() < 5]
                tickers = tickers.head(min(200, len(tickers.index)))
                self.save_progress(tickers, 'Tickers')
                break

        # Add in empty columns
        for col in ['New Constructs', 'Research Team', 'The Street', 'CFRA', 'Ford']:
            tickers[col] = np.nan
        return tickers.drop(columns=['Source'])

    def _ameritrade(self, row, driver):
        print(row['Ticker'])
        url1 = 'https://invest.ameritrade.com/grid/p/site#r=jPage/'
        url2 = f'https://research.ameritrade.com/grid/wwws/research/stocks/analystreports?symbol={row["Ticker"]}'
        url = url1 + url2

        srcs = {'newConstructs': 'New Constructs', 'researchTeam': 'Research Team', 'theStreet': 'The Street',
                'cfra': 'CFRA', 'ford': 'Ford'}
        tries = 0

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
                    return row
                continue

        if re.findall('/etfs/', driver.current_url) or tries == 3 or 'symbolFailure' in driver.current_url:
            print('Ameritrade: max tries exceeded or ETF or not found')
            return row

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
            return row

        for rating in ratings:
            name = rating.find('td').attrs['data-value']
            if name not in srcs.keys() or rating.find_all('div', attrs={'class': 'no-rating-provided'}):
                continue
            rating_box = rating.find('div', attrs={'class': self.class_tags[name]})
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
                    print(f'Check {name} for {row["Ticker"]}. {rec}')
                    rec = np.nan
            except ValueError:
                rec = np.nan
            except Exception as e:
                print(type(e), e)
                rec = np.nan
            row[srcs[name]] = rec

        return row

    @staticmethod
    def amer_login(driver):
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

    @staticmethod
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
            return Scraper._robinhood(row, driver)
        except Exception as e:
            print(e)
            row['Robinhood'] = np.nan

        return row

    @staticmethod
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

    @staticmethod
    def _zacks(row, driver):
        # -1, -0.5, 0, 0.5, 1
        driver.get("http://www.zacks.com/stock/quote/" + row['Ticker'])
        soup = BeautifulSoup(driver.page_source, "html.parser")
        try:
            rec = int(soup.find('p', attrs={'class': 'rank_view'}).text.strip()[0])
            rec = ([0, 5, 4, 3, 2, 1].index(rec) - 3) / 2
        except IndexError:
            rec = np.nan
        except AttributeError:
            rec = np.nan

        row['Zacks'] = rec
        return row

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

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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

    def run(self):
        # Main is only made at the very end after all files are merged. Read in finished file
        if os.path.exists(self.filepaths['Main']):
            results = pd.read_csv(self.filepaths['Main'])

        # Only collect if earlier than noon since data can change
        elif datetime.datetime.now() < datetime.datetime.combine(self.date, datetime.time(12)):
            self.all_tickers = self.load_progress('Tickers')
            results = pd.DataFrame({'Ticker': self.all_tickers['Ticker'].unique()})
            from multiprocessing import freeze_support
            freeze_support()
            pool = Pool(processes=4)
            # 4 processes start webdrivers, gather tickers, and collect information
            ps = []
            for src in self.sources.keys():
                if not os.path.exists(self.filepaths[src]):
                    ps.append(pool.apply_async(self.get_ratings, args=[src]))
                else:
                    results = results.merge(self.load_progress(src), on=['Ticker'], how='outer')
                    print(results)

            pool.close()
            pool.join()
            for p in ps:
                p = p.get()
                print(p)
                results = results.merge(p, on=['Ticker'], how='outer')

        # If past noon, just merge what you have from each source
        else:
            results = pd.DataFrame(columns=['Ticker'])
            for src in self.sources.keys():
                df = self.load_progress(src)
                results = results.merge(df, on='Ticker', how='outer')

        # Create final file for the day and progress files
        self.save_progress(results, 'Main')
        self.clean()
        return results

    def load_progress(self, source):
        filepath = self.filepaths[source]

        if source == 'Tickers':
            if os.path.exists(filepath):
                return pd.read_csv(filepath).drop_duplicates(subset='Ticker')
            elif self.date == datetime.date.today():
                return pd.DataFrame({'Ticker': self.include, 'Source': 'User'})
            else:
                return pd.DataFrame({'Ticker': [], 'Source': []})

        # Check for completed file
        if os.path.exists(filepath):
            return pd.read_csv(filepath)

        fld, fle = os.path.split(filepath)
        filepath = os.path.join(fld, '_' + fle)

        # Check for incomplete file
        if os.path.exists(filepath):
            return pd.read_csv(filepath)
        else:
            return pd.DataFrame({'Ticker': [], **{col: [] for col in self.sources[source]}}).astype({'Ticker': str})

    def save_progress(self, data, source, finished=True):
        filepath = self.filepaths[source]

        if source == 'Tickers':
            if os.path.exists(filepath):
                data = data.merge(pd.read_csv(filepath), on=['Ticker', 'Source'], how='outer')

        else:
            fld, fle = os.path.split(filepath)
            unf_path = os.path.join(fld, '_' + fle)  # Unfinished files have a _ at the beginning of the title
            if finished:
                if os.path.exists(unf_path):
                    os.remove(unf_path)
            else:
                filepath = unf_path

        data.to_csv(filepath, index=False)

    def clean(self):
        fld = os.path.split(self.filepaths['Main'])[0]
        for file in os.listdir(fld):
            if not (file in self.filepaths['Main'] or file in self.filepaths['Tickers']):
                os.remove(os.path.join(fld, file))