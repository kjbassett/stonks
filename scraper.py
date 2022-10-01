# I'm not really sure why why I used OOP either, but it works.
# TODO New Source Template
from multiprocessing import Pool
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
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