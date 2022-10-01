from bs4 import BeautifulSoup
import numpy as np


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
