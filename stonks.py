import datetime
from multiprocessing import Pool
import pandas as pd
import os
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.model_selection import train_test_split
from scraper import Scraper
from models import ALL_MODELS
from ameritrade_api import calc_date, get_changes
# from pyschej import scheduler
import time

pd.options.display.float_format = '{:.3f}'.format


def compile_recs(max_date, days, min_date=datetime.date(1900, 1, 1), change=True):
    df = pd.DataFrame()
    for fld in os.listdir('DailyRecs'):
        try:
            date = datetime.datetime.strptime(fld, '%Y-%m-%d').date()
            if date > max_date or date < min_date:
                continue
            next_ = pd.read_csv(f'DailyRecs\\{fld}\\main{fld}.csv')

            next_['Date'] = date
            if change:
                next_ = get_changes(next_, date, days, save=True)  # changes in y variable
            print(f'Using Data from {date}')
            df = pd.concat([df, next_], ignore_index=True)
        except ValueError:
            continue

    if 'index' in df.columns:
        df.drop('index', axis=1, inplace=True)
    return df


def update_training_data(days):
    """
    Updates and returns all training data according to real world time.
    Unfiltered data is saved for faster future updates. Filtered (~y.isnull) data is returned
    :param days: The number days forward your model predicts
    :return: DataFrame of all up-to-date training data
    """
    max_date = datetime.datetime.today()  # latest possible date
    if max_date.hour < 9:
        max_date -= datetime.timedelta(days=1)
    max_date = max_date.date()
    max_date = calc_date(max_date, -days)  # Latest date of complete data (assuming ending price is collected at 9)

    if os.path.exists(f'TrainingData\\{days}.csv'):
        tdata = pd.read_csv(f'TrainingData\\{days}.csv')
        tdata['Date'] = tdata.apply(lambda row: datetime.datetime.strptime(row['Date'], '%Y-%m-%d').date(), axis=1)
        last_date = tdata['Date'].max()  # date last calculated
        if last_date >= max_date:
            return tdata
    else:
        tdata = None
        last_date = datetime.date(1900, 1, 1)

    # new for training data, not for today
    new_data = compile_recs(max_date, days, min_date=last_date)
    new_data = new_data[new_data['Date'] > last_date]
    if isinstance(tdata, pd.DataFrame):
        tdata = pd.concat([tdata, new_data], ignore_index=True)
    else:
        tdata = new_data.reset_index()
    # Even if price change isn't available on a certain date, ratings are used to find dratings for previous days.
    tdata.to_csv(f'TrainingData\\{days}.csv', index=False)
    return tdata


def calc_dRatings(df, exclude=None, keep=True):
    if not exclude:
        exclude = []

    # df contains data from current and previous day
    df2 = df.copy()

    # move dates in df forward by 1 business day
    mapping = {d: calc_date(d, 1) for d in df2['Date'].unique()}
    df2['Date'] = df2['Date'].map(mapping)

    # Date and Ticker are now indices to ensure good subtraction
    df = df.set_index(['Date', 'Ticker'])
    df2 = df2.set_index(['Date', 'Ticker'])

    # Remove duplicates in each, keeping the first
    df = df[~df.index.duplicated()]
    df2 = df2[~df2.index.duplicated()]

    # Filter to only date and ticker matches that are in both dfs
    all_indices = df.index.intersection(df2.index)
    df = df.filter(all_indices, axis=0)
    df2 = df2.filter(all_indices, axis=0)

    # Subtract df2 from df2 to change change in ratings from previous business day
    # Rows without matches will have all NaNs for dRating columns
    # Exclude any columns from subtraction if in exclude
    exclude, exclude2 = df[exclude], df2[exclude]
    df, df2 = df.drop(columns=exclude), df2.drop(columns=exclude)

    df2 = df.subtract(df2)
    df2 = df2.add_prefix('d')

    # Add columns back into df. (Join took up too much memory)
    for col in df2.columns:
        if col == 'dy':
            continue
        df[col] = df2[col]

    # Exclude only excludes from difference calculation, still keep columns from current period
    for col in exclude.columns:
        df[col] = exclude[col]
    # Optionally keep columns from last period
    if keep:
        exclude2 = exclude2.add_prefix('prev_')
        for col in exclude2.columns:
            df[col] = exclude2[col]

    # We want date and ticker as columns for later
    return df.reset_index()


def compile_sources(one_hot=False):
    # Returns dataframe of date, ticker, and the source that recommended it on that day.
    df = pd.DataFrame(columns=['Date', 'Ticker', 'Source'])
    for fol in os.listdir('DailyRecs'):
        path = 'DailyRecs' + f'/{fol}'
        for file in os.listdir(path):
            if file[:7] == 'Tickers':
                new = pd.read_csv(path + f'/{file}')
                new['Date'] = datetime.datetime.strptime(fol, '%Y-%m-%d').date()
                df = pd.concat([df, new], ignore_index=True)

    if one_hot:
        df['val'] = 1
        df = df.pivot(index=['Date', 'Ticker'], columns=['Source'], values=['val'])
        df = df.fillna(0)
        df.columns = ['Source_' + col[1] for col in df.columns]
    return df


def calc_metadata(data):
    # Potential to add more to this one?
    weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")
    for i, d in enumerate(weekDays):
        data[d] = data['Date'].apply(lambda x: x.weekday() == i)
        data[d] = data[d].astype(int)

    return data


def filter_data(historical, new, date, days):
    # Cutoff invalid days in historical data
    historical = historical[historical['Date'] <= calc_date(date, -days)]

    # Remove Outliers
    # historical = historical[np.abs(stats.zscore(historical['y'], nan_policy='omit')) < 3]

    """
    # Remove columns of data from both datasets if it's 90?% NaN in either dataset
    # This keeps too many rows from being removed in the next step
    for col in new.columns:
        if (
                new[col].isna().sum() > 0.9 * len(new.index)
        ) or (
                historical[col].isna().sum() > 0.9 * len(historical.index)
        ):
            new = new.drop(columns=[col])
            historical = historical.drop(columns=[col])
    """

    new = new.replace([np.inf, -np.inf], np.nan)
    historical.replace([np.inf, -np.inf], np.nan)

    # Remove rows from historical if y data is unavailable
    historical = historical[~historical['y'].isna()]

    # Make column to indicate "missingness" of row
    new['na_count'] = new.isnull().sum(axis=1)
    historical['na_count'] = historical.isnull().sum(axis=1)

    # Remove rows with too many nulls
    # Todo make it a % instead of a flat value
    new = new[new['na_count'] < 5]
    historical = historical[historical['na_count'] < 5]

    return historical, new


def create_masks(date, data):
    """
    Creates multiple subsets of data to apply predictions methods to
    :param date: Date of prediction
    :param data: Data to apply masks to
    :return: dict, {'subset_name': mask, ...}
    """
    masks = dict()

    weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    other_cols = ['y', 'Date', 'Ticker', 'na_count', 'prev_na_count'] + weekdays
    src_cols = []
    d_cols = []
    rec_cols = []
    for col in data.columns:

        # d prefix means the col is the change in recommendatiaon from the previous date that the stock market was open
        if col[0] == 'd':
            d_cols.append(col)
        # for columns that start with "Source", 1 if ticker came from source, 0 otherwise
        elif col[:6] == 'Source':
            src_cols.append(col)
        # Actual recommended values, between -1 (sell) and 1 (buy)
        elif col not in other_cols:
            rec_cols.append(col)

    masks['Positive Sum'] = data[rec_cols].sum(axis=1) > 0
    masks['Positive Delta'] = data[d_cols].sum(axis=1) > 0
    masks['Same Weekday'] = data['Date'].apply(lambda row: row.weekday()) == date.weekday()
    masks['All Data'] = pd.Series([True] * len(data.index))
    for col in src_cols:
        masks[col] = data[col] > 0

    return masks


def predict(date, days, train, test, new):
    date_str = date.strftime("%Y-%m-%d")
    results_path = f'Results/{days}/{date_str}'
    if not os.path.exists(results_path):
        os.mkdir(results_path)

    # Each mask represents a subset of the data. Each combo of subset and model will be scored will be scored with each model.
    train_masks = create_masks(date, train)
    test_masks = create_masks(date, test)
    new_masks = create_masks(date, new)

    model_results = []
    with Pool(processes=4) as pool:
        for subset in train_masks.keys():
            for mname, model in ALL_MODELS.items():
                _train, _test, _new = train[train_masks[subset]], test[test_masks[subset]], new[new_masks[subset]]
                if len(_test.index) < 10 or len(_new.index) == 0:
                    print(f'Not enough data for subset {subset} and model {mname}')
                    model_results.append({'subset': subset,
                                          'model name': mname,
                                          'results': 'Not enough data'
                                          })
                else:
                    print(f'Starting prediction for subset {subset} and model {mname}')
                    model_results.append({'subset': subset,
                                          'model name': mname,
                                          'results': pool.apply_async(model, (_train, _test, _new))
                                          })
        results = pd.DataFrame(columns=['Ticker', 'subset', 'model name', 'prediction'])
        performance = []
        for m in model_results:
            res = m['results']
            if not isinstance(res, str):
                m['results'] = res.get()
                performance.append({'subset': m['subset'],
                                    'model name': m['model name'],
                                    'RMSE': m['results'][1],  # RMSE of predictions of test data
                                    'R^2': m['results'][2],  # R^2 of predictions of test data
                                    'Variance': m['results'][3],  # Variance of test data
                                    'n_train': m['results'][4]  # Rows of training data
                                    })
                m['results'][0]['subset'] = m['subset']
                m['results'][0]['model name'] = m['model name']
                results = pd.concat([results, m['results'][0][['Ticker', 'subset', 'model name', 'prediction']]],
                                    ignore_index=True)

    performance = pd.DataFrame(performance)

    results.sort_values(by='prediction', ascending=False).to_csv(f"{results_path}/results{date_str}.csv", index=False)
    performance.sort_values(by='R^2', ascending=False).to_csv(f"{results_path}/models{date_str}.csv", index=False)

    summary = results.merge(performance.drop('n_train', axis=1), on=['subset', 'model name'])
    # Todo weights by RMSE or R^2?
    summary['weight'] = -summary['RMSE'] + summary['RMSE'].min() + summary['RMSE'].max()
    summary['adj pred'] = summary['weight'] * summary['prediction']

    summary = summary.groupby('Ticker').agg({'adj pred': 'sum', 'weight': 'sum'})
    summary['adj pred'] = summary['adj pred'] / summary['weight']

    new['Sum'] = new[
        ['DMDaily', 'DMWeekly', 'Zacks', 'Robinhood', 'New Constructs', 'Research Team', 'The Street', 'CFRA',
         'Ford']].fillna(0).sum(axis=1)
    new['Change'] = new[
        ['dZacks', 'dRobinhood', 'dNew Constructs', 'dResearch Team', 'dThe Street', 'dCFRA', 'dFord']] \
        .fillna(0).sum(axis=1)
    summary = summary.merge(new[['Ticker', 'Sum', 'Change']], on=['Ticker'], how='outer')

    summary.sort_values(by='adj pred', inplace=True, ascending=False)
    summary.to_csv(f"{results_path}/summary{date_str}.csv", index=False)

    return summary


def main(date=datetime.date.today(),
         days=5,
         include=None):
    if not include:
        include = []

    scraper = Scraper(date, include=include, mimicUser=True)
    scraper.run()

    print(f'Compiling Historical data')
    train = update_training_data(days)
    print(f"Compiling data for {date}")
    new = compile_recs(date, days, calc_date(date, -1), change=False)

    # Filter & Format
    train, new = filter_data(train, new, date, days)
    train['y'] = train['y'] * 100

    # Split train into train and test
    train, test = train_test_split(train)

    # Imputation
    imp_col = [c for c in new.columns if c not in ['na_count', 'Date', 'Ticker']]  # columns to impute
    imputer = KNNImputer(n_neighbors=20)
    imputer.fit(train[imp_col])
    t = time.perf_counter()
    print('Imputing')
    train[imp_col] = imputer.transform(train[imp_col])
    test[imp_col] = imputer.transform(test[imp_col])
    new[imp_col] = imputer.transform(new[imp_col])
    print(f'Imputation took {time.perf_counter() - t} seconds.')

    # Get change in ratings
    new = calc_dRatings(new, exclude=['na_count'])
    test = calc_dRatings(test, exclude=['na_count'])
    train = calc_dRatings(train, exclude=['na_count'])

    if new is None:
        print("Today's data is not found or the stock market is closed")
        print(f'Previous Day: {calc_date(date, -1)}')
        return

    if new.empty:
        print('No data after calculating change in ratings.')
        print(f'Previous Day: {calc_date(date, -1)}')
        print(f'Date: {date}')
        return

    if train.empty or test.empty:
        print("You haven't gotten around to coding this part you buffoon.")
        print('See notes on when to calc dratings at bottom of file.')
        return

    # Add Source Columns (Source of Recommendation = Ameritrade, Robinhood, User ...
    sources = compile_sources(one_hot=True)
    train = train.merge(sources, on=['Date', 'Ticker'], how='inner')
    test = test.merge(sources, on=['Date', 'Ticker'], how='inner')
    new = new.merge(sources, on=['Date', 'Ticker'], how='inner')

    train, test, new = calc_metadata(train), calc_metadata(test), calc_metadata(new)

    results = predict(date, days, train, test, new)
    print(results)


if __name__ == '__main__':
    print(ALL_MODELS)

    # Args
    prediction_range = 5  # How many days in the future we are predicting
    start_time = datetime.datetime(2022, 5, 4, 9, 5)  # year month day hour minute
    mimicUser = True  # Move mouse occasionally
    tcks = pd.read_csv('tickers.csv')['Ticker'].unique().tolist()
    # restart_freq

    # scheduler.add_to_schedule(main, start_time, start_time.date(), prediction_range, tcks, timeout=1800)
    # scheduler.start_scheduler()

    print(main(start_time.date(), prediction_range, tcks))
    # compile_sources().groupby('Source').agg('count').sort_values('Source', ascending=False).to_csv('Deez2.csv')

    # fix_data(days)

# taskkill /im chromedriver.exe /f