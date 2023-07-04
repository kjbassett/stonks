import datetime
from functools import reduce
import pandas as pd
import os
import time
import importlib
from models import ALL_MODELS
from ameritrade_api import calc_date
from multiprocessing import Pool, Queue
from train_models import main as train_models
from train_models import compile_sources
# from pyschej import scheduler

pd.options.display.float_format = '{:.3f}'.format


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


def calc_metadata(data):
    # Potential to add more to this one?
    weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")
    for i, d in enumerate(weekDays):
        data[d] = data['Date'].apply(lambda x: x.weekday() == i)
        data[d] = data[d].astype(int)

    return data


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


def old_main(date=datetime.date.today(),
         days=5,
         include=None):
    if not include:
        include = []

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


def format_data(new: [dict, pd.DataFrame]):
    if isinstance(new, dict):
        new = pd.DataFrame(new)

    if 'Source' in new.columns:
        for s in new['Source'].unique():
            new['SRC_' + s] = 0
            new['SRC_' + s] = new.where(new['Source'] == s, 1)
        new = new.drop(columns=['Source'])

    new = new.set_index('Symbol')
    return new


def check_completion(folder):
    if os.path.exists(folder + 'data.csv'):
        return True
    return False


def is_complete(pool):
    if all(p.is_alive() for p in pool):
        return True
    return False


def start_sources():
    sources = dict()
    pool = Pool(7)
    recv_q = Queue()
    send_qs = []
    for file in os.listdir('Sources'):
        if file.endswith('.py'):
            sources[file] = {
                'main': importlib.import_module('Sources.' + file[:-3]).main,
                'recv_q': Queue(),
                'last_t': time.time()
            }

            pool.apply_async(sources[file].main, args=(recv_q, send_qs[-1]))
    return pool, recv_q, send_qs


def fancy_update(data, new):
    # Todo data needs new columns from new
    data = data.merge(new[[c for c in new.columns if c not in data.columns]])

    data = data.set_index(data.index.join(new.index, how='outer'))
    data.update(new)

    return data


def get_new_data(recv_q):
    new = None
    while not recv_q.empty():
        if new is None:
            new = format_data(recv_q.get())
        else:
            new = fancy_update(new, format_data(recv_q.get()))
    return new


def predict_incoming(pool, recv_q, send_qs, models):
    puq = {}
    i = 0
    data = pd.DataFrame(index={'Symbol': []})
    while not is_complete(pool):
        new = get_new_data(recv_q)
        data = fancy_update(data, new)

        # prediction update queue. Don't need to update prediction on every iteration
        puq.update(new['Symbol'])

        for queue in send_qs:
            queue.put(new[~new['Source'].isna()]['Symbol'])

        if i % 10 == 0:
            new_ys = predict(data.loc[list(puq)].drop(columns='y'), models)
            data.update(new_ys)
            save_progress(data)
            puq = {}

        i += 1
    pool.close()
    pool.join()
    data.update(predict(data.drop(columns='y'), models))
    save_progress(data)


def save_progress(folder, data):
    # Undo one-hot encoding for sources
    srcs = [col for col in data.columns if col.startswith('SRC_')]

    d1 = data.drop(srcs).remove_duplicates()
    d1.to_csv(folder + 'data.csv')

    # Good luck with this one. I wanted to see if I could put it in one line.
    d2 = reduce(lambda df1, df2: df1.merge(df2), map([data[data[s] == 1][[s]].replace(1, s[4:]) for s in srcs]))
    d2.to_csv(folder + 'sources.csv')


def clean_temp(folder):
    for f in os.listdir(folder):
        if f not in ['data.csv', 'sources.csv']:
            os.remove(folder + f)


def main(prediction_range, date=datetime.date.today(), user_symbols=None):
    d = date.strftime('%Y-%m-%d')
    f = f"DailyRecs\\{d}\\"
    if not os.path.exists(f):
        os.mkdir(f)

    check_completion(f)

    if user_symbols is None:
        user_symbols = []

    pool, recv_q, send_qs = start_sources()
    for q in send_qs:
        q.put(user_symbols)

    # load model
    models = load_model(date, prediction_range)  # Contains imputer

    # train on latest complete data?

    # start updating predictions as new data comes in
    data = predict_incoming(pool, recv_q, send_qs, models)

    clean_temp()

    train_new_model(date, prediction_range, )


if __name__ == '__main__':
    print(ALL_MODELS)

    # Args
    prediction_range = 5  # How many days in the future we are predicting
    start_time = datetime.datetime(2022, 5, 4, 9, 5)  # year month day hour minute
    tcks = pd.read_csv('tickers.csv')['Ticker'].unique().tolist()
    # restart_freq

    # scheduler.add_to_schedule(main, start_time, start_time.date(), prediction_range, tcks, timeout=1800)
    # scheduler.start_scheduler()

    print(main(start_time.date(), prediction_range, tcks))
    # compile_sources().groupby('Source').agg('count').sort_values('Source', ascending=False).to_csv('Deez2.csv')

    # fix_data(days)

# taskkill /im chromedriver.exe /f
