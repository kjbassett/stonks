import numpy as np
import statsmodels.api as sm

from sklearn.ensemble import RandomForestRegressor

"""
All functions that don't start with _ are prediction functions.
They should return X_new with a new prediction column, rmse, R^2 and len(X_train.index).
They expect train, test, and new as arguments.
"""


def separate_ys(train, test):
    X_train, y_train = train.drop(columns='y'), train['y']
    X_test, y_test = test.drop(columns='y'), test['y']
    return X_train, X_test, y_train, y_test


def _rmse(yh, ya):  # y-hat, y-actaul
    return np.sqrt(((ya - yh)**2).mean())


def _r2(yh, ya):
    print("========")
    SSR = np.sum((ya - yh)**2)  # Sum of squared residuals
    SST = np.sum((ya - np.mean(ya))**2)  # Sum of Squares Total
    print(SSR)
    print(SST)
    print(1 - SSR/SST)
    return 1 - SSR/SST


def _avg_nearest(pt, X_train, y_train):
    pt = pt.drop(['Ticker', 'Date'])
    distances = np.linalg.norm((pt.fillna(0) - X_train.fillna(0)), axis=1)
    n_pts = int(len(X_train.index)**0.5)
    min_ind = np.argpartition(distances, n_pts)[:n_pts]
    weights = np.reciprocal(distances + 1)
    C = weights[min_ind].sum()
    return np.dot(y_train.iloc[min_ind], weights[min_ind]) / C


def avg_nearest(train, test, new):
    X_train, X_test, y_train, y_test = separate_ys(train, test)
    X_train = X_train.drop(columns=['Ticker', 'Date'])
    test_pred = X_test.apply(lambda row: _avg_nearest(row, X_train, y_train), axis=1)
    rmse = _rmse(test_pred, y_test)
    r2 = _r2(test_pred, y_test)
    new['prediction'] = new.apply(lambda row: _avg_nearest(row, X_train, y_train), axis=1)
    return new, rmse, r2, np.var(y_test), len(X_train.index)


def avg(train, test, new):
    X_train, X_test, y_train, y_test = separate_ys(train, test)

    m = y_train.mean()
    new['prediction'] = m
    rmse = _rmse(m, y_test)
    r2 = _r2(m, y_test)

    return new, rmse, r2, np.var(y_test), len(X_train.index)


def _VIF(X_train, cutoff=5):
    # Normal VIF: 1/(1-R^2) < cutoff
    # cutoff * (1-R^2) > 1, smallest gets removed
    # cf = collinearity factor = cutoff * (1-R^2)
    removed_cols = []
    while True:
        min_cf = np.inf
        min_col = ''
        for col in X_train.columns:
            model = sm.OLS(X_train[col], X_train.drop(col, axis=1))
            results = model.fit()
            cf = cutoff * (1-results.rsquared)
            if cf < min_cf:
                min_cf, min_col = cf, col

        if min_cf < 1:
            removed_cols.append(min_col)
            X_train.drop(min_col, axis=1, inplace=True)
        else:
            return X_train, removed_cols


def linear_regression(train, test, new):
    X_train, X_test, y_train, y_test = separate_ys(train, test)
    X_train = X_train.drop(columns=['Date', 'Ticker']).fillna(0)
    X_test = X_test.drop(columns=['Date', 'Ticker']).fillna(0)
    new = new.fillna(0)

    # Remove any constant column in X_train from all Xs.
    for col in X_train.columns:
        if X_train[col].var() == 0:
            X_train = X_train.drop(col, axis=1)
            X_test = X_test.drop(col, axis=1)
            new = new.drop(col, axis=1)

    # Remove columns with high multicollinearity (VIF > 5?)
    X_train, removed_cols = _VIF(X_train)
    X_test = X_test.drop(columns=removed_cols)
    new = new.drop(columns=removed_cols)

    # Add intercept column
    X_train['intercept'] = 1
    X_test['intercept'] = 1
    new['intercept'] = 1

    model = sm.OLS(y_train, X_train)
    results = model.fit()
    test_pred = results.predict(X_test)
    rmse = _rmse(test_pred, y_test)
    r2 = _r2(test_pred, y_test)

    try:
        new['prediction'] = results.predict(new.drop(columns=['Date', 'Ticker']))
    except ValueError:
        print('It just happened here you dingus')
    return new, rmse, r2, np.var(y_test), len(X_train.index)


def random_forest(train, test, new):
    train, test, new = (
        train.set_index(['Date', 'Ticker']), test.set_index(['Date', 'Ticker']), new.set_index(['Date', 'Ticker'])
    )
    X_train, X_test, y_train, y_test = separate_ys(train, test)
    model = RandomForestRegressor()
    model.fit(X_train, y_train)

    test_pred = model.predict(X_test)
    rmse = _rmse(test_pred, y_test)
    r2 = _r2(test_pred, y_test)
    new['prediction'] = model.predict(new)
    return new.reset_index(), rmse, r2, np.var(y_test), len(train.index)


ALL_MODELS = [avg_nearest, avg, linear_regression, random_forest]
ALL_MODELS = {f.__name__: f for f in ALL_MODELS}

# Todo
#   XGBoost
