import numpy as np
import pandas as pd
from tqdm import tqdm
from webrock.decorator import plugin


def simulate_long_only(df, ratio_cutoff, capital=1.0, transaction_cost=0.0):
    """
    Simulate a long-only strategy using a fixed ratio cutoff.

    Args:
        df (pd.DataFrame): must contain ['ticker','timestamp','prediction','uncertainty','target'].
                           prediction & target are percent changes (e.g. 0.003 = 0.3%).
        ratio_cutoff (float): threshold on ratio = prediction / (uncertainty + EPS).
        capital (float): starting capital (default 1.0).
        transaction_cost (float): per-trade round-trip cost expressed as fraction of capital per trade (optional).
    """
    assert {"prediction", "uncertainty", "target", "timestamp"}.issubset(df.columns)
    print("Simulating")
    df2 = df.copy()
    # ratio only for positive prediction (long-only)
    df2["ratio"] = df2["prediction"] / (df2["uncertainty"] + 1e-9)

    # group by timestamp and compute per-timestamp portfolio return
    timestamps = sorted(df2["timestamp"].unique())
    print(f"{len(timestamps)} timestamps to simulate")
    port_returns = []
    capital_now = float(capital)

    # ensure ordering by timestamp
    grouped = df2.groupby("timestamp")

    for t in tqdm(timestamps, desc=f"Ratio_cutoff {ratio_cutoff}, {capital_now}"):
        group = grouped.get_group(t)
        # select long candidates
        selected = group[(group["prediction"] > 0) & (group["ratio"] >= ratio_cutoff)]
        m = len(selected)
        if m == 0:
            # no positions → zero return
            r = 0.0
        else:
            # equal-weighted: each position weight = 1/m
            # realized returns are the 'target' percent changes
            weights = np.ones(m) / m
            returns = selected["target"].values.astype(float)  # percent changes
            r = np.dot(weights, returns)

            # transaction cost: assume cost applies once per position as fraction of capital
            if transaction_cost and transaction_cost > 0:
                # cost reduces return by approx transaction_cost * (#positions)
                # convert to per-capital reduction: transaction_cost * (m / m) = transaction_cost
                # but more realistic: cost per traded notional = transaction_cost * sum(|w_i|)
                # sum(|w_i|)=1 so cost = transaction_cost
                r = r - transaction_cost

        r = round(r, 6)
        capital_now = capital_now * (1.0 + r)
        if r > 0:
            print(r, capital_now)
    #     port_returns.append(
    #         {
    #             "timestamp": t,
    #             "percentage_change": r,
    #             "n_selected": m,
    #             "new_capital": capital_now,
    #         }
    #     )
    #
    # port_returns = pd.DataFrame(port_returns).set_index("timestamp")

    total_return_pct = capital_now / capital - 1.0

    return total_return_pct


def tune_ratio_cutoff(
    df,
    candidate_cutoffs=None,
    n_candidates=50,
    capital=1.0,
    transaction_cost=0.0,
    min_ratio_percentile=50,
):
    """
    Tune the ratio cutoff by evaluating candidate cutoffs using the validation 'target' column.

    Args:
        df (pd.DataFrame): must contain ['prediction','uncertainty','target','timestamp'].
        candidate_cutoffs (array-like, optional): explicit cutoff values to test.
        n_candidates (int): if candidate_cutoffs is None, choose this many cutoffs between
                            percentile min_ratio_percentile and 99.9 percentile of ratio.
        metric (str): 'total_return' or 'sharpe' to maximize.
        capital, transaction_cost: passed to simulator.
        min_ratio_percentile (float): lowest percentile to consider (avoid tiny cutoffs).

    Returns:
        best (dict): {
            'best_cutoff', 'best_score', 'results_df' (cutoff, score, total_return, total_return_pct, sharpe)
        }
    """
    assert "target" in df.columns, "tuning requires target column"
    dfc = df.copy()
    dfc["ratio"] = dfc["prediction"] / (dfc["uncertainty"] + 1e-9)
    ratios = dfc["ratio"].values
    # candidate cutoffs derived from empirical distribution if not provided
    if candidate_cutoffs is None:
        low = np.percentile(ratios, min_ratio_percentile)
        high = np.percentile(ratios, 99.9)
        candidate_cutoffs = np.linspace(low, high, n_candidates)

    best_cutoff = np.inf
    best_returns = 0
    for cutoff in candidate_cutoffs:
        print(f"testing cutoff {cutoff}")
        returns = simulate_long_only(
            dfc, ratio_cutoff=cutoff, capital=capital, transaction_cost=transaction_cost
        )
        if returns > best_returns:
            best_cutoff = cutoff
            best_returns = returns

    return best_cutoff, best_returns


def apply_ratio_cutoff(df, ratio_cutoff):
    return df[df["prediction"] / df["uncertainty"] > ratio_cutoff]


@plugin()
def temp_tune_ratio_cutoff(
    df_path: str,
    n_candidates: int = 50,
    capital: float = 1.0,
    transaction_cost: float = 0.0,
    min_ratio_percentile: int = 50,
):
    df = pd.read_csv(df_path)
    cutoff, returns = tune_ratio_cutoff(
        df,
        candidate_cutoffs=None,
        n_candidates=n_candidates,
        capital=capital,
        transaction_cost=transaction_cost,
        min_ratio_percentile=min_ratio_percentile,
    )
    print(cutoff, returns)
