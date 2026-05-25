import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_predictions_vs_targets(df):
    preds = df["prediction"].copy()
    targets = df["target"].copy()

    pred_lo, pred_hi = np.percentile(preds, 1), np.percentile(preds, 99)
    tgt_lo, tgt_hi = np.percentile(targets, 1), np.percentile(targets, 99)
    mask = (preds >= pred_lo) & (preds <= pred_hi) & (targets >= tgt_lo) & (targets <= tgt_hi)
    preds, targets = preds[mask], targets[mask]

    fig, ax = plt.subplots(figsize=(12, 12))
    ax.scatter(targets, preds, alpha=0.5, edgecolor="k", s=1)

    min_val = min(preds.min(), targets.min())
    max_val = max(preds.max(), targets.max())
    ax.plot([min_val, max_val], [min_val, max_val], "r--", label="Ideal: y = x")

    ax.set_xlabel("Unscaled Target")
    ax.set_ylabel("Unscaled Prediction")
    ax.set_title("Predictions vs Targets")
    ax.legend()
    ax.grid(True)
    fig.tight_layout()
    return fig


def _plot_pct_stacked_hist(ax, series, is_positive, title, n_bins=80):
    lo, hi = np.percentile(series, 1), np.percentile(series, 99)
    mask = (series > lo) & (series < hi)
    s = series[mask]
    pos = is_positive[mask]

    bins = np.linspace(lo, hi, n_bins + 1)
    width = (bins[1] - bins[0]) * 0.9
    centers = (bins[:-1] + bins[1:]) / 2

    counts_pos, _ = np.histogram(s[pos], bins=bins)
    counts_neg, _ = np.histogram(s[~pos], bins=bins)
    total = counts_pos + counts_neg

    occupied = total > 0
    pct_neg = np.where(occupied, counts_neg / total * 100, 0.0)
    pct_pos = np.where(occupied, counts_pos / total * 100, 0.0)

    ax.bar(centers, pct_neg, width=width, color="steelblue", label="target ≤ 0")
    ax.bar(centers, pct_pos, width=width, bottom=pct_neg, color="salmon", label="target > 0")
    ax.axhline(50, color="black", linestyle="--", linewidth=0.8, alpha=0.5, label="50%")
    ax.set_ylim(0, 100)
    ax.set_ylabel("%")
    ax.set_title(title)
    ax.legend(fontsize=8)


def pred_hist(df):
    pred = df["prediction"]
    var = df["variance"]
    target = df["target"]
    z_score = pred / np.sqrt(var)
    chi_sq = np.sign(pred) * pred**2 / var
    cube_score = pred**3 / var**1.5
    is_positive = target > 0

    freq_series = [pred, var, z_score, chi_sq, cube_score, target]
    freq_titles = [
        "Prediction", "Variance",
        "Prediction / sqrt(Variance)", "Prediction² / Variance",
        "Prediction³ / Variance^(3/2)", "Target",
    ]
    pct_series = [pred, var, z_score, chi_sq, cube_score, target]
    pct_titles = [
        "Prediction %", "Variance %",
        "Prediction / sqrt(Variance) %", "Prediction² / Variance %",
        "Prediction³ / Variance^(3/2) %", "Target %",
    ]

    n_cols = 6
    fig = plt.figure(figsize=(24, 10))
    freq_axes = [fig.add_subplot(2, n_cols, i + 1) for i in range(n_cols)]
    pct_axes = [fig.add_subplot(2, n_cols, i + n_cols + 1) for i in range(n_cols)]

    for ax, s, title in zip(freq_axes, freq_series, freq_titles):
        lo, hi = np.percentile(s, 1), np.percentile(s, 99)
        mask = (s > lo) & (s < hi)
        bins = np.linspace(lo, hi, 81).tolist()
        ax.hist(  # type: ignore[call-overload]
            [s[mask & ~is_positive], s[mask & is_positive]],
            bins=bins,
            stacked=True,
            color=["steelblue", "salmon"],  # type: ignore[arg-type]
            edgecolor="black",
            linewidth=0.3,
            label=["target ≤ 0", "target > 0"],  # type: ignore[arg-type]
        )
        ax.set_title(title)
        ax.legend(fontsize=8)

    for ax, s, title in zip(pct_axes, pct_series, pct_titles):
        _plot_pct_stacked_hist(ax, s, is_positive, title)

    fig.tight_layout()
    return fig


def plot_loss_history(train_loss_history, val_loss_history, window_size=100):
    train_series = pd.Series(train_loss_history)
    moving_avg = train_series.rolling(window=window_size).mean()

    val_steps = [step for step, _ in val_loss_history]
    val_losses = [loss for _, loss in val_loss_history]

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(train_series.index, train_series, color="steelblue", alpha=0.3, linewidth=0.5, label="Train loss (raw)")
    ax1.plot(moving_avg.index, moving_avg, color="steelblue", linewidth=1.5, label=f"Train loss (MA {window_size})")
    ax1.set_xlabel("Batch step")
    ax1.set_ylabel("Train Loss", color="steelblue")
    ax1.tick_params(axis="y", labelcolor="steelblue")

    lines, labels = ax1.get_legend_handles_labels()
    if val_steps:
        ax2 = ax1.twinx()
        ax2.plot(val_steps, val_losses, color="darkorange", linewidth=1.5, marker="o", markersize=4, label="Val loss")
        ax2.set_ylabel("Val Loss", color="darkorange")
        ax2.tick_params(axis="y", labelcolor="darkorange")
        val_lines, val_labels = ax2.get_legend_handles_labels()
        lines, labels = lines + val_lines, labels + val_labels

    ax1.set_title("Training & Validation Loss")
    ax1.legend(lines, labels)
    ax1.grid(True)
    fig.tight_layout()
    return fig


def plot_training(predictions, train_loss_history, val_loss_history):
    scatter_fig = plot_predictions_vs_targets(predictions)
    pred_hist_fig = pred_hist(predictions)
    loss_fig = plot_loss_history(train_loss_history, val_loss_history)
    return scatter_fig, pred_hist_fig, loss_fig


if __name__ == "__main__":
    csv_path = "C:/Coding/stonks/organisms/model_19/2026-05-02_09-54-43/1877365762864_1777732560840.csv"
    df = pd.read_csv(csv_path)
    fig = plot_predictions_vs_targets(df)
    fig.savefig("plot.png")
    fig2 = pred_hist(df)
    fig2.savefig("pred_histogram.png")
