"""Data quality checks for ML training pipelines."""

import numpy as np
import pandas as pd

# --- Constants ---

NEAR_CONSTANT_RANGE_FAIL: float = 0.1
NEAR_CONSTANT_RANGE_WARN: float = 1.0

OUTLIER_Z_THRESHOLD: float = 5.0
OUTLIER_FRACTION_WARN: float = 0.001
OUTLIER_FRACTION_FAIL: float = 0.005

LOSS_SPIKE_ROLLING_WINDOW: int = 100
LOSS_SPIKE_WARN_MULTIPLIER: float = 10.0
LOSS_SPIKE_FAIL_MULTIPLIER: float = 100.0

_STATUS_ORDER: dict = {"pass": 0, "warn": 1, "fail": 2}


# --- Helpers ---


def _make_result(status: str, value: float, message: str) -> dict:
    """Build a standardized check result dict."""
    return {"status": status, "value": value, "message": message}


def _worst_status(statuses: list) -> str:
    """Return the most severe status from a list of status strings."""
    if not statuses:
        return "pass"
    return max(statuses, key=lambda s: _STATUS_ORDER.get(s, 0))


# --- Individual checks ---


def _check_post_scale_range(col_name: str, values: np.ndarray) -> dict:
    """Check that a z-scored column has meaningful spread (p99 - p1).

    Args:
        col_name: Column name for reporting.
        values: 1-D array of z-scored values with NaNs removed.

    Returns:
        Result dict with status, value (p99-p1 spread), and message.
    """
    if len(values) < 2:
        return _make_result("pass", 0.0, f"{col_name}: too few values to check")
    spread = float(np.percentile(values, 99) - np.percentile(values, 1))
    if spread < NEAR_CONSTANT_RANGE_FAIL:
        return _make_result(
            "fail", spread,
            f"{col_name}: near-constant (p99-p1={spread:.4f} < {NEAR_CONSTANT_RANGE_FAIL})",
        )
    if spread < NEAR_CONSTANT_RANGE_WARN:
        return _make_result(
            "warn", spread,
            f"{col_name}: low variance (p99-p1={spread:.4f} < {NEAR_CONSTANT_RANGE_WARN})",
        )
    return _make_result("pass", spread, f"{col_name}: p99-p1={spread:.4f}")


def _check_outlier_fraction(col_name: str, values: np.ndarray) -> dict:
    """Check that the fraction of extreme z-scores is below limits.

    Outliers are defined as |z| > OUTLIER_Z_THRESHOLD.

    Args:
        col_name: Column name for reporting.
        values: 1-D array of z-scored values with NaNs removed.

    Returns:
        Result dict with status, value (outlier fraction), and message.
    """
    if len(values) == 0:
        return _make_result("pass", 0.0, f"{col_name}: no values to check")
    fraction = float(np.mean(np.abs(values) > OUTLIER_Z_THRESHOLD))
    if fraction > OUTLIER_FRACTION_FAIL:
        return _make_result(
            "fail", fraction,
            f"{col_name}: {fraction:.4%} outliers (|z|>{OUTLIER_Z_THRESHOLD})"
            f" > {OUTLIER_FRACTION_FAIL:.4%}",
        )
    if fraction > OUTLIER_FRACTION_WARN:
        return _make_result(
            "warn", fraction,
            f"{col_name}: {fraction:.4%} outliers (|z|>{OUTLIER_Z_THRESHOLD})"
            f" > {OUTLIER_FRACTION_WARN:.4%}",
        )
    return _make_result("pass", fraction, f"{col_name}: {fraction:.4%} outliers")


# --- Composite checks ---


def check_features(
    df: pd.DataFrame,
    ignore_cols: list | None = None,
) -> dict:
    """Run post-scale range and outlier fraction checks on all numeric columns.

    Args:
        df: DataFrame whose numeric columns contain z-scored values.
        ignore_cols: Column names to skip entirely.

    Returns:
        Dict mapping column name to
        {"post_scale_range": result, "outlier_fraction": result, "status": str}.
    """
    skip = set(ignore_cols or [])
    report = {}
    for col in df.select_dtypes(include="number").columns:
        if col in skip:
            continue
        values = df[col].dropna().to_numpy()
        range_result = _check_post_scale_range(col, values)
        outlier_result = _check_outlier_fraction(col, values)
        report[col] = {
            "post_scale_range": range_result,
            "outlier_fraction": outlier_result,
            "status": _worst_status([range_result["status"], outlier_result["status"]]),
        }
    return report


def check_loss_spikes(
    train_loss_history: list,
    val_loss_history: list | None = None,
) -> dict:
    """Detect loss spikes relative to the rolling minimum of prior batches.

    For each batch after the warm-up window, the spike ratio is defined as
    loss[i] / rolling_min(loss[i-window:i]). A high ratio means the loss
    jumped up sharply after the model had reached a lower level.
    The first LOSS_SPIKE_ROLLING_WINDOW points are excluded (warm-up phase).

    Args:
        train_loss_history: Per-batch training losses.
        val_loss_history: Per-batch validation losses (optional).

    Returns:
        Dict with "train" and optionally "val" result dicts.
    """
    results = {}
    for key, history in [("train", train_loss_history), ("val", val_loss_history)]:
        if not history:
            continue
        arr = np.array(history, dtype=float)
        if len(arr) <= LOSS_SPIKE_ROLLING_WINDOW:
            results[key] = _make_result(
                "pass", 1.0, f"{key}: too few batches ({len(arr)}) to detect spikes"
            )
            continue
        rolling_min = (
            pd.Series(arr)
            .rolling(LOSS_SPIKE_ROLLING_WINDOW, min_periods=1)
            .min()
            .shift(1)
        )
        ratios = arr / (rolling_min.to_numpy() + 1e-10)
        max_ratio = float(np.nanmax(ratios[LOSS_SPIKE_ROLLING_WINDOW:]))
        if max_ratio > LOSS_SPIKE_FAIL_MULTIPLIER:
            status = "fail"
        elif max_ratio > LOSS_SPIKE_WARN_MULTIPLIER:
            status = "warn"
        else:
            status = "pass"
        results[key] = _make_result(
            status, max_ratio,
            f"{key}: max spike ratio={max_ratio:.1f}x rolling min"
            f" (warn={LOSS_SPIKE_WARN_MULTIPLIER}, fail={LOSS_SPIKE_FAIL_MULTIPLIER})",
        )
    return results


def _summarize(feature_report: dict, loss_report: dict) -> str:
    """Generate a one-line human-readable summary of the combined report."""
    n_fail = sum(1 for v in feature_report.values() if v["status"] == "fail")
    n_warn = sum(1 for v in feature_report.values() if v["status"] == "warn")
    loss_worst = _worst_status([v["status"] for v in loss_report.values()])
    parts = [f"{len(feature_report)} features checked"]
    if n_fail:
        parts.append(f"{n_fail} failed")
    if n_warn:
        parts.append(f"{n_warn} warned")
    parts.append(f"loss spikes: {loss_worst}")
    return "; ".join(parts)


def run_data_quality_checks(
    train_data: pd.DataFrame,
    train_loss_history: list,
    val_loss_history: list | None = None,
    ignore_cols: list | None = None,
) -> dict:
    """Run all data quality checks and return a JSON-serializable report.

    Checks performed:
      - Per-column post-scale range (p99-p1): detects near-constant features.
      - Per-column outlier fraction (|z| > threshold): detects extreme values.
      - Loss spike detection: detects sudden upward jumps in training loss.

    Args:
        train_data: Scaled and imputed training DataFrame.
        train_loss_history: Per-batch training losses from the most recent run.
        val_loss_history: Per-batch validation losses (optional).
        ignore_cols: Column names to exclude from feature checks.

    Returns:
        Report dict: {overall_status, feature_checks, loss_spike_check, summary}.
    """
    effective_ignore = ignore_cols or ["symbol", "timestamp", "target"]
    feature_report = check_features(train_data, effective_ignore)
    loss_report = check_loss_spikes(train_loss_history, val_loss_history)
    feature_statuses = [v["status"] for v in feature_report.values()]
    loss_statuses = [v["status"] for v in loss_report.values()]
    overall = _worst_status(feature_statuses + loss_statuses)
    return {
        "overall_status": overall,
        "feature_checks": feature_report,
        "loss_spike_check": loss_report,
        "summary": _summarize(feature_report, loss_report),
    }
