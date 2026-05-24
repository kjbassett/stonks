import re
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

_NEWS_ID_PAT = re.compile(r"^news\d+_id$")
_NON_X_COLS = {"target", "symbol", "timestamp"}


def _news_id_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if _NEWS_ID_PAT.match(c)]


class HybridDataset(Dataset):
    """Dataset for rows with pre-computed news embeddings.

    Structured numeric features and embedding vectors are concatenated into a
    single flat tensor, matching the NumericalDataset output format so both
    can be used interchangeably with NumericalModel.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        embedding_lookup: Dict[str, np.ndarray],
        use_weights: bool = False,
        max_weight: float = 10.0,
        n_bins: int = 50,
    ) -> None:
        """Initialise the dataset.

        Args:
            data: Pre-processed DataFrame (one-hot encoded, scaled).
            embedding_lookup: Dict mapping news_id -> float32 numpy array.
            use_weights: Compute inverse-density sample weights.
            max_weight: Cap on per-sample weight.
            n_bins: Histogram bins used for density estimation.
        """
        self.data = data.reset_index(drop=True)
        self.embedding_lookup = embedding_lookup
        self.news_id_cols = _news_id_cols(data)

        first_emb = next(iter(embedding_lookup.values()), None)
        self.embedding_dim: int = len(first_emb) if first_emb is not None else 0

        exclude = _NON_X_COLS | set(self.news_id_cols)
        x_cols = [c for c in data.columns if c not in exclude and data[c].dtype != object]
        self._x_struct = data[x_cols].fillna(0).values.astype(np.float32)

        self.symbols = data["symbol"].values
        self.timestamps = data["timestamp"].values
        self.closes = data["close"].values

        self.y, self.weights = _build_targets_and_weights(
            data, use_weights, max_weight, n_bins
        )
        # Pre-extract news IDs as object array — avoids ambiguous Series indexing
        n_news = len(self.news_id_cols)
        self._news_id_arr: np.ndarray = (
            data[self.news_id_cols].to_numpy()
            if n_news > 0
            else np.empty((len(data), 0), dtype=object)
        )

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: int) -> tuple:
        emb_parts = []
        for j in range(len(self.news_id_cols)):
            news_id = self._news_id_arr[idx, j]
            if not isinstance(news_id, str) or news_id not in self.embedding_lookup:
                emb_parts.append(np.zeros(self.embedding_dim, dtype=np.float32))
            else:
                emb_parts.append(self.embedding_lookup[news_id].astype(np.float32))

        x = np.concatenate([self._x_struct[idx], *emb_parts]) if emb_parts else self._x_struct[idx]
        meta = {
            "symbol": self.symbols[idx],
            "timestamp": self.timestamps[idx],
            "close": self.closes[idx],
        }
        return torch.tensor(x, dtype=torch.float32), self.y[idx], self.weights[idx], meta


class NumericalDataset(Dataset):
    """Dataset for rows with no text features."""

    def __init__(
        self,
        data: pd.DataFrame,
        n_bins: int = 50,
        use_weights: bool = False,
        max_weight: float = 10.0,
    ) -> None:
        self.symbols = data["symbol"].values
        self.timestamps = data["timestamp"].values
        self.closes = data["close"].values
        self.x = torch.tensor(
            data[data.columns.difference(["target", "symbol", "timestamp"])].values,
            dtype=torch.float32,
        )
        self.y, self.weights = _build_targets_and_weights(
            data, use_weights, max_weight, n_bins
        )

    def __len__(self) -> int:
        return len(self.x)

    def __getitem__(self, idx: int) -> tuple:
        meta = {
            "symbol": self.symbols[idx],
            "timestamp": self.timestamps[idx],
            "close": self.closes[idx],
        }
        return self.x[idx], self.y[idx], self.weights[idx], meta


def _build_targets_and_weights(
    data: pd.DataFrame,
    use_weights: bool,
    max_weight: float,
    n_bins: int,
) -> tuple:
    """Return (y_tensor, weights_tensor) for a dataset.

    Args:
        data: DataFrame possibly containing a 'target' column.
        use_weights: Whether to compute inverse-density weights.
        max_weight: Cap on per-sample weight.
        n_bins: Histogram bins for density estimation.

    Returns:
        Tuple of (y, weights) float32 tensors, both shape (N,).
    """
    if "target" not in data.columns:
        y = torch.full((len(data),), float("nan"), dtype=torch.float32)
        return y, torch.ones(len(data), dtype=torch.float32)

    y_np = data["target"].values.astype(np.float32)
    y = torch.tensor(y_np, dtype=torch.float32)

    if use_weights:
        hist, bin_edges = np.histogram(y_np, bins=n_bins, density=False)
        bin_idx = np.digitize(y_np, bin_edges[:-1], right=True)
        hist = hist.astype(np.float32) + 1e-6
        density = hist[bin_idx - 1]
        weights = np.clip(1.0 / density / (1.0 / density).mean(), 0.0, max_weight)
        return y, torch.tensor(weights, dtype=torch.float32)

    return y, torch.ones(len(data), dtype=torch.float32)


def shuffle(df: pd.DataFrame) -> pd.DataFrame:
    """Shuffle the DataFrame rows with a fixed seed.

    Args:
        df: Input DataFrame.

    Returns:
        Shuffled DataFrame with reset index.
    """
    return df.sample(frac=1, random_state=42)


async def create_datasets(
    train_data: pd.DataFrame,
    embedding_lookup: Optional[Dict[str, np.ndarray]] = None,
    test_data: Optional[pd.DataFrame] = None,
    use_weights: bool = False,
) -> "list | Dataset":
    """Create PyTorch Dataset(s) from pre-processed DataFrames.

    Args:
        train_data: Training DataFrame (or sole DataFrame in inference mode).
        embedding_lookup: Optional dict of news_id -> embedding array.
        test_data: Pre-split test DataFrame. When provided returns
            [train_dataset, test_dataset]. When None returns a single dataset.
        use_weights: Apply inverse-density sample weights (training only).

    Returns:
        ``[train_dataset, test_dataset]`` when test_data is given, else a single Dataset.
    """
    if test_data is not None:
        return [
            create_dataset(train_data, use_weights=True, embedding_lookup=embedding_lookup),
            create_dataset(test_data, use_weights=False, embedding_lookup=embedding_lookup),
        ]
    return create_dataset(train_data, use_weights=use_weights, embedding_lookup=embedding_lookup)


def create_dataset(
    data: pd.DataFrame,
    use_weights: bool = False,
    embedding_lookup: Optional[Dict[str, np.ndarray]] = None,
) -> Dataset:
    """Create the appropriate Dataset type based on whether embeddings are provided.

    Args:
        data: Pre-processed DataFrame.
        use_weights: Apply inverse-density sample weights.
        embedding_lookup: If provided, creates a HybridDataset.

    Returns:
        HybridDataset when embedding_lookup is not None, else NumericalDataset.
    """
    if embedding_lookup is not None:
        return HybridDataset(data, embedding_lookup, use_weights=use_weights)
    return NumericalDataset(data, use_weights=use_weights)
