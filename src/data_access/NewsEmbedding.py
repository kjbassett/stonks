from typing import Dict, List

import numpy as np

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_EMBEDDING_DTYPE = np.float32


class NewsEmbedding(BaseDAO):
    """DAO for pre-computed sentence-transformer embeddings stored as BLOBs."""

    def __init__(self, db: AsyncDatabase) -> None:
        super().__init__(db, "NewsEmbedding")

    async def get_embeddings(
        self, news_ids: List[str], model: str
    ) -> Dict[str, np.ndarray]:
        """Fetch embeddings for a batch of news IDs.

        Args:
            news_ids: News IDs to look up.
            model: Embedding model name (e.g. 'all-MiniLM-L6-v2').

        Returns:
            Dict mapping news_id -> numpy array (float32).
        """
        if not news_ids:
            return {}
        placeholders = ",".join("?" * len(news_ids))
        query = (
            f"SELECT news_id, embedding FROM NewsEmbedding"
            f" WHERE model = ? AND news_id IN ({placeholders})"
        )
        rows = await self.db.execute_query(query, (model, *news_ids))
        return {
            row[0]: np.frombuffer(row[1], dtype=_EMBEDDING_DTYPE) for row in rows
        }

    async def insert_embedding(
        self, news_id: str, model: str, embedding: np.ndarray
    ) -> None:
        """Insert a single embedding, ignoring conflicts.

        Args:
            news_id: News article ID.
            model: Embedding model name.
            embedding: Float32 numpy array to store.
        """
        query = (
            "INSERT INTO NewsEmbedding (news_id, model, embedding)"
            " VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
        )
        await self.db.execute_query(
            query,
            (news_id, model, embedding.astype(_EMBEDDING_DTYPE).tobytes()),
        )

    async def get_unembedded_ids(self, model: str) -> List[str]:
        """Return News IDs that have no embedding for the given model.

        Args:
            model: Embedding model name.

        Returns:
            List of news_id strings.
        """
        query = (
            "SELECT id FROM News"
            " WHERE id NOT IN (SELECT news_id FROM NewsEmbedding WHERE model = ?)"
        )
        rows = await self.db.execute_query(query, (model,))
        return [row[0] for row in rows]

    async def clean_data(self, min_timestamp: int) -> None:
        """Delete embeddings for news articles older than min_timestamp or that no longer exist.

        Args:
            min_timestamp: Unix timestamp cutoff; embeddings for older or deleted news are removed.
        """
        query = (
            "DELETE FROM NewsEmbedding"
            " WHERE news_id NOT IN (SELECT id FROM News WHERE timestamp >= ?)"
        )
        await self.db.execute_query(query, (min_timestamp,), query_type="DELETE")
