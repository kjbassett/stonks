from typing import List

from src.data_access.dao_manager import dao_manager
from webrock.decorator import plugin

_BATCH_SIZE = 256


async def _load_texts(news_ids: List[str]) -> List[tuple]:
    """Fetch (id, title, body) for a list of news IDs.

    Args:
        news_ids: News article IDs to load.

    Returns:
        List of (id, title, body) tuples.
    """
    placeholders = ",".join("?" * len(news_ids))
    query = f"SELECT id, title, body FROM News WHERE id IN ({placeholders})"
    rows = await dao_manager.db.execute_query(query, tuple(news_ids))
    return rows


def _build_text(title: str, body: str) -> str:
    """Concatenate title and body into a single string for embedding.

    Args:
        title: Article title.
        body: Article body.

    Returns:
        Combined text string.
    """
    parts = [p for p in (title, body) if p]
    return " ".join(parts)


@plugin()
async def compute_news_embeddings(
    model_name: str = "all-MiniLM-L6-v2",
) -> str:
    """Pre-compute and store sentence-transformer embeddings for all un-embedded news.

    Args:
        model_name: HuggingFace sentence-transformers model identifier.

    Returns:
        Summary string with counts of embedded and skipped articles.
    """
    from sentence_transformers import SentenceTransformer  # optional heavy dep

    emb_dao = dao_manager.get_dao("NewsEmbedding")
    unembedded_ids = await emb_dao.get_unembedded_ids(model_name)

    if not unembedded_ids:
        return f"0 articles embedded (all already done for model '{model_name}')."

    model = SentenceTransformer(model_name)
    embedded = 0
    skipped = 0

    for batch_start in range(0, len(unembedded_ids), _BATCH_SIZE):
        batch_ids = unembedded_ids[batch_start : batch_start + _BATCH_SIZE]
        rows = await _load_texts(batch_ids)

        if not rows:
            skipped += len(batch_ids)
            continue

        texts = [_build_text(row[1], row[2]) for row in rows]
        embeddings = model.encode(texts, show_progress_bar=False)

        for row, emb in zip(rows, embeddings):
            await emb_dao.insert_embedding(row[0], model_name, emb)

        embedded += len(rows)
        skipped += len(batch_ids) - len(rows)
        print(f"Embedded {embedded}/{len(unembedded_ids)} articles...")

    return (
        f"Done. {embedded} articles embedded, {skipped} skipped"
        f" (model: '{model_name}')."
    )
