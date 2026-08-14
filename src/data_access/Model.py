import json
import time
from typing import Optional

import pandas as pd

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS Model (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    organism_name        TEXT NOT NULL,
    organism_version     TEXT NOT NULL,
    created_at           INTEGER NOT NULL,
    score                REAL,
    fitness              REAL,
    parameters           TEXT,
    parameter_diff       TEXT,
    dna_summary          TEXT,
    data_quality_metrics TEXT,
    model_performance    TEXT,
    notes                TEXT,
    UNIQUE(organism_name, organism_version)
)
"""


class Model(BaseDAO):
    """DAO for the organism-version performance log.

    One row per saved organism version (score/fitness/hyperparameters/diff/notes),
    keyed by (organism_name, organism_version). Prediction rows link here via
    model_id rather than duplicating those strings.
    """

    def __init__(self, db: AsyncDatabase) -> None:
        super().__init__(db, "Model")

    async def init2(self) -> None:
        """Create the Model table if absent, dropping a stale pre-migration schema first."""
        if await self.table_exists():
            columns = await self.db.execute_query("PRAGMA table_info(Model)", query_type="SELECT")
            column_names = {row[1] for row in columns}
            if "organism_name" not in column_names:
                await self.db.execute_query("DROP TABLE Model", query_type="DELETE")
            elif "model_performance" not in column_names:
                # Additive: preserves existing rows, just adds the new column.
                await self.db.execute_query(
                    "ALTER TABLE Model ADD COLUMN model_performance TEXT", query_type="INSERT"
                )
        await self.db.execute_query(_CREATE_TABLE, query_type="INSERT")
        await super().init2()

    async def log_version(
        self,
        organism_name: str,
        organism_version: str,
        score: Optional[float],
        fitness: Optional[float],
        parameters: Optional[dict],
        dna_summary: Optional[str],
        data_quality_metrics: Optional[dict],
        model_performance: Optional[dict] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Insert a new organism-version row, diffing parameters against the
        most recent prior version of the same organism_name.

        Args:
            organism_name: Organism name.
            organism_version: Organism version string (never "latest" here — the
                actual resolved version).
            score: Raw score, or None if not computed on this code path.
            fitness: Population-relative fitness, or None if not applicable.
            parameters: organism.parameters at save time.
            dna_summary: dna2str(organism.dna) snapshot.
            data_quality_metrics: data quality checks about the *input data*
                (feature ranges, outliers, loss spikes) — not model performance.
            model_performance: how well the trained *model* performs
                (classification_metrics, final_mse, warmup_mse, final_nll).
            notes: Optional free-text note supplied by the caller.

        Returns:
            The new row's id.
        """
        previous = await self.db.execute_query(
            """
            SELECT parameters FROM Model
            WHERE organism_name = ?
            ORDER BY created_at DESC, id DESC LIMIT 1
            """,
            (organism_name,),
            return_type="DataFrame",
        )
        parameter_diff = None
        if parameters and not previous.empty and previous.iloc[0]["parameters"]:
            old_parameters = json.loads(previous.iloc[0]["parameters"])
            diff = {}
            for key in set(old_parameters) | set(parameters):
                old_val, new_val = old_parameters.get(key), parameters.get(key)
                if old_val != new_val:
                    diff[key] = {"old": old_val, "new": new_val}
            parameter_diff = json.dumps(diff) if diff else None

        row = {
            "organism_name": organism_name,
            "organism_version": organism_version,
            "created_at": int(time.time()),
            "score": score,
            "fitness": fitness,
            "parameters": json.dumps(parameters) if parameters else None,
            "parameter_diff": parameter_diff,
            "dna_summary": dna_summary,
            "data_quality_metrics": json.dumps(data_quality_metrics) if data_quality_metrics else None,
            "model_performance": json.dumps(model_performance) if model_performance else None,
            "notes": notes,
        }
        await self.insert(row, on_conflict=None)
        result = await self.db.execute_query(
            "SELECT id FROM Model WHERE organism_name = ? AND organism_version = ?",
            (organism_name, organism_version),
            return_type="DataFrame",
        )
        return int(result.iloc[0]["id"])

    async def get_id(self, organism_name: str, organism_version: str) -> Optional[int]:
        """Look up a Model row's id by organism_name/organism_version.

        Args:
            organism_name: Organism name.
            organism_version: Exact version string, or "latest" to resolve the
                most recently created row for organism_name.

        Returns:
            The row's id, or None if not found.
        """
        if organism_version == "latest":
            # id DESC breaks ties when two versions are logged within the same
            # created_at second — id is AUTOINCREMENT, so it's a reliable
            # insertion-order tiebreaker where created_at alone isn't.
            query = """
                SELECT id FROM Model WHERE organism_name = ?
                ORDER BY created_at DESC, id DESC LIMIT 1
            """
            params = (organism_name,)
        else:
            query = "SELECT id FROM Model WHERE organism_name = ? AND organism_version = ?"
            params = (organism_name, organism_version)
        result = await self.db.execute_query(query, params, return_type="DataFrame")
        return int(result.iloc[0]["id"]) if not result.empty else None

    async def get_history(self, organism_name: str) -> pd.DataFrame:
        """Return all logged versions for organism_name, ordered by creation time."""
        return await self.db.execute_query(
            "SELECT * FROM Model WHERE organism_name = ? ORDER BY created_at",
            (organism_name,),
            return_type="DataFrame",
        )
