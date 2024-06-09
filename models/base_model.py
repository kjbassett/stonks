from typing import Any, Dict, List, Tuple, Union

import pandas as pd
from async_lru import alru_cache

from .db.async_database import AsyncDatabase


class BaseModel:
    def __init__(self, db: AsyncDatabase, table_name: str):
        self.db = db
        self.table_name = table_name

    async def insert(self, data: Union[Dict[str, Any], pd.DataFrame, tuple]):
        return await self.db.insert(self.table_name, data)

    async def update(self, identifier: Any, data: Dict[str, Any]):
        set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
        params = tuple(data.values()) + (identifier,)
        query = f"UPDATE {self.table_name} SET {set_clause} WHERE id = ?;"
        return await self.db.execute_query(query, params)

    async def delete(self, identifier: Any):
        query = f"DELETE FROM {self.table_name} WHERE id = ?;"
        return await self.db.execute_query(query, (identifier,))

    async def get(self, identifier: Any) -> Union[pd.DataFrame, List[Tuple]]:
        query = f"SELECT * FROM {self.table_name} WHERE id = ?;"
        return await self.db.execute_query(
            query, (identifier,), return_type="DataFrame"
        )

    async def get_all(self) -> Union[pd.DataFrame, List[Tuple]]:
        query = f"SELECT * FROM {self.table_name};"
        return await self.db.execute_query(query, return_type="DataFrame")

    @alru_cache
    async def table_exists(self) -> bool:
        result = await self.db.execute_query(
            f"SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{self.table_name}';"
        )
        return bool(result)
