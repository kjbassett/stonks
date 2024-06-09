# models/db/tf_model.py

from typing import Union

from async_database import AsyncDatabase
from base_model import BaseModel


class TFModel(BaseModel):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "Models")

    async def get_by_name(self, name: str) -> Union[pd.DataFrame, list]:
        query = f"SELECT * FROM {self.table_name} WHERE name = ?;"
        return await self.db.execute_query(query, (name,), return_type="DataFrame")
