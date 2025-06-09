from typing import List, Tuple, Union

import pandas as pd

from .base_dao import BaseDAO, _create_filters
from .db.async_database import AsyncDatabase


class Company(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "Company")

    async def get(
        self, columns: list | tuple | str = "*", **kwargs
    ) -> Union[pd.DataFrame, List[Tuple]]:
        if isinstance(columns, (tuple, list)):
            columns = ", ".join(columns)
        if "enabled" not in kwargs:
            kwargs["enabled"] = 1
        qry = f"SELECT {columns} FROM {self.table_name} LEFT JOIN TickerType ON Company.ticker_type_id = TickerType.id"
        params, where_clause = _create_filters(kwargs)
        qry += f" WHERE {' AND '.join(where_clause)}" if where_clause else ""

        return await self.db.execute_query(qry, params, return_type="DataFrame")
