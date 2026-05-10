from typing import List, Tuple, Union

import pandas as pd

from src.data_access.base_dao import BaseDAO, _create_filters
from src.data_access.db.async_database import AsyncDatabase


class Company(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "Company")

    async def get(
        self, columns: list | tuple | str = "*", **kwargs
    ) -> Union[pd.DataFrame, List[Tuple]]:
        if isinstance(columns, str):
            columns = columns.replace(" ", "").split(",")
        for i in range(len(columns)):
            # Make sure we are selecting from the Company table only in case of duplicate column names
            if columns[i].startswith(self.table_name + "."):
                continue
            columns[i] = self.table_name + "." + columns[i]
        columns = ", ".join(columns)
        if "Company.enabled" not in kwargs:
            kwargs["Company.enabled"] = 1
        qry = f"SELECT {columns} FROM {self.table_name} LEFT JOIN TickerType ON Company.ticker_type_id = TickerType.id"
        params, where_clause = _create_filters(kwargs)
        qry += f" WHERE {' AND '.join(where_clause)}" if where_clause else ""

        print(qry)

        return await self.db.execute_query(qry, params, return_type="DataFrame")
