from typing import Tuple, Union, List, Dict, Any

import aiosqlite
import pandas as pd
from async_lru import alru_cache


class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    async def connect(self):
        if self.conn is None:
            self.conn = await aiosqlite.connect(self.db_path)

    async def close(self):
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def __call__(
        self, query: str, params: Tuple = (), return_type: str = "list"
    ) -> Union[pd.DataFrame, List[Tuple]]:
        return await self.execute_query(query, params, return_type)

    async def execute_query(
        self,
        query: str,
        params: Union[Tuple, List] = (),
        return_type: str = "list",
        many=False,
    ) -> Union[int, pd.DataFrame, List[Tuple]]:
        await self.connect()
        if many:  # TODO detect this automatically somehow
            cursor = await self.conn.executemany(query, params)
        else:
            cursor = await self.conn.execute(query, params)

        if query.strip().upper().startswith("SELECT"):
            result = await cursor.fetchall()
            if return_type == "DataFrame":
                # get columns from cursor
                columns = [column[0] for column in cursor.description]
                result = pd.DataFrame(result, columns=columns)
            await cursor.close()
            return result
        else:
            await self.conn.commit()
            rowcount = cursor.rowcount
            await cursor.close()
            return rowcount  # Return number of rows affected

    async def insert(
        self,
        table: str,
        data: Union[Dict[str, Any], pd.DataFrame, tuple],
        skip_existing: bool = True,
    ):
        if isinstance(data, (dict, tuple)):
            return await self._insert_single_record(table, data, skip_existing)
        elif isinstance(data, (pd.DataFrame, list)):
            return await self._insert_multiple_records(table, data, skip_existing)
        else:
            raise ValueError(
                "Unsupported data type. Use a dictionary, list or DataFrame."
            )

    async def _insert_single_record(
        self, table: str, data: Dict[str, Any], skip_existing: bool = True
    ):
        if isinstance(data, tuple):
            columns = ""
            params = data
        elif isinstance(data, dict):
            columns = "(" + ", ".join(data.keys()) + ") "
            params = tuple(data.values())
        else:
            raise ValueError("Unsupported data type. Use a dictionary or tuple.")
        placeholders = ", ".join(["?"] * len(data))
        query = f"INSERT {'OR IGNORE ' if skip_existing else ''}INTO {table} {columns}VALUES ({placeholders});"
        return await self.execute_query(query, params)

    async def _insert_multiple_records(
        self, table: str, data: Union[pd.DataFrame, list], skip_existing: bool = True
    ):
        if isinstance(data, pd.DataFrame):
            columns = "(" + ", ".join(data.columns) + ") "
            params = [tuple(row) for row in data.values]
            placeholders = ", ".join(["?"] * len(data.columns))
        else:
            if isinstance(data[0], dict):
                columns = "(" + ", ".join([key for key in data[0].keys()]) + ") "
                params = [tuple(row.values()) for row in data]
            else:  # data is list of tuples
                columns = ""
                params = data
            placeholders = ", ".join(["?"] * len(data[0]))

        query = f"INSERT {'OR IGNORE ' if skip_existing else ''}INTO {table} {columns} VALUES ({placeholders});"
        return await self.execute_query(query, params, many=True)

    @alru_cache
    async def table_exists(self, table: str):
        result = await self.execute_query(
            f"SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table}';"
        )
        return bool(result)
