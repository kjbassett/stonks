from typing import Tuple, Union, List

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
        query_type="",
    ) -> Union[int, pd.DataFrame, List[Tuple]]:
        await self.connect()
        if many:  # TODO detect this automatically somehow
            cursor = await self.conn.executemany(query, params)
        else:
            cursor = await self.conn.execute(query, params)

        if query.strip().upper().startswith("SELECT") or query_type.upper() == "SELECT":
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

    async def get_all_tables(self):
        result = await self.execute_query(
            "SELECT name FROM sqlite_master WHERE type = 'table';"
        )
        return [row[0] for row in result]

    @alru_cache
    async def table_exists(self, table: str):
        result = await self.execute_query(
            f"SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table}';"
        )
        return bool(result)
