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

    async def insert(
        self,
        table: str,
        data: Union[Dict[str, Any], pd.DataFrame, tuple, list],
        on_conflict: str = None,
    ):
        query, params, many = construct_insert_query(table, data, on_conflict)
        return await self.execute_query(query, params, many=many)

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


def construct_insert_query(table, data, on_conflict, update_cols: list = None):
    if isinstance(data, pd.DataFrame):  # data is a DataFrame
        if data.empty:
            raise ValueError("Cannot insert an empty DataFrame")
        columns = " (" + ", ".join(data.columns) + ")"
        params = [tuple(row) for row in data.values]
        placeholders = ", ".join(["?"] * len(data.columns))
        if len(data.index) == 1:
            params = params[0]
            many = False
        else:
            many = True

    elif isinstance(data, (list, tuple)):
        if not data:
            raise ValueError("Cannot insert an empty list or tuple")
        if isinstance(data[0], (list, tuple)):  # data is list/tuple of lists/tuples
            columns = ""
            placeholders = ", ".join(["?"] * len(data[0]))
            params = data
            many = True
        elif isinstance(data[0], dict):  # data is a list/tuple of dicts
            columns = " (" + ", ".join([key for key in data[0].keys()]) + ")"
            placeholders = ", ".join(["?"] * len(data[0]))
            params = [tuple(row.values()) for row in data]
            many = True
        else:  # Data is single record in a list or tuple
            columns = ""
            placeholders = ", ".join(["?"] * len(data))
            params = data
            many = False

    elif isinstance(data, dict):
        if not data:
            raise ValueError("Cannot insert an empty dictionary")
        k = list(data.keys())[0]
        if isinstance(data[k], (list, tuple)):  # data is a dict of lists or tuple
            columns = " (" + ", ".join(data.keys()) + ")"
            placeholders = ", ".join(["?"] * len(data.keys()))
            params = [
                tuple(data[k][i] for k in data.keys()) for i in range(len(data[k]))
            ]
            if len(data[k]) == 1:
                params = params[0]
                many = False
            else:
                many = True
        else:  # data is a dict of a single record
            columns = " (" + ", ".join(data.keys()) + ")"
            placeholders = ", ".join(["?"] * len(data.keys()))
            params = tuple(data.values())
            many = False
    else:
        raise ValueError("Unsupported data type")

    # Handle ON CONFLICT clause
    if on_conflict == "UPDATE":
        if update_cols is None:
            raise ValueError(
                "update_cols must be provided when on_conflict is 'UPDATE'"
            )
        on_conflict_clause = " ON CONFLICT DO UPDATE SET "
        update_parts = [f"{col} = excluded.{col}" for col in update_cols]
        on_conflict_clause += ", ".join(update_parts)
    elif on_conflict == "IGNORE":
        on_conflict_clause = " ON CONFLICT IGNORE"
    elif on_conflict is None:
        on_conflict_clause = ""
    else:
        raise NotImplementedError(f"Unsupported on_conflict value: {on_conflict}")

    query = f"INSERT INTO {table}{columns} VALUES ({placeholders}){on_conflict_clause};"
    return query, params, many
