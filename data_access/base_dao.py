from typing import Any, Dict, List, Tuple, Union

import pandas as pd
from async_lru import alru_cache

from .db.async_database import AsyncDatabase


# Base Data Access Object (DAO) class.
class BaseDAO:
    def __init__(self, db: AsyncDatabase, table_name: str):
        self.db = db
        self.table_name = table_name

    async def insert(
        self,
        table: str,
        data: Union[Dict[str, Any], pd.DataFrame, tuple, list],
        on_conflict: str = "IGNORE",
    ):
        query, params, many = construct_insert_query(table, data, on_conflict)
        return await self.execute_query(query, params, many=many)

    async def update(self, identifier: Any, data: Dict[str, Any]):
        set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
        params = tuple(data.values()) + (identifier,)
        query = f"UPDATE {self.table_name} SET {set_clause} WHERE id = ?;"
        return await self.db.execute_query(query, params)

    async def delete(self, identifier: Any):
        query = f"DELETE FROM {self.table_name} WHERE id = ?;"
        return await self.db.execute_query(query, (identifier,))

    async def get(
        self, identifier: Any = None, **kwargs
    ) -> Union[pd.DataFrame, List[Tuple]]:
        query = f"SELECT * FROM {self.table_name}"
        where_clause = []
        params = []
        if identifier is not None:
            where_clause.append("id =?")
            params.append(identifier)
        for key, value in kwargs.items():
            where_clause.append(f"{key} =?")
            params.append(value)

        query += f" WHERE {' AND '.join(where_clause)}" if where_clause else ""

        return await self.db.execute_query(query, params, return_type="DataFrame")

    async def get_all(self) -> Union[pd.DataFrame, List[Tuple]]:
        query = f"SELECT * FROM {self.table_name};"
        return await self.db.execute_query(query, return_type="DataFrame")

    @alru_cache
    async def table_exists(self) -> bool:
        result = await self.db.execute_query(
            f"SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{self.table_name}';"
        )
        return bool(result)


def construct_insert_query(table, data, on_conflict, update_cols: list = None):
    if isinstance(data, pd.DataFrame):  # data is a DataFrame
        columns, placeholders, params, many = _construct_insert_query_dataframe(data)
    elif isinstance(data, (list, tuple)):
        columns, placeholders, params, many = _construct_insert_query_list(data)
    elif isinstance(data, dict):
        columns, placeholders, params, many = _construct_insert_query_dict(data)
    else:
        raise ValueError("Unsupported data type")

    on_conflict_clause = _construct_on_conflict_clause(on_conflict, update_cols)

    query = f"INSERT INTO {table}{columns} VALUES ({placeholders}){on_conflict_clause};"
    return query, params, many


def _construct_on_conflict_clause(on_conflict, update_cols):
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
    return on_conflict_clause


def _construct_insert_query_dict(data):
    if not data:
        raise ValueError("Cannot insert an empty dictionary")
    k = list(data.keys())[0]
    if isinstance(data[k], (list, tuple)):  # data is a dict of lists or tuple
        columns = " (" + ", ".join(data.keys()) + ")"
        placeholders = ", ".join(["?"] * len(data.keys()))
        params = [tuple(data[k][i] for k in data.keys()) for i in range(len(data[k]))]
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
    return columns, placeholders, params, many


def _construct_insert_query_list(data):
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
    return columns, placeholders, params, many


def _construct_insert_query_dataframe(data):
    if data.empty:
        raise ValueError("Cannot insert an empty DataFrame")
    columns = " (" + ", ".join(data.columns) + ")"
    placeholders = ", ".join(["?"] * len(data.columns))
    params = [tuple(row) for row in data.values]

    if len(data.index) == 1:
        params = params[0]
        many = False
    else:
        many = True
    return columns, placeholders, params, many
