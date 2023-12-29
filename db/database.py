import os
import sqlite3
from typing import List, Tuple, Union, Dict, Any

import pandas as pd


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def __call__(
        self, query: str, params: Tuple = (), return_type: str = "list"
    ) -> Union[pd.DataFrame, List[Tuple]]:
        """Make the Database object callable to execute queries."""
        return self.execute_query(query, params, return_type)

    def _connect(self):
        """Create a database connection."""
        return sqlite3.connect(self.db_path)

    def execute_query(
        self, query: str, params: Union[Tuple, List] = (), return_type: str = "list"
    ) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute a SQL query with the specified return type.
        Emphasis on convenience.

        Args:
            query (str): The SQL query to execute.
            params (Union[Tuple, List], optional): Parameters to substitute in the query.
            return_type (str, optional): The desired return type ('DataFrame' or 'List').

        Returns:
            Union[pd.DataFrame, List[Tuple]]: The query results in the specified return type.

        If `return_type` is set to 'DataFrame', the results are returned as a Pandas DataFrame.
        If `return_type` is set to 'list', the results are returned as a list of tuples.

        The function can handle both single query executions and bulk insert operations based on the data type
        of `params`. When `params` is a list of tuples, it performs an `executemany` operation.
        """
        print(query)
        with self._connect() as conn:
            cursor = conn.cursor()
            if return_type == "DataFrame":
                cursor.execute(query, params)
                # Use Pandas to read the results into a DataFrame
                result = pd.read_sql_query(query, conn, params=params)
            elif return_type == "list":
                # Check if params is a list of tuples for executemany
                if isinstance(params, list) and all(
                    isinstance(p, tuple) for p in params
                ):
                    cursor.executemany(query, params)
                    result = None  # No direct result for executemany
                else:
                    cursor.execute(query, params)
                    result = cursor.fetchall()
            else:
                raise ValueError("Invalid return_type. Use 'DataFrame' or 'List'.")

            conn.commit()

        return result

    def insert(
        self,
        table: str,
        data: Union[Dict[str, Any], pd.DataFrame],
        skip_existing: bool = True,
    ):
        """Insert a record or records into the database."""
        if isinstance(data, dict):
            self._insert_single_record(table, data, skip_existing)
        elif isinstance(data, pd.DataFrame):
            self._insert_multiple_records(table, data, skip_existing)
        else:
            raise ValueError("Unsupported data type. Use a dictionary or a DataFrame.")

    def _insert_single_record(
        self, table: str, data: Dict[str, Any], skip_existing: bool = True
    ):
        """Insert a single record into the database."""
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        query = f"INSERT {'OR IGNORE ' if skip_existing else ''}INTO {table} ({columns}) VALUES ({placeholders});"
        params = tuple(data.values())
        self.execute_query(query, params)

    def _insert_multiple_records(
        self, table: str, data: pd.DataFrame, skip_existing: bool = True
    ):
        """Insert multiple records from a DataFrame into the database."""
        columns = ", ".join(data.columns)
        placeholders = ", ".join(["?"] * len(data.columns))
        query = f"INSERT {'OR IGNORE ' if skip_existing else ''}INTO {table} ({columns}) VALUES ({placeholders});"
        params = [tuple(row) for row in data.values]
        self.execute_query(query, params)


def create_database_if_not_exists(db_path: str, schema_path: str = "create_db.sql"):
    # Check if the database already exists
    db_exists = os.path.exists(db_path)

    if not db_exists:
        # Establish a new database connection
        with sqlite3.connect(db_path) as conn:
            # Open the schema file to read the SQL commands
            with open(schema_path, "r") as f:
                sql_script = f.read()

            # Execute the SQL script
            conn.executescript(sql_script)
            print(f"Database created and initialized at {db_path}")
    else:
        print(f"Database already exists at {db_path}")
