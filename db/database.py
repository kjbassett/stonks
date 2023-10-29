import os
import sqlite3
from config import CONFIG
from typing import List, Tuple


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self):
        """Create a database connection."""
        return sqlite3.connect(self.db_path)

    def execute_query(self, query: str, params: Tuple = ()):
        """Execute a single SQL query."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.fetchall()

    def insert(self, table: str, column_values: dict):
        """Insert a record into the database."""
        columns = ', '.join(column_values.keys())
        placeholders = ', '.join(['?'] * len(column_values))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
        self.execute_query(query, tuple(column_values.values()))

    def select(self, table: str, columns: List[str] = None):
        """Select a record from the database."""
        if columns is None:
            columns = '*'
        query = f"SELECT {columns} FROM {table};"
        return self.execute_query(query)


def create_database_if_not_exists(db_path: str, schema_path: str = 'create_db.sql'):
    # Check if the database already exists
    db_exists = os.path.exists(db_path)

    if not db_exists:
        # Establish a new database connection
        with sqlite3.connect(db_path) as conn:
            # Open the schema file to read the SQL commands
            with open(schema_path, 'r') as f:
                sql_script = f.read()

            # Execute the SQL script
            conn.executescript(sql_script)
            print(f"Database created and initialized at {db_path}")
    else:
        print(f"Database already exists at {db_path}")


if __name__ == '__main__':
    create_database_if_not_exists(CONFIG['db_folder'] + CONFIG['db_name'])
