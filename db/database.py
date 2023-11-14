import os
import sqlite3
from typing import List, Tuple, Union, Dict, Any
import pandas as pd


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def __call__(
            self,
            query: str,
            params: Tuple = (),
            return_type: str = 'list'
    ) -> Union[pd.DataFrame, List[Tuple]]:
        """Make the Database object callable to execute queries."""
        return self.execute_query(query, params, return_type)

    def _connect(self):
        """Create a database connection."""
        return sqlite3.connect(self.db_path)

    def execute_query(
            self,
            query: str,
            params: Union[Tuple, List] = (),
            return_type: str = 'list'
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
        with self._connect() as conn:
            cursor = conn.cursor()

            if return_type == 'DataFrame':
                cursor.execute(query, params)
                # Use Pandas to read the results into a DataFrame
                result = pd.read_sql_query(query, conn, params=params)
            elif return_type == 'list':
                # Check if params is a list of tuples for executemany
                if isinstance(params, list) and all(isinstance(p, tuple) for p in params):
                    cursor.executemany(query, params)
                    result = None  # No direct result for executemany
                else:
                    cursor.execute(query, params)
                    result = cursor.fetchall()
            else:
                raise ValueError("Invalid return_type. Use 'DataFrame' or 'List'.")

            conn.commit()

        return result

    def insert(self, table: str, data: Union[Dict[str, Any], pd.DataFrame], skip_existing: bool = True):
        """Insert a record or records into the database."""
        if isinstance(data, dict):
            self._insert_single_record(table, data, skip_existing)
        elif isinstance(data, pd.DataFrame):
            self._insert_multiple_records(table, data, skip_existing)
        else:
            raise ValueError("Unsupported data type. Use a dictionary or a DataFrame.")

    def _insert_single_record(self, table: str, data: Dict[str, Any], skip_existing: bool = True):
        """Insert a single record into the database."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT {'OR IGNORE ' if skip_existing else ''}INTO {table} ({columns}) VALUES ({placeholders});"
        params = tuple(data.values())
        self.execute_query(query, params)

    def _insert_multiple_records(self, table: str, data: pd.DataFrame, skip_existing: bool = True):
        """Insert multiple records from a DataFrame into the database."""
        columns = ', '.join(data.columns)
        placeholders = ', '.join(['?'] * len(data.columns))
        query = f"INSERT {'OR IGNORE ' if skip_existing else ''}INTO {table} ({columns}) VALUES ({placeholders});"
        params = [tuple(row) for row in data.values]
        self.execute_query(query, params)


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
    pass

    # Examples
    from config import CONFIG
    name = CONFIG['db_folder'] + CONFIG['db_name']
    # create_database_if_not_exists(name)
    db = Database(name)
    # db.insert('Companies', {'name': 'Test Company2', 'symbol': 'TEST2'})
    # db.insert('Companies', {'name': 'Test Company2', 'symbol': 'TEST2'})
    # db.insert('Companies', pd.DataFrame({'name': ['Test Company3', 'Test Company 4'], 'symbol': ['TEST3', 'TEST4']}))
    # print(db('SELECT * FROM Companies', return_type='DataFrame'))
    # db('DELETE FROM Companies')

    # # tickers.csv import
    # tickers = pd.read_csv('../tickers.csv')
    # tickers.columns = ['name', 'symbol']
    # db.insert('Companies', tickers)
    # print(db('SELECT * FROM Companies', return_type='DataFrame'))

    # # parquet file import script
    # import os
    # folder = r'C:\Users\Ken\Dropbox\Programming\Stonks\Data'
    # for file in os.listdir(folder):
    #     if file.endswith('.parquet'):
    #         symbol = file.split('.')[0]
    #         print(symbol)
    #
    #         path = os.path.join(folder, file)
    #         df = pd.read_parquet(path)
    #         company_id = db("SELECT id FROM Companies WHERE symbol = ?", (symbol,))
    #         if not company_id:
    #             db.insert('Companies', {'symbol': symbol})
    #             company_id = db("SELECT id FROM Companies WHERE symbol = ?", (symbol,))
    #         company_id = company_id[0][0]
    #         df['company_id'] = company_id
    #         df['timestamp'] = df['timestamp'] // 1000
    #         db.insert('TradingData', df)
    #         dbdf = db('SELECT * FROM TradingData WHERE company_id = ?', (company_id,), return_type='DataFrame')
    #         print(len(df.index) - len(dbdf.index))

    print(db('SELECT * FROM TradingDataGaps;'))
    db('DELETE FROM TradingDataGaps;')
    print(db('SELECT * FROM TradingDataGaps;'))
    print(db('SELECT COUNT(timestamp) from TradingData WHERE timestamp > 1000000000000', return_type='DataFrame'))
    db('DELETE FROM TradingData WHERE timestamp > 1000000000000')
    print(db('SELECT COUNT(timestamp) from TradingData WHERE timestamp > 1000000000000', return_type='DataFrame'))

    print(db('SELECT * FROM Companies WHERE symbol = \'KE\';', return_type='DataFrame'))
    print(db('SELECT * FROM TradingData WHERE timestamp <= 1636986600 and company_id = (SELECT id FROM Companies WHERE symbol = \'KE\');', return_type='DataFrame'))