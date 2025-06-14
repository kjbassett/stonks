import asyncio
import datetime
import time
from typing import Tuple, Union, List

import aiosqlite
import pandas as pd
from async_lru import alru_cache
from icecream import ic


class AsyncDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.query_limiter = asyncio.Semaphore(32)  # Limit concurrent tasks
        self.active_operations = 0
        self.operation_lock = (
            asyncio.Lock()
        )  # For when this object needs to do ONE thing
        self.backup_in_progress = asyncio.Event()
        self.backup_in_progress.set()  # Initially allow operations

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
        print_query=False,
    ) -> Union[int, pd.DataFrame, List[Tuple]]:
        async with self.query_limiter:
            await self.backup_in_progress.wait()  # Wait if a backup is in progress
            async with self.operation_lock:  # Lock to prevent simultaneous incrementing of counter
                self.active_operations += (
                    1  # Count how many operations are currently active
                )
            try:
                return await self._execute(
                    query, params, return_type, many, query_type, print_query
                )
            finally:
                async with self.operation_lock:
                    self.active_operations -= 1

    async def _execute(
        self,
        query: str,
        params: Union[Tuple, List] = (),
        return_type: str = "list",
        many=False,
        query_type="",
        print_query=False,
    ) -> Union[int, pd.DataFrame, List[Tuple]]:
        if print_query:
            print(f"Executing query:\n{query}")
            ic(params)
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

    async def recreate_all_indices(self):
        # Get all user-defined index names and their creation SQL
        indices = await self.execute_query(
            """
            SELECT name, sql 
            FROM sqlite_master 
            WHERE type = 'index' 
              AND sql IS NOT NULL
            """
        )

        if not indices:
            print("No user-defined indices found.")
            return

        for name, sql in indices:
            print(f"\nRecreating index: {name}")
            print(f"Original SQL: {sql}")

            try:
                await self.execute_query(f"DROP INDEX IF EXISTS {name}")
                await self.execute_query(sql)
                print("✅ Recreated successfully")
            except Exception as e:
                print(f"❌ Failed to recreate index {name}: {e}")
        print("\nAll indices processed.")

    async def backup(self):
        self.backup_in_progress.clear()  # Block new operations
        try:
            now = datetime.datetime.now()
            print(
                f'{now.strftime("%Y-%m-%d %H:%M:%S")} Backup waiting for all db operations to finish...'
            )
            t = time.time()
            while time.time() - t < 3600 * 3:
                async with self.operation_lock:  # lock to access self.active_operations
                    if self.active_operations == 0:
                        break
            else:
                print(
                    f'{now.strftime("%Y-%m-%d %H:%M:%S")} Waiting for all db operations to finish timed out.'
                )
                return
            now = datetime.datetime.now()
            print(
                f'{now.strftime("%Y-%m-%d %H:%M:%S")} All db operations finished. Starting backup...'
            )
            await self.close()
            await self._backup()
            await self.connect()
            duration = (datetime.datetime.now() - now).total_seconds()
            print(
                f'{now.strftime("%Y-%m-%d %H:%M:%S")} Backup completed after {duration} seconds.'
            )
        finally:
            self.backup_in_progress.set()  # Allow operations after backup

    async def _backup(self):
        import shutil
        import os

        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        root, extension = os.path.splitext(self.db_path)
        backup_path = f"{root}_{now}{extension}"
        # Get the current event loop
        loop = asyncio.get_running_loop()

        # Run the blocking function in a thread pool and get a Future object
        await loop.run_in_executor(None, shutil.copy, self.db_path, backup_path)
