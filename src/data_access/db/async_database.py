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
        self.edit_lock = asyncio.Lock()  # Lock for transaction management
        self.operation_lock = (
            asyncio.Lock()
        )  # lock to access the variable self.active_operations
        self.backup_in_progress = asyncio.Event()
        self.backup_in_progress.set()  # Initially allow operations

    async def connect(self):
        if self.conn is None:
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute("PRAGMA journal_mode = WAL;")
            await self.conn.execute("PRAGMA synchronous = NORMAL;")

    async def close(self):
        if self.conn is not None:
            while self.active_operations > 0:  # Wait for all operations to finish
                await asyncio.sleep(0.5)  # Allow other tasks to run
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
        await self.connect()
        async with self.query_limiter:  # Don't let the number of queries grow to infinity
            await self.backup_in_progress.wait()  # Wait if a backup is in progress
            async with self.operation_lock:  # Lock to prevent simultaneous incrementing of counter
                # Count how many operations are currently active
                self.active_operations += 1
            try:
                # Use transaction lock only for write operations
                # TODO query type = read or write
                if query_type.upper() in ("INSERT", "UPDATE", "DELETE"):
                    async with self.edit_lock:
                        result = await self._execute(
                            query, params, return_type, many, query_type, print_query
                        )
                        await self.conn.commit()  # Commit only for write operations
                else:
                    result = await self._execute(
                        query, params, return_type, many, query_type, print_query
                    )
                return result
            except Exception as e:
                if query_type.upper() in {"INSERT", "UPDATE", "DELETE"}:
                    await self.conn.rollback()  # Rollback on error for write operations
                raise e
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
                await self.execute_query(
                    f"DROP INDEX IF EXISTS {name}", query_type="DELETE"
                )
                await self.execute_query(sql, query_type="INSERT")
                print("✅ Recreated successfully")
            except Exception as e:
                print(f"❌ Failed to recreate index {name}: {e}")
        print("\nAll indices processed.")

    async def backup(self, destination: str = None) -> None:
        """Copy the database file to a timestamped backup.

        Args:
            destination: Directory to write the backup into.  Defaults to the
                same directory as the database file.
        """
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
            await self._backup(destination)
            await self.connect()
            duration = (datetime.datetime.now() - now).total_seconds()
            print(
                f'{now.strftime("%Y-%m-%d %H:%M:%S")} Backup completed after {duration} seconds.'
            )
        finally:
            self.backup_in_progress.set()  # Allow operations after backup

    async def _backup(self, destination: str = None) -> None:
        """Copy the database file to a timestamped path.

        Args:
            destination: Directory to write the backup into.  Defaults to the
                same directory as the database file.
        """
        import os
        import shutil

        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        fname = os.path.basename(self.db_path)
        root, extension = os.path.splitext(fname)
        dest_dir = destination if destination else os.path.dirname(self.db_path)
        os.makedirs(dest_dir, exist_ok=True)
        backup_path = os.path.join(dest_dir, f"{root}_{now}{extension}")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, shutil.copy, self.db_path, backup_path)

    async def optimize(self):
        """
        Performs SQLite maintenance:
        1. Checks fragmentation (freelist / page_count)
        2. VACUUMs only if fragmentation is high
        3. Runs PRAGMA optimize
        """

        await self.connect()

        # Block new operations
        self.backup_in_progress.clear()

        try:
            print("🔧 Starting database optimization...")

            # Wait for active operations to finish
            t0 = time.time()
            while True:
                async with self.operation_lock:
                    if self.active_operations == 0:
                        break
                if time.time() - t0 > 3600:
                    print("⚠️ Timeout waiting for active operations to finish")
                    return
                await asyncio.sleep(0.5)

            async with self.edit_lock:
                # --- Check fragmentation ---
                cursor = await self.conn.execute("PRAGMA freelist_count;")
                freelist_count = (await cursor.fetchone())[0]
                await cursor.close()

                cursor = await self.conn.execute("PRAGMA page_count;")
                page_count = (await cursor.fetchone())[0]
                await cursor.close()

                fragmentation = freelist_count / page_count if page_count > 0 else 0.0

                print(
                    f"📊 Fragmentation: {fragmentation:.2%} "
                    f"({freelist_count}/{page_count} pages)"
                )

                # --- WAL checkpoint before vacuum ---
                cursor = await self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                await cursor.fetchall()
                await cursor.close()

                # --- VACUUM only if needed ---
                if fragmentation > 0.10:
                    print("🧹 Fragmentation high — running VACUUM...")
                    await self.conn.execute("VACUUM;")
                    print("✅ VACUUM completed")
                else:
                    print("✅ Fragmentation acceptable — skipping VACUUM")

                # --- Optimize query planner ---
                print("⚙️ Running PRAGMA optimize...")
                await self.conn.execute("PRAGMA optimize;")
                print("✅ PRAGMA optimize completed")

                await self.conn.commit()

            print("🎉 Database optimization finished")

        finally:
            # Allow operations again
            self.backup_in_progress.set()
