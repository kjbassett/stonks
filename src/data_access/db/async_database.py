import asyncio
import datetime
import logging
import pathlib
import time
from typing import Tuple, Union, List

import aiosqlite
import pandas as pd
from async_lru import alru_cache

_log = logging.getLogger("data_access.db")

_READ_POOL_SIZE = 4
_SLOW_QUERY_THRESHOLD_S = 30.0


class AsyncDatabase:
    def __init__(self, db_path: str, read_pool_size: int = _READ_POOL_SIZE):
        self.db_path = db_path
        self.conn = None  # single writer connection
        self.read_pool_size = read_pool_size
        self._read_pool: asyncio.Queue = asyncio.Queue()
        self._read_conns: List[aiosqlite.Connection] = []
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
        if not self._read_conns:
            # WAL mode is a database-level setting (persisted in the file header
            # by the writer above), so read-only connections pick it up automatically
            # — they just need mode=ro so SQLite lets a would-be-writer PRAGMA no-op
            # instead of erroring, and so multiple of these can read concurrently
            # with each other and with the single writer.
            ro_uri = pathlib.Path(self.db_path).absolute().as_uri() + "?mode=ro"
            for _ in range(self.read_pool_size):
                rconn = await aiosqlite.connect(ro_uri, uri=True)
                self._read_conns.append(rconn)
                await self._read_pool.put(rconn)

    async def close(self):
        if self.conn is not None:
            while self.active_operations > 0:  # Wait for all operations to finish
                await asyncio.sleep(0.5)  # Allow other tasks to run
            await self.conn.close()
            self.conn = None
        for rconn in self._read_conns:
            await rconn.close()
        self._read_conns = []
        self._read_pool = asyncio.Queue()

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
            t0 = time.monotonic()
            try:
                # Use transaction lock only for write operations
                # TODO query type = read or write
                if query_type.upper() in ("INSERT", "UPDATE", "DELETE"):
                    async with self.edit_lock:
                        result = await self._execute(
                            query, params, return_type, many, query_type, print_query,
                            conn=self.conn,
                        )
                        await self.conn.commit()  # Commit only for write operations
                else:
                    rconn = await self._read_pool.get()
                    try:
                        result = await self._execute(
                            query, params, return_type, many, query_type, print_query,
                            conn=rconn,
                        )
                    finally:
                        await self._read_pool.put(rconn)
                return result
            except Exception as e:
                if query_type.upper() in {"INSERT", "UPDATE", "DELETE"}:
                    await self.conn.rollback()  # Rollback on error for write operations
                raise e
            finally:
                elapsed = time.monotonic() - t0
                if elapsed > _SLOW_QUERY_THRESHOLD_S:
                    preview = " ".join(query.split())[:200]
                    _log.warning(
                        "Slow query (%.1fs, type=%s): %s",
                        elapsed, query_type or "SELECT", preview,
                    )
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
        conn=None,
    ) -> Union[int, pd.DataFrame, List[Tuple]]:
        conn = conn if conn is not None else self.conn
        if print_query:
            _log.debug("Executing query:\n%s\nparams: %s", query, params)

        if many:  # TODO detect this automatically somehow
            cursor = await conn.executemany(query, params)
        else:
            cursor = await conn.execute(query, params)

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
            _log.info("No user-defined indices found")
            return

        for name, sql in indices:
            _log.info("Recreating index: %s", name)
            try:
                await self.execute_query(
                    f"DROP INDEX IF EXISTS {name}", query_type="DELETE"
                )
                await self.execute_query(sql, query_type="INSERT")
                _log.info("Recreated index %s", name)
            except Exception as e:
                _log.error("Failed to recreate index %s: %s", name, e)
        _log.info("All indices processed")

    async def backup(self, destination: str = None) -> None:
        """Copy the database file to a timestamped backup.

        Args:
            destination: Directory to write the backup into.  Defaults to the
                same directory as the database file.
        """
        self.backup_in_progress.clear()  # Block new operations
        try:
            now = datetime.datetime.now()
            _log.info("Backup waiting for all db operations to finish")
            t = time.time()
            while time.time() - t < 3600 * 3:
                async with self.operation_lock:
                    if self.active_operations == 0:
                        break
                await asyncio.sleep(0.5)
            else:
                _log.warning("Timed out waiting for db operations to finish — backup aborted")
                return
            now = datetime.datetime.now()
            _log.info("All db operations finished — starting backup")
            await self.close()
            await self._backup(destination)
            await self.connect()
            duration = (datetime.datetime.now() - now).total_seconds()
            _log.info("Backup completed in %.1f seconds", duration)
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
            _log.info("Starting database optimization")

            # Wait for active operations to finish
            t0 = time.time()
            while True:
                async with self.operation_lock:
                    if self.active_operations == 0:
                        break
                if time.time() - t0 > 3600:
                    _log.warning("Timeout waiting for active operations — optimization aborted")
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
                _log.info(
                    "Fragmentation: %.2f%% (%d/%d pages)",
                    fragmentation * 100, freelist_count, page_count,
                )

                # --- WAL checkpoint before vacuum ---
                cursor = await self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                await cursor.fetchall()
                await cursor.close()

                # --- VACUUM only if needed ---
                if fragmentation > 0.10:
                    _log.info("Fragmentation high — running VACUUM")
                    await self.conn.execute("VACUUM;")
                    _log.info("VACUUM completed")
                else:
                    _log.info("Fragmentation acceptable — skipping VACUUM")

                # --- Optimize query planner ---
                _log.info("Running PRAGMA optimize")
                await self.conn.execute("PRAGMA optimize;")
                _log.info("PRAGMA optimize completed")

                await self.conn.commit()

            _log.info("Database optimization finished")

        finally:
            # Allow operations again
            self.backup_in_progress.set()
