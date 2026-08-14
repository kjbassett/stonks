# dao_manager.py
import importlib.util
import logging
import os

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase
from src.utils.market_calendar import earliest_market_time
from src.utils.project_utilities import config
from webrock.decorator import plugin, init, shutdown

_log = logging.getLogger("data_access.dao_manager")


class DAOManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DAOManager, cls).__new__(cls)
            cls._instance.daos = {}
        return cls._instance

    async def initialize(self):
        name = os.path.join(config["db_folder"], config["db_name"])
        self.db = AsyncDatabase(name)

        await self.load_default_daos()
        await self.load_custom_daos()

        _log.debug("Loaded DAOs: %s", list(self.daos))

    async def load_custom_daos(self):
        # Read in any custom data access objects, potentially overwriting the base ones

        # Get folder of this file
        dao_folder = os.path.split(os.path.abspath(__file__))[0]
        relative_path = os.path.relpath(dao_folder, os.getcwd())
        _log.debug("DAO folder relative path: %s", relative_path)
        if relative_path == ".":
            relative_path = ""
        else:
            relative_path = relative_path.replace(os.sep, ".")

        # Check all files in dao_folder
        for dao_file in os.listdir(dao_folder):
            if not dao_file.endswith(".py"):
                continue
            if dao_file in [
                "__init__.py",
                "base_dao.py",
                "dao_manager.py",
            ]:
                continue

            # import_path = f"{relative_path}.{dao_file}".strip(".")
            import_path = os.path.join(dao_folder, dao_file)
            dao_name = os.path.splitext(dao_file)[0]

            spec = importlib.util.spec_from_file_location(dao_name, import_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            _log.info("Loading user-defined DAO: %s", dao_name)
            try:
                dao_class = getattr(module, dao_name)
            except Exception as e:
                _log.error("Failed to import %s: %s", dao_file, e)
                continue
            self.daos[dao_name] = dao_class(self.db)
            await self.daos[dao_name].init2()

    async def load_default_daos(self):
        all_tables = await self.db.get_all_tables()
        # Create base data access objects for all tables
        for table in all_tables:
            self.daos[table] = BaseDAO(self.db, table)
            await self.daos[table].init2()

    def get_dao(self, dao_name):
        return self.daos.get(dao_name)

    @plugin()
    async def clean_data(
        self,
        min_timestamp: int = 0,
    ):
        if min_timestamp == 0:
            min_timestamp = earliest_market_time()
        for table, dao in self.daos.items():
            if hasattr(dao, "clean_data"):
                _log.info("Cleaning data from %s dao", table)
                await dao.clean_data(min_timestamp)


# Singleton instance
dao_manager = DAOManager()


@init
async def initialize_dao_manager():
    await dao_manager.initialize()


@shutdown
async def shutdown_dao_manager():
    await dao_manager.db.close()


@plugin()
async def query_db(sql: str):
    result = await dao_manager.db.execute_query(sql)
    _log.info("Query result: %s", result)
    return result


@plugin()
async def compile_data(
    min_timestamp: int = 1761969600,
    max_timestamp: int = 0,
    max_window: int = 24,
    num_windows: int = 6,
    num_news: int = 0,
    news_history_threshold: int = 0,
    include_target: bool = True,
    target_offset: int = 24,
    include_close_ratio: bool = True,
    include_cv_close_ratio: bool = True,
    include_avg_volume_ratio: bool = True,
    include_cv_volume_ratio: bool = True,
    keep_latest_only: bool = False,
):
    structured_data_dao = dao_manager.get_dao("DataCompiler")
    structured_data = await structured_data_dao.get_data(
        "hour",
        min_timestamp,
        max_timestamp,
        max_window,
        num_windows,
        num_news,
        news_history_threshold,
        include_target,
        target_offset,
        include_close_ratio,
        include_cv_close_ratio,
        include_avg_volume_ratio,
        include_cv_volume_ratio,
        keep_latest_only,
    )
    return structured_data
