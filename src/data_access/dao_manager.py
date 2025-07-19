# dao_manager.py
import os
from datetime import datetime, time
from importlib import import_module

from config import CONFIG
from icecream import ic
from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase
from webrock.decorator import plugin, init


class DAOManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DAOManager, cls).__new__(cls)
            cls._instance.daos = {}
        return cls._instance

    async def initialize(self):
        # TODO module 'config' has no attribute 'config'
        name = os.path.join(CONFIG["db_folder"], CONFIG["db_name"])
        self.db = AsyncDatabase(name)

        await self.load_default_daos()
        await self.load_custom_daos()

        ic(self.daos)
        # TODO DELETE ME after webrock gets shutdown code functionality
        await self.db.close()

    async def load_custom_daos(self):
        # Read in any custom data access objects, potentially overwriting the base ones

        # Get folder of this file
        dao_folder = os.path.split(os.path.abspath(__file__))[0]
        relative_path = os.path.relpath(dao_folder, os.getcwd())
        print(f"Relative path: {relative_path}")
        if relative_path == ".":
            relative_path = ""
        else:
            relative_path = relative_path.replace(os.sep, ".")

        # Check all files in dao_folder
        for dao_file in os.listdir(dao_folder):
            dao_file = os.path.splitext(dao_file)[0]
            if not dao_file.endswith(".py") or dao_file in [
                "__init__.py",
                "base_dao.py",
                "dao_manager.py",
            ]:
                continue
            dao_name = dao_file.split(".")[0]
            import_path = f"{relative_path}.{dao_file}".strip(".")

            print(f"Loading DAO: {import_path}")
            try:
                dao_class = getattr(import_module(import_path), dao_name)
            except Exception as e:
                print(f"Failed to import {dao_file}: {e}")
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
        min_timestamp: int = datetime.combine(CONFIG["min_date"], time()).timestamp(),
    ):
        for table, dao in self.daos.items():
            if hasattr(dao, "clean_data"):
                print(f"Cleaning data from {table} dao...")
                await dao.clean_data(min_timestamp)


# Singleton instance
dao_manager = DAOManager()


@init
async def initialize_dao_manager():
    await dao_manager.initialize()
