# daos_singleton.py
import os
from importlib import import_module

from config import CONFIG
from icecream import ic

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class DAOManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DAOManager, cls).__new__(cls)
            cls._instance.daos = {}
        return cls._instance

    async def initialize(self):
        name = CONFIG["db_folder"] + CONFIG["db_name"]
        self.db = AsyncDatabase(name)

        non_base_daos = [
            os.path.splitext(f)[0]
            for f in os.listdir("data_access")
            if f.endswith(".py") and f not in ["base_dao.py", "dao_manager.py"]
        ]

        all_tables = await self.db.get_all_tables()
        # Create base data access objects for all tables
        for table in all_tables:
            self.daos[table] = BaseDAO(self.db, table)
        # Read in any custom data access objects, potentially overwriting the base ones
        for dao in non_base_daos:
            dao_class = getattr(import_module(f"data_access.{dao}"), dao)
            self.daos[dao] = dao_class(self.db)

        ic(self.daos)

    def get_dao(self, dao_name):
        return self.daos.get(dao_name)


# Singleton instance
dao_manager = DAOManager()
