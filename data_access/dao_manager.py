# daos_singleton.py
import os
from importlib import import_module

from config import CONFIG

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
            if f.endswith(".py")
        ]

        all_tables = await self.db.get_all_tables()
        for table in all_tables:
            if table not in non_base_daos:
                self.daos[table] = BaseDAO(self.db, table)
            else:
                dao_class = getattr(import_module(f"data_access.{table}"), table)
                self.daos[table] = dao_class(self.db)

    def get_dao(self, dao_name):
        return self.daos.get(dao_name)


# Singleton instance
dao_manager = DAOManager()
