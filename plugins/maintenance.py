from datetime import datetime, time

from config import CONFIG
from data_access.dao_manager import dao_manager
from plugins.decorator import plugin


@plugin()
async def clean_data():
    await dao_manager.clean_data(
        datetime.combine(CONFIG["min_date"], time()).timestamp()
    )


@plugin()
async def backup_db():
    await dao_manager.db.backup()


@plugin()
async def recreate_indices():
    await dao_manager.db.recreate_all_indices()
