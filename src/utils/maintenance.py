from src.data_access.dao_manager import dao_manager
from src.utils.market_calendar import earliest_market_time
from webrock.decorator import plugin


@plugin()
async def clean_data():
    await dao_manager.clean_data(earliest_market_time())


@plugin(backup_path={"ui_element": "textbox", "default": "H:/databases"})
async def backup_db(backup_path: str = "H:/databases") -> None:
    """Back up the database to a timestamped file in backup_path.

    Args:
        backup_path: Directory to write the backup into. Defaults to H:/databases.
    """
    await dao_manager.db.backup(backup_path)


@plugin()
async def recreate_indices():
    await dao_manager.db.recreate_all_indices()


@plugin()
async def optimize_db():
    result = await dao_manager.db.optimize()
    return result
