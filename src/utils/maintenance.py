from src.data_access.dao_manager import dao_manager
from src.utils.market_calendar import earliest_market_time
from webrock.decorator import plugin


@plugin()
async def clean_data():
    await dao_manager.clean_data(earliest_market_time())


@plugin()
async def backup_db():
    await dao_manager.db.backup()


@plugin()
async def recreate_indices():
    await dao_manager.db.recreate_all_indices()


@plugin()
async def optimize_db():
    result = await dao_manager.db.optimize()
    print(result)
    return result
