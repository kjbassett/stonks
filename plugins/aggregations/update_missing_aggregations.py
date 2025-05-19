from data_access.dao_manager import dao_manager
from plugins.decorator import plugin


@plugin()
async def update_missing_aggregations():
    tda = dao_manager.get_dao("TradingDataAggregation")
    await tda.update_missing_hourly_aggregations()
