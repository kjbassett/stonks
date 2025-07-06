from webrock.decorator import plugin

from src.data_access.dao_manager import dao_manager


@plugin()
async def update_missing_aggregations():
    tda = dao_manager.get_dao("TradingDataAggregation")
    await tda.update_missing_hourly_aggregations()
