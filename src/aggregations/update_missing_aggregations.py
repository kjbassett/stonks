import asyncio
from datetime import datetime

from src.utils.market_calendar import earliest_market_time
from webrock.decorator import plugin


@plugin()
async def update_hourly_aggregations(companies: str = None, hours_per_query: int = 24):
    # TODO src/aggregations folder appears before the data_access folder, so dao_manager had no daos until I put dao manager here.
    from src.data_access.dao_manager import dao_manager

    cmp = dao_manager.get_dao("Company")
    window = 3600 * hours_per_query
    tda = dao_manager.get_dao("TradingDataAggregation")
    earliest_timestamp = earliest_market_time()
    if companies:
        companies = await cmp.get(symbol=companies)
    else:
        companies = await cmp.get()
    n_cpy = len(companies)
    tasks = []
    for c, cpy in companies.iterrows():
        print(f"Company {c + 1}/{n_cpy}, {cpy['symbol']}")
        current_iter_start = int(datetime.now().timestamp())
        windows = range(earliest_timestamp, current_iter_start, window)
        for t in windows:
            task = asyncio.create_task(
                tda.update_missing_hourly_aggregations(cpy["id"], t, t + window)
            )
            tasks.append(task)
        await asyncio.gather(*tasks)
        print(f"Finished aggregation query for company {cpy['symbol']}")
        print(f"Time taken: {datetime.now().timestamp() - current_iter_start} seconds")
        tasks.clear()
    print("Missing hourly aggregations updated.")
