import json
import os
import time
from unittest import IsolatedAsyncioTestCase

import numpy as np
from scipy.stats import normaltest
from src.data_access.DataCompiler import construct_target_column
from src.data_access.db.async_database import AsyncDatabase

with open("config.json", "r") as f:
    config = json.load(f)


class TestTargetIsNormallyDistributed(IsolatedAsyncioTestCase):
    async def test_target_is_normal(self):
        return  # errors leave the db connection open. Also takes a long time to run
        name = os.path.join(config["db_folder"], config["db_name"])
        db = AsyncDatabase(name)

        columns, joins, filters = construct_target_column(aggregation_interval="hour", offset=24)
        filters += [f"t.end > {time.time() - 86400 * 28}"]

        columns = ",\n".join(columns)
        joins = "\n".join(joins)
        filters = f"WHERE {' AND '.join(filters)}" if filters else " "

        query = f"""
        SELECT {columns}
        FROM TradingDataAggregation t
        {joins}
        {filters}
        """
        data = await db(query, return_type="DataFrame")
        cutoff1, cutoff2 = np.percentile(data["target"], 1), np.percentile(
            data["target"], 99
        )
        data = data[(data["target"] > cutoff1) & (data["target"] < cutoff2)]
        stat, p_value = normaltest(data["target"])

        print(f"Statistic: {stat}, P-value: {p_value}")
        await db.close()
        assert p_value > 0.05
