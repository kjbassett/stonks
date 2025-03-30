from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class TradingDataAggregation(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")

    async def update_missing_hourly_aggregations(self):
        import time
        t = time.time()
        query = """
WITH LatestTimestamps AS (
    SELECT company_id, MAX(timestamp) AS latest_timestamp
    FROM TradingData
    GROUP BY company_id
),
FilteredData AS (
    SELECT
        td.company_id,
        DATE(td.timestamp, 'unixepoch') AS date, -- TODO convert to eastern time zone maybe
        strftime('%H', td.timestamp, 'unixepoch') AS hour,
        td.timestamp,
        td.open,
        td.high,
        td.low,
        td.close,
        td.volume
    FROM TradingData td
    LEFT JOIN TradingDataAggregation tda
    ON td.company_id = tda.company_id
    AND DATE(td.timestamp, 'unixepoch') = tda.date
    AND strftime('%H', td.timestamp, 'unixepoch') = tda.hour
    WHERE tda.company_id IS NULL
),
GroupedData AS (
    SELECT
        fd.company_id,
        fd.date,
        fd.hour,
        MIN(fd.timestamp) AS start,
        MAX(fd.timestamp) AS end,
        (SELECT fd2.open FROM FilteredData fd2 WHERE fd2.company_id = fd.company_id AND fd2.date = fd.date AND fd2.hour = fd.hour ORDER BY fd2.timestamp ASC LIMIT 1) AS open,
        MAX(fd.high) AS high,
        MIN(fd.low) AS low,
        (SELECT fd2.close FROM FilteredData fd2 WHERE fd2.company_id = fd.company_id AND fd2.date = fd.date AND fd2.hour = fd.hour ORDER BY fd2.timestamp DESC LIMIT 1) AS close,
        AVG(fd.close) AS avg_close,
        AVG(fd.volume) AS avg_volume,
        AVG(fd.close * fd.close) as avg_sq_close,
        AVG(fd.volume * fd.volume) AS avg_sq_volume,
        COUNT(*) AS row_count,
        CAST(strftime('%s', fd.date || ' ' || fd.hour || ':59:59') AS INTEGER) AS period_end
    FROM FilteredData fd
    JOIN LatestTimestamps lt ON fd.company_id = lt.company_id
    GROUP BY fd.company_id, fd.date, fd.hour
    HAVING period_end <= lt.latest_timestamp
),
CalculatedMetrics AS (
    SELECT
        gd.company_id,
        gd.date,
        gd.hour,
        gd.start,
        gd.end,
        gd.open,
        gd.high,
        gd.low,
        gd.close,
        gd.avg_close,
        gd.avg_volume,
        gd.row_count,
        (gd.close - gd.open) / gd.open AS price_change,
        (CASE WHEN gd.avg_close != 0 THEN 
            SQRT(gd.avg_sq_close - gd.avg_close * gd.avg_close) / gd.avg_close 
        ELSE 0 END) AS cv_close,
        (CASE WHEN gd.avg_volume != 0 THEN 
            SQRT(gd.avg_sq_volume - gd.avg_volume * gd.avg_volume) / gd.avg_volume
        ELSE 0 END) AS cv_volume
    FROM GroupedData gd
)
INSERT INTO TradingDataAggregation (
    company_id, interval, date, hour, start, end, open, high, low, close, avg_close, cv_close, price_change, avg_volume, cv_volume, row_count
)
SELECT
    company_id, 'hourly', date, hour, start, end, open, high, low, close, avg_close, cv_close, price_change, avg_volume, cv_volume, row_count
FROM CalculatedMetrics;
        """
        await self.db.execute_query(query)
        print(f"Time taken: {time.time() - t} seconds")
        print("Missing hourly aggregations updated.")


