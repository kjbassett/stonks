from datetime import datetime, time

from config import CONFIG

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class TradingDataAggregation(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingDataAggregation")

    async def update_missing_hourly_aggregations(self, window: int = 3600 * 24):
        earliest_timestamp = int(
            datetime.combine(CONFIG["min_date"], time()).timestamp()
        )  # midnight of earliest date in seconds since epoch
        now = int(datetime.now().timestamp())
        for t in range(earliest_timestamp, now, window):
            if (
                t + window > now
            ):  # final iteration, don't aggregate past the current time
                latest_ts_cte = """
                -- Get the latest timestamp for each company. Used later to make sure we don't aggregate an hour of incomplete data.
                LatestTimestamps AS ( 
                    SELECT company_id, MAX(timestamp) AS latest_timestamp
                    FROM TradingData
                    GROUP BY company_id
                ),"""
                grouped_data_part = f"""
                    JOIN LatestTimestamps lt ON fd.company_id = lt.company_id
                    GROUP BY fd.company_id, fd.date, fd.hour
                    HAVING period_end <= lt.latest_timestamp
                """
            else:
                latest_ts_cte = ""
                grouped_data_part = "GROUP BY fd.company_id, fd.date, fd.hour"

            query = f"""
WITH
    {latest_ts_cte}
-- Get trading data that has not been aggregated yet.
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
    WHERE tda.company_id IS NULL AND td.timestamp >= {t} AND td.timestamp < {t + window}
),
-- Group the filtered data by company, date, and hour.
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
    {grouped_data_part}
),
-- Perform some additional calculations from the grouped data
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
-- Finally, insert the calculated metrics into the TradingDataAggregation table.
INSERT INTO TradingDataAggregation (
    company_id, interval, date, hour, start, end, open, high, low, close, avg_close, cv_close, price_change, avg_volume, cv_volume, row_count
)
SELECT
    company_id, 'hourly', date, hour, start, end, open, high, low, close, avg_close, cv_close, price_change, avg_volume, cv_volume, row_count
FROM CalculatedMetrics;
        """
            current_iter_start = datetime.now().timestamp()
            print(f"Starting aggegation query from {t} to {t + window}, end at {now}")
            await self.db.execute_query(query)
            print(
                f"Time to group data by hour: {int(datetime.now().timestamp() - current_iter_start)} seconds"
            )
            print("Missing hourly aggregations updated.")
