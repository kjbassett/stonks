from typing import List, Tuple

from src.data_access.base_dao import BaseDAO
from src.data_access.db.async_database import AsyncDatabase


class TradingDataAggregation(BaseDAO):
    """DAO for the TradingDataAggregation table."""

    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingDataAggregation")

    async def update_missing_hourly_aggregations(
        self, company_ids: List[int], start: int, end: int
    ) -> None:
        """Update or insert hourly OHLCV aggregations for a batch of companies.

        Args:
            company_ids: IDs of the companies to aggregate.
            start: Unix timestamp for the start of the window (inclusive).
            end: Unix timestamp for the end of the window (exclusive).
        """
        placeholders = ",".join("?" * len(company_ids))
        params: Tuple = (start, end, *company_ids, start, end, *company_ids)
        query = f"""
    WITH RowCounts AS (
        SELECT
            td.company_id,
            DATE(td.timestamp, 'unixepoch') AS date,
            strftime('%H', td.timestamp, 'unixepoch') AS hour,
            COUNT(*) AS row_count
        FROM TradingData td
        WHERE td.timestamp >= ?
            AND td.timestamp < ?
            AND td.company_id IN ({placeholders})
        GROUP BY td.company_id, DATE(td.timestamp, 'unixepoch'), strftime('%H', td.timestamp, 'unixepoch')
    ),
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
        LEFT JOIN RowCounts rc
        ON td.company_id = rc.company_id
        AND DATE(td.timestamp, 'unixepoch') = rc.date
        AND strftime('%H', td.timestamp, 'unixepoch') = rc.hour
        LEFT JOIN TradingDataAggregation tda
        ON td.company_id = tda.company_id
        AND DATE(td.timestamp, 'unixepoch') = tda.date
        AND strftime('%H', td.timestamp, 'unixepoch') = tda.hour
        WHERE
            (td.timestamp >= ? AND td.timestamp < ?)
            AND td.company_id IN ({placeholders})
            -- only include data if there is more data in the aggregation than the currently saved aggregation
            AND (tda.row_count IS NULL or rc.row_count > tda.row_count)
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
        GROUP BY fd.company_id, fd.date, fd.hour
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
    INSERT INTO TradingDataAggregation (
        company_id, interval, date, hour, start, end, open, high, low, close, avg_close, cv_close, price_change, avg_volume, cv_volume, row_count
    )
    SELECT
        company_id, 'hour', date, hour, start, end, open, high, low, close, avg_close, cv_close, price_change, avg_volume, cv_volume, row_count
    FROM CalculatedMetrics 
    WHERE true -- sqlite needs a WHERE clause so it knows that the ON is part of the UPSERT and not part of a table join
    ON CONFLICT (company_id, interval, date, hour) DO UPDATE SET
        start = excluded.start,
        end = excluded.end,
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        avg_close = excluded.avg_close,
        cv_close = excluded.cv_close,
        price_change = excluded.price_change,
        avg_volume = excluded.avg_volume,
        cv_volume = excluded.cv_volume,
        row_count = excluded.row_count;
            """
        await self.db.execute_query(query, params, query_type="INSERT")

    async def clean_data(self, min_timestamp: int) -> None:
        # delete old data and data on companies with disabled ticker types
        query = f"""
        DELETE FROM {self.table_name} 
          WHERE end <=? 
          OR company_id in (
            SELECT Company.
            id 
            FROM Company 
            LEFT JOIN TickerType
            ON Company.ticker_type_id = TickerType.id
            WHERE TickerType.enabled <> 1
          );
        """
        print(query)
        await self.db.execute_query(query, (min_timestamp,), query_type="DELETE")
