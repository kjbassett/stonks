from async_database import AsyncDatabase
from base_model import BaseModel


class TradingData(BaseModel):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "TradingData")

    async def get_data(
        self,
        company_id: int = None,
        symbol: str = None,
        start_timestamp: int = None,
        end_timestamp: int = None,
    ) -> pd.DataFrame:
        # TODO upgrade the get method in base model
        query = f"SELECT * FROM {self.table_name}"

        where_clause = []
        params = []
        if company_id is not None:
            where_clause.append("company_id = ?")
            params.append(company_id)
        if symbol is not None:
            where_clause.append("symbol = ?")
            params.append(symbol)
        if start_timestamp is not None:
            where_clause.append("timestamp >= ?")
            params.append(start_timestamp)
        if end_timestamp is not None:
            where_clause.append("timestamp <= ?")
            params.append(end_timestamp)

        if len(where_clause) > 0:
            query += " WHERE " + " AND ".join(where_clause)
        return await self.db.execute_query(
            query, (start_timestamp, end_timestamp), return_type="DataFrame"
        )
