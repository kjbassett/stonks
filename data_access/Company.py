from typing import List, Tuple, Union

import pandas as pd

from .base_dao import BaseDAO
from .db.async_database import AsyncDatabase


class Company(BaseDAO):
    def __init__(self, db: AsyncDatabase):
        super().__init__(db, "News")

    async def get(
        self, columns: list | tuple | str = "*", **kwargs
    ) -> Union[pd.DataFrame, List[Tuple]]:
        if "enabled" not in kwargs:g
            kwargs["enabled"] = 1
        return await super().get(columns, **kwargs)
