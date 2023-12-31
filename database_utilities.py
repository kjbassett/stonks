from async_lru import alru_cache


@alru_cache(maxsize=500)
async def get_or_create_company(
    db, symbol: str = None, name: str = None, sector: str = None, industry: str = None
):
    if not symbol and not name:
        raise ValueError("Please provide either a symbol or a name.")
    company = None
    if symbol:
        company = await db("SELECT * FROM Companies WHERE symbol =?", (symbol,))
    elif name:
        company = await db("SELECT * FROM Companies WHERE name =?", (name,))

    # if not found, create a new company if symbol is provided
    if not company:
        if symbol:
            await db.insert(
                "Companies", {"symbol": symbol, "name": name, "industry": industry}
            )
            company = await db("SELECT * FROM Companies WHERE symbol =?", (symbol,))
        else:
            raise ValueError(
                "No company found, and no symbol provided to create new company."
            )

    return company[0]


@alru_cache
async def table_exists(db, table: str):
    result = await db(
        f"SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table}';"
    )
    return bool(result)
