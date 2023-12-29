from functools import cache


@cache
async def get_or_create_company(
    db, symbol: str = None, name: str = None, sector: str = None, industry: str = None
):
    if not symbol and not name:
        raise ValueError("Please provide either a symbol or a name.")
    company = None
    if symbol:
        company = await db("SELECT * FROM Companies WHERE symbol =?", symbol)
    elif name:
        company = await db("SELECT * FROM Companies WHERE name =?", name)
    if not company and symbol:
        await db.insert("Companies", (symbol, name, sector, industry))
        company = await db("SELECT * FROM Companies WHERE symbol =?", symbol)
    else:
        raise ValueError(
            "No company found, and no symbol provided to create new company."
        )

    return company[0]
