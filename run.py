import asyncio

from app import create_app

app = asyncio.run(create_app())
