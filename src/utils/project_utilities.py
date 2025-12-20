import asyncio
import json

with open("config.json", "r") as f:
    config = json.load(f)


call_limiter = asyncio.Semaphore(32)
