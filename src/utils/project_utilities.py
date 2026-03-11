import asyncio
import json

from dotenv import load_dotenv

load_dotenv(override=False)

with open("config.json", "r") as f:
    config = json.load(f)

call_limiter = asyncio.Semaphore(32)
