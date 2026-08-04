import asyncio
import json

import urllib3
from dotenv import load_dotenv
from massive import RESTClient

load_dotenv(override=False)

with open("config.json", "r") as f:
    config = json.load(f)

call_limiter = asyncio.Semaphore(32)

# Set up file+console logging as early as possible; safe to call multiple times (no-op after first).
from src.utils.log_config import setup_logging  # noqa: E402
setup_logging()


def make_rest_client(concurrency: int = 10) -> RESTClient:
    """Create a Massive RESTClient sized for concurrent use.

    Patches the default urllib3 PoolManager (maxsize=1) to match the
    caller's concurrency level so connections are reused instead of
    discarded, eliminating 'Connection pool is full' warnings.

    Args:
        concurrency: Number of concurrent requests the caller will issue.

    Returns:
        Configured RESTClient instance.
    """
    c = RESTClient(config["polygon_io"], read_timeout=30.0)
    c.client = urllib3.PoolManager(
        num_pools=10,
        maxsize=concurrency,
        headers=c.headers,
        **c.client.connection_pool_kw,
    )
    return c
