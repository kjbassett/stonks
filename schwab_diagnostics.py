"""
Schwab API diagnostic script.

Calls every read-only endpoint and logs the full response for review.
No orders are placed or cancelled.

Usage:
    python schwab_diagnostics.py
Output goes to console AND logs/schwab_diagnostics.log
"""

import asyncio
import json
import logging
import sys
from datetime import date, timedelta
from logging.handlers import RotatingFileHandler

# Load .env before anything else (project_utilities will also do it,
# but being explicit here keeps the script self-contained)
from dotenv import load_dotenv
load_dotenv(override=False)


# ---------------------------------------------------------------------------
# Logging: console + rotating file
# ---------------------------------------------------------------------------

_LOG_FILE = "logs/schwab_diagnostics.log"

_formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

_file_handler = RotatingFileHandler(_LOG_FILE, maxBytes=1_000_000, backupCount=3)
_file_handler.setFormatter(_formatter)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
_log = logging.getLogger("schwab_diagnostics")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    """Print a visible section divider."""
    _log.info("")
    _log.info("=" * 60)
    _log.info(f"  {title}")
    _log.info("=" * 60)


def _dump(label: str, data: object) -> None:
    """Log an API response as pretty-printed JSON."""
    _log.info(f"{label}:\n{json.dumps(data, indent=2, default=str)}")


async def _run_check(label: str, coro) -> tuple[bool, object]:
    """Await a coroutine, log the result, return (passed, result)."""
    try:
        result = await coro
        _dump(label, result)
        return True, result
    except Exception as exc:
        _log.error(f"{label} FAILED: {exc}", exc_info=True)
        return False, None


# ---------------------------------------------------------------------------
# Diagnostic sections
# ---------------------------------------------------------------------------

async def _check_auth() -> "SchwabClient | None":
    """Verify token loading and return an authenticated client."""
    _section("AUTH — token loading")
    from src.trading.brokers.schwab_auth import SchwabAuth
    try:
        client = await SchwabAuth().get_client()
        _log.info("Token loaded and valid.")
        return client
    except Exception as exc:
        _log.error(f"Auth failed — cannot continue: {exc}", exc_info=True)
        return None


async def _check_account_endpoints(client, hash_value: str) -> None:
    """Run all account-level read endpoints."""
    today = date.today().isoformat()
    month_ago = (date.today() - timedelta(days=30)).isoformat()

    checks = [
        ("get_accounts()",
         client.get_accounts()),
        ("get_account() — balances only",
         client.get_account(hash_value, fields="")),
        ("get_account() — with positions",
         client.get_account(hash_value, fields="positions")),
        ("get_account_equity()",
         client.get_account_equity(hash_value)),
        ("get_cash_available()",
         client.get_cash_available(hash_value)),
        ("get_positions()",
         client.get_positions(hash_value)),
        ("get_orders() — last 30 days",
         client.get_orders(
             hash_value,
             from_entered_time=f"{month_ago}T00:00:00.000Z",
             to_entered_time=f"{today}T23:59:59.999Z",
         )),
    ]

    results = []
    for label, coro in checks:
        _section(f"ACCOUNTS — {label}")
        passed, _ = await _run_check(label, coro)
        results.append((label, passed))

    return results


async def _check_market_data(client) -> list:
    """Run all market data read endpoints."""
    symbols = ["AAPL", "MSFT", "SPY", "QQQ"]

    checks = [
        (f"get_quotes({symbols})",
         client.get_quotes(symbols)),
    ]

    results = []
    for label, coro in checks:
        _section(f"MARKET DATA — {label}")
        passed, _ = await _run_check(label, coro)
        results.append((label, passed))

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Run all diagnostics and print a pass/fail summary."""
    _log.info("Schwab API Diagnostics — starting")

    client = await _check_auth()
    if client is None:
        _log.error("Aborting: could not obtain a valid token.")
        return

    # Discover account hash value first — needed for account endpoints
    _section("ACCOUNTS — get_account_numbers()")
    passed, entries = await _run_check("get_account_numbers()", client.get_account_numbers())
    account_results = [("get_account_numbers()", passed)]

    hash_value = None
    if passed and entries:
        hash_value = entries[0]["hashValue"]
        _log.info(f"Using hashValue: {hash_value}")

    if hash_value:
        account_results += await _check_account_endpoints(client, hash_value)

    market_results = await _check_market_data(client)
    await client.aclose()

    # Summary
    all_results = account_results + market_results
    _section("SUMMARY")
    passed_count = sum(1 for _, ok in all_results if ok)
    for label, ok in all_results:
        status = "PASS" if ok else "FAIL"
        _log.info(f"  [{status}]  {label}")
    _log.info("")
    _log.info(f"  {passed_count}/{len(all_results)} checks passed.")
    _log.info(f"  Full output saved to: {_LOG_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
