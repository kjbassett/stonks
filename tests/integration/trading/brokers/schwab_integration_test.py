"""
Integration tests for Schwab API connectivity.

These tests call the REAL Schwab API — no mocks. They are read-only and
safe to run at any time (no orders are placed or cancelled).

Requirements:
  - SCHWAB_APP_KEY and SCHWAB_APP_SECRET environment variables set
  - Valid schwab_tokens.json present (run SchwabAuth().authorize() first)

Run with:
    python -m pytest tests/integration/ -v

All tests are skipped automatically if credentials are not configured.
"""

import datetime
import os
import unittest

from dotenv import load_dotenv

load_dotenv(override=False)

_SCHWAB_CONFIGURED = bool(
    os.environ.get("SCHWAB_APP_KEY") and os.environ.get("SCHWAB_APP_SECRET")
)
_SKIP_REASON = (
    "Schwab credentials not configured. "
    "Set SCHWAB_APP_KEY and SCHWAB_APP_SECRET environment variables."
)


@unittest.skipUnless(_SCHWAB_CONFIGURED, _SKIP_REASON)
class TestSchwabAuthIntegration(unittest.IsolatedAsyncioTestCase):
    """Verify token loading and client construction against the real API."""

    async def test_get_client_succeeds(self):
        """SchwabAuth.get_client() returns a usable SchwabClient."""
        from src.trading.brokers.schwab_auth import SchwabAuth

        client = await SchwabAuth().get_client()
        self.assertIsNotNone(client)
        await client.aclose()

    async def test_token_not_expired_after_get_client(self):
        """Tokens are valid (or refreshed) when get_client() returns."""
        import json
        import time
        from src.trading.brokers.schwab_auth import SchwabAuth, _REFRESH_BUFFER_SECONDS
        from src.utils.project_utilities import config

        client = await SchwabAuth().get_client()
        await client.aclose()

        with open(config["schwab"]["token_file"]) as f:
            tokens = json.load(f)

        self.assertIn("expires_at", tokens)
        self.assertGreater(
            tokens["expires_at"] - time.time(),
            _REFRESH_BUFFER_SECONDS,
            "Token should have more than the refresh buffer remaining after get_client()",
        )


@unittest.skipUnless(_SCHWAB_CONFIGURED, _SKIP_REASON)
class TestSchwabClientAccountsIntegration(unittest.IsolatedAsyncioTestCase):
    """Verify account-related Schwab API endpoints."""

    async def asyncSetUp(self) -> None:
        from src.trading.brokers.schwab_auth import SchwabAuth

        self.client = await SchwabAuth().get_client()
        entries = await self.client.get_account_numbers()
        self.hash_value: str = entries[0]["hashValue"]

    async def asyncTearDown(self) -> None:
        await self.client.aclose()

    async def test_get_account_numbers_returns_hash_value(self):
        """/accounts/accountNumbers returns at least one entry with a hashValue."""
        entries = await self.client.get_account_numbers()

        self.assertIsInstance(entries, list)
        self.assertGreater(len(entries), 0, "Expected at least one linked account")
        self.assertIn("hashValue", entries[0])
        self.assertTrue(entries[0]["hashValue"], "hashValue should be non-empty")

    async def test_get_accounts_returns_securities_account(self):
        """/accounts returns a list with securitiesAccount structure."""
        accounts = await self.client.get_accounts()

        self.assertIsInstance(accounts, list)
        self.assertGreater(len(accounts), 0)
        self.assertIn("securitiesAccount", accounts[0])

    async def test_get_account_equity_is_positive(self):
        """Liquidation value is a positive number."""
        equity = await self.client.get_account_equity(self.hash_value)

        self.assertIsInstance(equity, float)
        self.assertGreater(equity, 0, "Account equity should be positive")

    async def test_get_cash_available_is_non_negative(self):
        """Cash available for trading is >= 0."""
        cash = await self.client.get_cash_available(self.hash_value)

        self.assertIsInstance(cash, float)
        self.assertGreaterEqual(cash, 0)

    async def test_get_positions_returns_list(self):
        """get_positions() returns a list (may be empty if fully in cash)."""
        positions = await self.client.get_positions(self.hash_value)

        self.assertIsInstance(positions, list)
        for pos in positions:
            self.assertIn("instrument", pos)
            self.assertIn("symbol", pos["instrument"])

    async def test_get_orders_returns_list(self):
        """get_orders() for today returns a list."""
        today = datetime.date.today().isoformat()
        orders = await self.client.get_orders(
            self.hash_value,
            from_entered_time=f"{today}T00:00:00.000Z",
            to_entered_time=f"{today}T23:59:59.999Z",
        )

        self.assertIsInstance(orders, list)


@unittest.skipUnless(_SCHWAB_CONFIGURED, _SKIP_REASON)
class TestSchwabClientMarketDataIntegration(unittest.IsolatedAsyncioTestCase):
    """Verify market data endpoints against the real API."""

    async def asyncSetUp(self) -> None:
        from src.trading.brokers.schwab_auth import SchwabAuth

        self.client = await SchwabAuth().get_client()

    async def asyncTearDown(self) -> None:
        await self.client.aclose()

    async def test_get_quotes_returns_symbol_entry(self):
        """get_quotes() returns a dict keyed by symbol."""
        quotes = await self.client.get_quotes(["AAPL", "MSFT"])

        self.assertIsInstance(quotes, dict)
        self.assertIn("AAPL", quotes)
        self.assertIn("MSFT", quotes)

    async def test_get_quotes_contains_quote_block(self):
        """Each quote entry has a nested 'quote' block."""
        quotes = await self.client.get_quotes(["AAPL"])

        self.assertIn("quote", quotes["AAPL"])

    async def test_get_quotes_close_price_is_numeric(self):
        """closePrice is always present and numeric (unlike lastPrice outside hours)."""
        quotes = await self.client.get_quotes(["AAPL"])

        close = quotes["AAPL"]["quote"].get("closePrice")
        self.assertIsNotNone(close, "closePrice should always be present")
        self.assertIsInstance(close, (int, float))
        self.assertGreater(close, 0)


@unittest.skipUnless(_SCHWAB_CONFIGURED, _SKIP_REASON)
class TestSchwabBrokerIntegration(unittest.IsolatedAsyncioTestCase):
    """Verify SchwabBroker.from_auth() and read-only broker methods."""

    async def asyncSetUp(self) -> None:
        from src.trading.brokers.schwab_broker import SchwabBroker

        # dry_run=True — belt-and-suspenders: no live orders possible from this suite
        self.broker = await SchwabBroker.from_auth(dry_run=True)

    async def asyncTearDown(self) -> None:
        await self.broker.client.aclose()

    async def test_from_auth_populates_account_number(self):
        """from_auth() resolves and stores a non-empty account number."""
        self.assertTrue(
            self.broker.account_number,
            "account_number should be set after from_auth()",
        )
        print(self.broker.account_number)

    async def test_get_equity_is_positive(self):
        """get_equity() returns a positive portfolio value."""
        equity = await self.broker.get_equity({})

        self.assertGreater(equity, 0)

    async def test_get_positions_returns_position_objects(self):
        """get_positions() returns a dict of Position dataclass instances."""
        from src.trading.portfolio import Position

        positions = await self.broker.get_positions()

        self.assertIsInstance(positions, dict)
        for symbol, pos in positions.items():
            self.assertIsInstance(symbol, str)
            self.assertIsInstance(pos, Position)
            self.assertGreater(pos.shares, 0)
            self.assertGreater(pos.avg_price, 0)

    async def test_get_today_fills_returns_list(self):
        """get_today_fills() returns a list of (symbol, direction) tuples."""
        fills = await self.broker.get_today_fills()

        self.assertIsInstance(fills, list)
        for symbol, direction in fills:
            self.assertIsInstance(symbol, str)
            self.assertIn(direction, ("BUY", "SELL"))


if __name__ == "__main__":
    unittest.main()
