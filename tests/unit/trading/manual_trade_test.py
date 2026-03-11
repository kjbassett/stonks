"""Unit tests for src/trading/manual_trade.py."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from src.trading.brokers.base_broker import TradeResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_broker(filled: bool = True, order_id: str = "oid-1") -> MagicMock:
    """Return a mock SchwabBroker with a stubbed fill_order and client."""
    broker = MagicMock()
    broker.account_number = "ACC123"
    broker.fill_order = AsyncMock(
        return_value=TradeResult(
            symbol="AAPL",
            shares_delta=1.0 if filled else 0.0,
            price=150.0,
            filled=filled,
            reason="filled" if filled else "unconfirmed",
            order_id=order_id,
        )
    )
    broker.client = MagicMock()
    broker.client.get_cash_available = AsyncMock(return_value=10_000.0)
    broker.client.get_quotes = AsyncMock(
        return_value={"AAPL": {"quote": {"lastPrice": 150.0}}}
    )
    return broker


# ---------------------------------------------------------------------------
# _validate_inputs
# ---------------------------------------------------------------------------


class TestValidateInputs(unittest.TestCase):
    """Tests for manual_trade._validate_inputs."""

    def _call(self, symbol="AAPL", quantity=1, price="auto", timeout=60.0):
        from src.trading.manual_trade import _validate_inputs
        _validate_inputs(symbol, quantity, price, timeout)

    def test_valid_inputs_do_not_raise(self):
        self._call()  # should not raise

    def test_valid_explicit_price(self):
        self._call(price="150.50")  # should not raise

    def test_invalid_symbol_with_digits_raises(self):
        with self.assertRaises(ValueError):
            self._call(symbol="A4PL")

    def test_invalid_symbol_empty_raises(self):
        with self.assertRaises(ValueError):
            self._call(symbol="")

    def test_invalid_symbol_with_spaces_raises(self):
        with self.assertRaises(ValueError):
            self._call(symbol="AP PL")

    def test_quantity_zero_raises(self):
        with self.assertRaises(ValueError):
            self._call(quantity=0)

    def test_quantity_negative_raises(self):
        with self.assertRaises(ValueError):
            self._call(quantity=-5)

    def test_price_non_numeric_string_raises(self):
        with self.assertRaises(ValueError):
            self._call(price="cheap")

    def test_price_zero_raises(self):
        with self.assertRaises(ValueError):
            self._call(price="0")

    def test_price_negative_raises(self):
        with self.assertRaises(ValueError):
            self._call(price="-10.0")

    def test_timeout_too_small_raises(self):
        with self.assertRaises(ValueError):
            self._call(timeout=1.0)

    def test_timeout_too_large_raises(self):
        with self.assertRaises(ValueError):
            self._call(timeout=9999.0)


# ---------------------------------------------------------------------------
# _resolve_price
# ---------------------------------------------------------------------------


class TestResolvePrice(unittest.IsolatedAsyncioTestCase):
    """Tests for manual_trade._resolve_price."""

    async def test_explicit_price_returned_without_api_call(self):
        # Arrange
        from src.trading.manual_trade import _resolve_price
        client = MagicMock()
        client.get_quotes = AsyncMock()

        # Act
        result = await _resolve_price(client, "AAPL", "155.0")

        # Assert
        self.assertAlmostEqual(result, 155.0)
        client.get_quotes.assert_not_awaited()

    async def test_auto_fetches_last_price_from_quotes(self):
        # Arrange
        from src.trading.manual_trade import _resolve_price
        client = MagicMock()
        client.get_quotes = AsyncMock(
            return_value={"AAPL": {"quote": {"lastPrice": 178.25}}}
        )

        # Act
        result = await _resolve_price(client, "AAPL", "auto")

        # Assert
        self.assertAlmostEqual(result, 178.25)

    async def test_auto_raises_when_quote_missing(self):
        # Arrange
        from src.trading.manual_trade import _resolve_price
        client = MagicMock()
        client.get_quotes = AsyncMock(return_value={})

        # Act / Assert
        with self.assertRaises(ValueError):
            await _resolve_price(client, "AAPL", "auto")

    async def test_auto_raises_when_last_price_key_absent(self):
        # Arrange
        from src.trading.manual_trade import _resolve_price
        client = MagicMock()
        client.get_quotes = AsyncMock(return_value={"AAPL": {"quote": {}}})

        # Act / Assert
        with self.assertRaises(ValueError):
            await _resolve_price(client, "AAPL", "auto")


# ---------------------------------------------------------------------------
# _verify_connection_and_get_balance
# ---------------------------------------------------------------------------


class TestVerifyConnectionAndGetBalance(unittest.IsolatedAsyncioTestCase):
    """Tests for manual_trade._verify_connection_and_get_balance."""

    async def test_returns_cash_on_success(self):
        # Arrange
        from src.trading.manual_trade import _verify_connection_and_get_balance
        broker = _make_broker()

        # Act
        result = await _verify_connection_and_get_balance(broker)

        # Assert
        self.assertAlmostEqual(result, 10_000.0)

    async def test_raises_on_api_failure(self):
        # Arrange
        from src.trading.manual_trade import _verify_connection_and_get_balance
        broker = _make_broker()
        broker.client.get_cash_available = AsyncMock(side_effect=ConnectionError("down"))

        # Act / Assert
        with self.assertRaises(ConnectionError):
            await _verify_connection_and_get_balance(broker)


# ---------------------------------------------------------------------------
# _execute_manual_trade
# ---------------------------------------------------------------------------


class TestExecuteManualTrade(unittest.IsolatedAsyncioTestCase):
    """Tests for manual_trade._execute_manual_trade."""

    async def test_raises_when_market_is_closed(self):
        # Arrange
        from src.trading.manual_trade import _execute_manual_trade

        with patch("src.trading.manual_trade.is_currently_open", return_value=False):
            # Act / Assert
            with self.assertRaises(ValueError, msg="Should refuse outside market hours"):
                await _execute_manual_trade("AAPL", 1, "auto", 60.0)

    async def test_successful_buy_calls_fill_order_with_positive_delta(self):
        # Arrange
        from src.trading.manual_trade import _execute_manual_trade
        broker = _make_broker(filled=True)

        with patch("src.trading.manual_trade.is_currently_open", return_value=True), \
             patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock(return_value=broker)

            # Act
            await _execute_manual_trade("AAPL", 2, "auto", 60.0)

        # Assert — fill_order called with positive shares_delta
        broker.fill_order.assert_awaited_once()
        _, shares_delta, _ = broker.fill_order.call_args[0]
        self.assertGreater(shares_delta, 0)

    async def test_successful_sell_calls_fill_order_with_negative_delta(self):
        # Arrange
        from src.trading.manual_trade import _execute_manual_trade
        broker = _make_broker(filled=True)
        broker.fill_order = AsyncMock(
            return_value=TradeResult("AAPL", -1.0, 150.0, True, "filled")
        )

        with patch("src.trading.manual_trade.is_currently_open", return_value=True), \
             patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock(return_value=broker)

            # Act
            await _execute_manual_trade("AAPL", -1, "auto", 60.0)

        # Assert — fill_order called with negative shares_delta
        broker.fill_order.assert_awaited_once()
        _, shares_delta, _ = broker.fill_order.call_args[0]
        self.assertLess(shares_delta, 0)

    async def test_connectivity_failure_raises_before_order(self):
        # Arrange
        from src.trading.manual_trade import _execute_manual_trade
        broker = _make_broker()
        broker.client.get_cash_available = AsyncMock(side_effect=ConnectionError("timeout"))

        with patch("src.trading.manual_trade.is_currently_open", return_value=True), \
             patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock(return_value=broker)

            # Act / Assert
            with self.assertRaises(ConnectionError):
                await _execute_manual_trade("AAPL", 1, "auto", 60.0)

        # Assert — fill_order was never called
        broker.fill_order.assert_not_awaited()


# ---------------------------------------------------------------------------
# test_buy / test_sell plugin entry points
# ---------------------------------------------------------------------------


class TestPluginEntryPoints(unittest.IsolatedAsyncioTestCase):
    """Tests for the test_buy and test_sell plugin functions."""

    async def test_buy_validates_inputs_before_trading(self):
        # Arrange — invalid symbol should raise before any broker interaction
        from src.trading.manual_trade import test_buy

        with patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock()

            with self.assertRaises(ValueError):
                await test_buy("123BAD", 1)

            MockBroker.from_auth.assert_not_awaited()

    async def test_sell_validates_inputs_before_trading(self):
        # Arrange — quantity of 0 should raise before any broker interaction
        from src.trading.manual_trade import test_sell

        with patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock()

            with self.assertRaises(ValueError):
                await test_sell("AAPL", 0)

            MockBroker.from_auth.assert_not_awaited()

    async def test_buy_passes_positive_quantity(self):
        # Arrange
        from src.trading.manual_trade import test_buy
        broker = _make_broker()

        with patch("src.trading.manual_trade.is_currently_open", return_value=True), \
             patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock(return_value=broker)

            await test_buy("AAPL", 3, timeout=10.0)

        _, shares_delta, _ = broker.fill_order.call_args[0]
        self.assertEqual(shares_delta, 3.0)

    async def test_sell_passes_negative_quantity(self):
        # Arrange
        from src.trading.manual_trade import test_sell
        broker = _make_broker()
        broker.fill_order = AsyncMock(
            return_value=TradeResult("AAPL", -2.0, 150.0, True, "filled")
        )

        with patch("src.trading.manual_trade.is_currently_open", return_value=True), \
             patch("src.trading.manual_trade.SchwabBroker") as MockBroker:
            MockBroker.from_auth = AsyncMock(return_value=broker)

            await test_sell("AAPL", 2, timeout=10.0)

        _, shares_delta, _ = broker.fill_order.call_args[0]
        self.assertEqual(shares_delta, -2.0)


if __name__ == "__main__":
    unittest.main()
