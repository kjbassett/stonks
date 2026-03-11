"""Unit tests for SchwabBroker._cancel_with_retry and cancel-on-timeout behavior."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from src.trading.brokers.schwab_client import SchwabAPIError

_CONFIG_PATCH = {
    "schwab": {"account_number": "ACC123", "token_file": "t.json", "callback_url": ""}
}


def _make_broker(dry_run: bool = False, order_confirm_timeout: float = 0.05):
    """Build a SchwabBroker with a mocked SchwabClient."""
    from src.trading.brokers.schwab_broker import SchwabBroker

    client = MagicMock()
    client.cancel_order = AsyncMock()
    client.place_order = AsyncMock(return_value="order-1")
    client.get_order = AsyncMock(return_value={"status": "WORKING"})
    with patch("src.trading.brokers.schwab_broker.config", _CONFIG_PATCH):
        return SchwabBroker(
            dry_run=dry_run,
            order_confirm_timeout=order_confirm_timeout,
            client=client,
        )


class TestCancelWithRetry(unittest.IsolatedAsyncioTestCase):
    """Tests for SchwabBroker._cancel_with_retry."""

    async def test_cancel_succeeds_on_first_attempt(self):
        # Arrange
        broker = _make_broker()
        broker.client.cancel_order = AsyncMock()

        # Act
        await broker._cancel_with_retry("order-99")

        # Assert — called exactly once
        broker.client.cancel_order.assert_awaited_once_with(broker.account_number, "order-99")

    async def test_cancel_retries_once_after_failure(self):
        # Arrange — first attempt fails, second succeeds
        broker = _make_broker()
        broker.client.cancel_order = AsyncMock(
            side_effect=[SchwabAPIError(500, "server error"), None]
        )

        # Act
        await broker._cancel_with_retry("order-99")

        # Assert — retried exactly once
        self.assertEqual(broker.client.cancel_order.await_count, 2)

    async def test_does_not_raise_when_all_attempts_fail(self):
        # Arrange — both attempts fail
        broker = _make_broker()
        broker.client.cancel_order = AsyncMock(side_effect=SchwabAPIError(500, "error"))

        # Act / Assert — must not propagate the exception
        try:
            await broker._cancel_with_retry("order-99")
        except Exception as exc:
            self.fail(f"_cancel_with_retry raised unexpectedly: {exc}")

        self.assertEqual(broker.client.cancel_order.await_count, 2)


class TestFillOrderCancelBehavior(unittest.IsolatedAsyncioTestCase):
    """Verify fill_order cancels on timeout but not on terminal rejection."""

    async def test_cancel_called_when_fill_times_out(self):
        # Arrange — get_order always returns WORKING so the poll times out
        broker = _make_broker(order_confirm_timeout=0.01)
        broker._cancel_with_retry = AsyncMock()

        # Act
        result = await broker.fill_order("AAPL", 1.0, 150.0)

        # Assert — cancel was attempted, result is unfilled
        broker._cancel_with_retry.assert_awaited_once()
        self.assertFalse(result.filled)
        self.assertEqual(result.reason, "unconfirmed")

    async def test_cancel_not_called_on_immediate_fill(self):
        # Arrange — order fills immediately
        broker = _make_broker(order_confirm_timeout=5.0)
        broker.client.get_order = AsyncMock(return_value={"status": "FILLED"})
        broker._cancel_with_retry = AsyncMock()

        # Act
        result = await broker.fill_order("AAPL", 1.0, 150.0)

        # Assert
        broker._cancel_with_retry.assert_not_awaited()
        self.assertTrue(result.filled)

    async def test_cancel_not_called_on_terminal_rejection(self):
        # Arrange — order is REJECTED (terminal status, not a timeout)
        broker = _make_broker(order_confirm_timeout=5.0)
        broker.client.get_order = AsyncMock(return_value={"status": "REJECTED"})
        broker._cancel_with_retry = AsyncMock()

        # Act
        result = await broker.fill_order("AAPL", 1.0, 150.0)

        # Assert — already terminal, no cancel attempted
        broker._cancel_with_retry.assert_not_awaited()
        self.assertFalse(result.filled)


if __name__ == "__main__":
    unittest.main()
