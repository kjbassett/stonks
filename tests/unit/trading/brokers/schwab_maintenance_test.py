"""Unit tests for src/trading/brokers/schwab_maintenance.py."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class TestRefreshSchwabToken(unittest.IsolatedAsyncioTestCase):
    """Tests for refresh_schwab_token's alerting behavior."""

    async def test_sends_failure_email_and_reraises_on_auth_error(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError
        import src.trading.brokers.schwab_maintenance as schwab_maintenance

        fake_auth = MagicMock()
        fake_auth.refresh = AsyncMock(side_effect=SchwabAuthError("refresh token dead"))

        with patch(
            "src.trading.brokers.schwab_maintenance.SchwabAuth", return_value=fake_auth
        ), patch(
            "src.trading.brokers.schwab_maintenance.send_email"
        ) as mock_send_email, patch(
            "src.trading.brokers.schwab_maintenance.config",
            {"notification_email_address": "me@example.com"},
        ):
            # Act / Assert — webrock's existing error logging depends on this
            # still propagating, so the email must not swallow the exception.
            with self.assertRaises(SchwabAuthError):
                await schwab_maintenance.refresh_schwab_token()

        # Assert
        mock_send_email.assert_called_once()
        subject, body, recipient = mock_send_email.call_args[0]
        self.assertEqual(subject, "Schwab re-authentication required")
        self.assertIn("refresh token dead", body)
        self.assertEqual(recipient, "me@example.com")

    async def test_sends_warning_email_when_expiry_is_near(self):
        # Arrange
        import src.trading.brokers.schwab_maintenance as schwab_maintenance

        fake_auth = MagicMock()
        fake_auth.refresh = AsyncMock()
        fake_auth.seconds_until_refresh_expiry = MagicMock(return_value=3600 * 10)  # 10h left

        with patch(
            "src.trading.brokers.schwab_maintenance.SchwabAuth", return_value=fake_auth
        ), patch(
            "src.trading.brokers.schwab_maintenance.send_email"
        ) as mock_send_email, patch(
            "src.trading.brokers.schwab_maintenance.config",
            {"notification_email_address": "me@example.com"},
        ):
            # Act
            await schwab_maintenance.refresh_schwab_token()

        # Assert
        mock_send_email.assert_called_once()
        subject, body, recipient = mock_send_email.call_args[0]
        self.assertEqual(subject, "Schwab refresh token expiring soon")
        self.assertIn("10.0 hours", body)
        self.assertEqual(recipient, "me@example.com")

    async def test_sends_no_email_when_refresh_succeeds_with_plenty_of_runway(self):
        # Arrange
        import src.trading.brokers.schwab_maintenance as schwab_maintenance

        fake_auth = MagicMock()
        fake_auth.refresh = AsyncMock()
        fake_auth.seconds_until_refresh_expiry = MagicMock(return_value=3600 * 100)  # 100h left

        with patch(
            "src.trading.brokers.schwab_maintenance.SchwabAuth", return_value=fake_auth
        ), patch(
            "src.trading.brokers.schwab_maintenance.send_email"
        ) as mock_send_email, patch(
            "src.trading.brokers.schwab_maintenance.config",
            {"notification_email_address": "me@example.com"},
        ):
            # Act
            await schwab_maintenance.refresh_schwab_token()

        # Assert
        mock_send_email.assert_not_called()

    async def test_warning_threshold_boundary_is_exclusive(self):
        # Arrange — exactly at the threshold should not warn (strict <)
        import src.trading.brokers.schwab_maintenance as schwab_maintenance

        fake_auth = MagicMock()
        fake_auth.refresh = AsyncMock()
        fake_auth.seconds_until_refresh_expiry = MagicMock(
            return_value=schwab_maintenance._REAUTH_WARNING_THRESHOLD_SECONDS
        )

        with patch(
            "src.trading.brokers.schwab_maintenance.SchwabAuth", return_value=fake_auth
        ), patch(
            "src.trading.brokers.schwab_maintenance.send_email"
        ) as mock_send_email, patch(
            "src.trading.brokers.schwab_maintenance.config",
            {"notification_email_address": "me@example.com"},
        ):
            # Act
            await schwab_maintenance.refresh_schwab_token()

        # Assert
        mock_send_email.assert_not_called()


class TestAuthorizeSchwab(unittest.TestCase):
    """Tests for authorize_schwab."""

    def test_calls_schwab_auth_authorize(self):
        # Arrange
        import src.trading.brokers.schwab_maintenance as schwab_maintenance

        fake_auth = MagicMock()
        with patch(
            "src.trading.brokers.schwab_maintenance.SchwabAuth", return_value=fake_auth
        ):
            # Act
            schwab_maintenance.authorize_schwab()

        # Assert
        fake_auth.authorize.assert_called_once()


if __name__ == "__main__":
    unittest.main()
