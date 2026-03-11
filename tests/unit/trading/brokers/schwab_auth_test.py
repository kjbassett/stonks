"""Unit tests for src/trading/brokers/schwab_auth.py."""

import json
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, mock_open, patch


class TestSchwabAuthHelpers(unittest.TestCase):
    """Tests for the module-level helper functions."""

    def test_annotate_expiry_sets_future_timestamp(self):
        # Arrange
        from src.trading.brokers.schwab_auth import _annotate_expiry

        token_data = {"access_token": "abc", "expires_in": 1800}
        before = time.time()

        # Act
        result = _annotate_expiry(token_data)

        # Assert
        self.assertIn("expires_at", result)
        self.assertAlmostEqual(result["expires_at"], before + 1800, delta=2)

    def test_annotate_expiry_defaults_to_1800_when_missing(self):
        # Arrange
        from src.trading.brokers.schwab_auth import _annotate_expiry

        # Act
        result = _annotate_expiry({"access_token": "abc"})

        # Assert
        self.assertAlmostEqual(result["expires_at"], time.time() + 1800, delta=2)

    def test_is_expired_returns_false_for_fresh_token(self):
        # Arrange
        from src.trading.brokers.schwab_auth import _is_expired

        tokens = {"expires_at": time.time() + 1800}

        # Act / Assert
        self.assertFalse(_is_expired(tokens))

    def test_is_expired_returns_true_for_stale_token(self):
        # Arrange
        from src.trading.brokers.schwab_auth import _is_expired

        tokens = {"expires_at": time.time() - 1}

        # Act / Assert
        self.assertTrue(_is_expired(tokens))

    def test_is_expired_returns_true_within_refresh_buffer(self):
        # Arrange — token expires in 60s, buffer is 300s
        from src.trading.brokers.schwab_auth import _is_expired

        tokens = {"expires_at": time.time() + 60}

        # Act / Assert
        self.assertTrue(_is_expired(tokens))

    def test_is_expired_returns_true_when_key_missing(self):
        # Arrange
        from src.trading.brokers.schwab_auth import _is_expired

        # Act / Assert
        self.assertTrue(_is_expired({}))


class TestSchwabAuthInit(unittest.TestCase):
    """Tests for SchwabAuth.__init__ reading from environment variables."""

    @patch.dict("os.environ", {"SCHWAB_APP_KEY": "key123", "SCHWAB_APP_SECRET": "sec456"})
    @patch(
        "src.trading.brokers.schwab_auth.config",
        {"schwab": {"token_file": "tokens.json", "callback_url": "https://127.0.0.1"}},
    )
    def test_reads_credentials_from_env(self):
        # Arrange / Act
        from src.trading.brokers.schwab_auth import SchwabAuth

        auth = SchwabAuth()

        # Assert
        self.assertEqual(auth._app_key, "key123")
        self.assertEqual(auth._app_secret, "sec456")
        self.assertEqual(auth._token_file, "tokens.json")
        self.assertEqual(auth._callback_url, "https://127.0.0.1")

    def test_raises_if_env_vars_missing(self):
        # Arrange — ensure env vars are absent
        import os
        from src.trading.brokers.schwab_auth import SchwabAuth

        os.environ.pop("SCHWAB_APP_KEY", None)
        os.environ.pop("SCHWAB_APP_SECRET", None)

        # Act / Assert
        with self.assertRaises(KeyError):
            SchwabAuth()


class TestBuildAuthUrl(unittest.TestCase):
    """Tests for SchwabAuth._build_auth_url."""

    def _make_auth(self) -> "SchwabAuth":
        from src.trading.brokers.schwab_auth import SchwabAuth

        with patch.dict(
            "os.environ", {"SCHWAB_APP_KEY": "mykey", "SCHWAB_APP_SECRET": "mysecret"}
        ):
            with patch(
                "src.trading.brokers.schwab_auth.config",
                {"schwab": {"token_file": "t.json", "callback_url": "https://127.0.0.1"}},
            ):
                return SchwabAuth()

    def test_url_contains_app_key(self):
        auth = self._make_auth()
        url = auth._build_auth_url()
        self.assertIn("mykey", url)

    def test_url_contains_encoded_callback(self):
        auth = self._make_auth()
        url = auth._build_auth_url()
        self.assertIn("127.0.0.1", url)

    def test_url_contains_response_type_code(self):
        auth = self._make_auth()
        url = auth._build_auth_url()
        self.assertIn("response_type=code", url)


class TestParseCodeFromUrl(unittest.TestCase):
    """Tests for SchwabAuth._parse_code_from_url."""

    def _make_auth(self) -> "SchwabAuth":
        from src.trading.brokers.schwab_auth import SchwabAuth

        with patch.dict(
            "os.environ", {"SCHWAB_APP_KEY": "k", "SCHWAB_APP_SECRET": "s"}
        ):
            with patch(
                "src.trading.brokers.schwab_auth.config",
                {"schwab": {"token_file": "t.json", "callback_url": "https://127.0.0.1"}},
            ):
                return SchwabAuth()

    def test_extracts_code_from_valid_redirect(self):
        # Arrange
        auth = self._make_auth()
        url = "https://127.0.0.1?code=abc123&session=xyz"

        # Act
        code = auth._parse_code_from_url(url)

        # Assert
        self.assertEqual(code, "abc123")

    def test_raises_when_code_param_missing(self):
        # Arrange
        auth = self._make_auth()
        url = "https://127.0.0.1?session=xyz"

        # Act / Assert
        from src.trading.brokers.schwab_auth import SchwabAuthError

        with self.assertRaises(SchwabAuthError):
            auth._parse_code_from_url(url)

    def test_raises_on_malformed_url(self):
        # Arrange
        auth = self._make_auth()

        # Act / Assert
        from src.trading.brokers.schwab_auth import SchwabAuthError

        with self.assertRaises(SchwabAuthError):
            auth._parse_code_from_url("not-a-url-at-all")


class TestLoadTokens(unittest.TestCase):
    """Tests for SchwabAuth._load_tokens."""

    def _make_auth(self) -> "SchwabAuth":
        from src.trading.brokers.schwab_auth import SchwabAuth

        with patch.dict(
            "os.environ", {"SCHWAB_APP_KEY": "k", "SCHWAB_APP_SECRET": "s"}
        ):
            with patch(
                "src.trading.brokers.schwab_auth.config",
                {"schwab": {"token_file": "tokens.json", "callback_url": "https://127.0.0.1"}},
            ):
                return SchwabAuth()

    def test_loads_valid_token_file(self):
        # Arrange
        auth = self._make_auth()
        token_data = {"access_token": "tok", "refresh_token": "ref", "expires_at": 9999999999.0}
        with patch("builtins.open", mock_open(read_data=json.dumps(token_data))):
            # Act
            result = auth._load_tokens()

        # Assert
        self.assertEqual(result["access_token"], "tok")

    def test_raises_when_file_not_found(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        with patch("builtins.open", side_effect=FileNotFoundError):
            # Act / Assert
            with self.assertRaises(SchwabAuthError):
                auth._load_tokens()

    def test_raises_when_file_contains_invalid_json(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        with patch("builtins.open", mock_open(read_data="not-json{")):
            # Act / Assert
            with self.assertRaises(SchwabAuthError):
                auth._load_tokens()


class TestRefresh(unittest.IsolatedAsyncioTestCase):
    """Tests for SchwabAuth.refresh."""

    def _make_auth(self) -> "SchwabAuth":
        from src.trading.brokers.schwab_auth import SchwabAuth

        with patch.dict(
            "os.environ", {"SCHWAB_APP_KEY": "k", "SCHWAB_APP_SECRET": "s"}
        ):
            with patch(
                "src.trading.brokers.schwab_auth.config",
                {"schwab": {"token_file": "tokens.json", "callback_url": "https://127.0.0.1"}},
            ):
                return SchwabAuth()

    async def test_refresh_calls_post_token_and_saves(self):
        # Arrange
        auth = self._make_auth()
        existing = {"access_token": "old", "refresh_token": "ref_tok", "expires_at": 0.0}
        new_tokens = {"access_token": "new", "expires_in": 1800}

        auth._load_tokens = MagicMock(return_value=existing)
        auth._post_token = AsyncMock(return_value=new_tokens)
        auth._save_tokens = MagicMock()

        # Act
        await auth.refresh()

        # Assert
        auth._post_token.assert_awaited_once_with(
            {"grant_type": "refresh_token", "refresh_token": "ref_tok"}
        )
        auth._save_tokens.assert_called_once()
        saved = auth._save_tokens.call_args[0][0]
        self.assertEqual(saved["refresh_token"], "ref_tok")  # preserved

    async def test_refresh_raises_when_no_refresh_token(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        auth._load_tokens = MagicMock(return_value={"access_token": "old", "expires_at": 0.0})

        # Act / Assert
        with self.assertRaises(SchwabAuthError):
            await auth.refresh()

    async def test_refresh_propagates_post_token_error(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        auth._load_tokens = MagicMock(
            return_value={"refresh_token": "ref", "expires_at": 0.0}
        )
        auth._post_token = AsyncMock(side_effect=SchwabAuthError("network error"))

        # Act / Assert
        with self.assertRaises(SchwabAuthError):
            await auth.refresh()


class TestGetClient(unittest.IsolatedAsyncioTestCase):
    """Tests for SchwabAuth.get_client."""

    def _make_auth(self) -> "SchwabAuth":
        from src.trading.brokers.schwab_auth import SchwabAuth

        with patch.dict(
            "os.environ", {"SCHWAB_APP_KEY": "k", "SCHWAB_APP_SECRET": "s"}
        ):
            with patch(
                "src.trading.brokers.schwab_auth.config",
                {"schwab": {"token_file": "tokens.json", "callback_url": "https://127.0.0.1"}},
            ):
                return SchwabAuth()

    async def test_returns_client_with_fresh_token(self):
        # Arrange
        auth = self._make_auth()
        tokens = {"access_token": "valid_tok", "expires_at": time.time() + 1800}
        auth._load_tokens = MagicMock(return_value=tokens)
        auth.refresh = AsyncMock()

        # Act
        with patch("src.trading.brokers.schwab_client.SchwabClient") as MockClient:
            client = await auth.get_client()

        # Assert
        auth.refresh.assert_not_awaited()
        MockClient.assert_called_once_with(access_token="valid_tok")

    async def test_refreshes_when_token_expired(self):
        # Arrange
        auth = self._make_auth()
        stale = {"access_token": "old", "expires_at": time.time() - 1}
        fresh = {"access_token": "new", "expires_at": time.time() + 1800}
        auth._load_tokens = MagicMock(side_effect=[stale, fresh])
        auth.refresh = AsyncMock()

        # Act
        with patch("src.trading.brokers.schwab_client.SchwabClient"):
            await auth.get_client()

        # Assert
        auth.refresh.assert_awaited_once()

    async def test_raises_when_token_file_missing(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        auth._load_tokens = MagicMock(side_effect=SchwabAuthError("no file"))

        # Act / Assert
        with self.assertRaises(SchwabAuthError):
            await auth.get_client()


if __name__ == "__main__":
    unittest.main()
