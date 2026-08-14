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
        with patch("src.trading.brokers.schwab_auth.os.path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data=json.dumps(token_data))):
            # Act
            result = auth._load_tokens()

        # Assert
        self.assertEqual(result["access_token"], "tok")

    def test_returns_none_when_file_not_found(self):
        # Arrange — _load_tokens returns None for a missing file; callers
        # (refresh(), get_client()) already handle None with their own
        # contextual SchwabAuthError, so this is the correct behavior, not
        # a raise from _load_tokens itself.
        auth = self._make_auth()
        with patch("src.trading.brokers.schwab_auth.os.path.exists", return_value=False):
            # Act
            result = auth._load_tokens()

        # Assert
        self.assertIsNone(result)

    def test_raises_when_file_contains_invalid_json(self):
        # Arrange
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        with patch("src.trading.brokers.schwab_auth.os.path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data="not-json{")):
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
        existing = {
            "access_token": "old",
            "refresh_token": "ref_tok",
            "expires_at": 0.0,
            "refresh_expires_at": time.time() + 86400,
        }
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

    async def test_refresh_preserves_real_refresh_expires_at_when_token_not_rotated(self):
        # Arrange — old tokens carry a real, already-partially-elapsed deadline.
        # Schwab's response (post-_annotate_expiry) always guesses a fresh
        # "now + 7 days" and omits refresh_token, since routine refreshes don't
        # rotate it — refresh() must not let that fresh guess overwrite the
        # real deadline it already knew about.
        auth = self._make_auth()
        real_deadline = time.time() + 4 * 86400  # 4 real days left
        existing = {
            "access_token": "old",
            "refresh_token": "stable_ref_tok",
            "expires_at": 0.0,
            "refresh_expires_at": real_deadline,
        }
        wrong_fresh_guess = time.time() + 7 * 86400
        new_tokens = {
            "access_token": "new",
            "expires_in": 1800,
            "refresh_expires_at": wrong_fresh_guess,
        }
        auth._load_tokens = MagicMock(return_value=existing)
        auth._post_token = AsyncMock(return_value=new_tokens)
        auth._save_tokens = MagicMock()

        # Act
        await auth.refresh()

        # Assert
        saved = auth._save_tokens.call_args[0][0]
        self.assertEqual(saved["refresh_expires_at"], real_deadline)
        self.assertNotEqual(saved["refresh_expires_at"], wrong_fresh_guess)

    async def test_refresh_adopts_new_deadline_when_token_is_rotated(self):
        # Arrange — Schwab actually returns a NEW refresh_token this time, so
        # the fresh deadline is legitimate and should be adopted.
        auth = self._make_auth()
        real_deadline = time.time() + 4 * 86400
        existing = {
            "access_token": "old",
            "refresh_token": "old_ref_tok",
            "expires_at": 0.0,
            "refresh_expires_at": real_deadline,
        }
        fresh_deadline = time.time() + 7 * 86400
        new_tokens = {
            "access_token": "new",
            "refresh_token": "ROTATED_ref_tok",
            "expires_in": 1800,
            "refresh_expires_at": fresh_deadline,
        }
        auth._load_tokens = MagicMock(return_value=existing)
        auth._post_token = AsyncMock(return_value=new_tokens)
        auth._save_tokens = MagicMock()

        # Act
        await auth.refresh()

        # Assert
        saved = auth._save_tokens.call_args[0][0]
        self.assertEqual(saved["refresh_token"], "ROTATED_ref_tok")
        self.assertEqual(saved["refresh_expires_at"], fresh_deadline)

    async def test_refresh_raises_when_refresh_expires_at_missing(self):
        # Arrange — old-format token file predating refresh_expires_at
        # tracking entirely. _is_refresh_expired treats a missing deadline as
        # "expired" and rejects before ever reaching the preservation logic
        # above, so there's no path where a missing old deadline could let a
        # fresh (unverified) guess slip through silently — this locks that in.
        from src.trading.brokers.schwab_auth import SchwabAuthError

        auth = self._make_auth()
        existing = {"access_token": "old", "refresh_token": "ref_tok", "expires_at": 0.0}
        auth._load_tokens = MagicMock(return_value=existing)
        auth._post_token = AsyncMock()
        auth._save_tokens = MagicMock()

        # Act / Assert
        with self.assertRaises(SchwabAuthError):
            await auth.refresh()
        auth._post_token.assert_not_awaited()


class TestSecondsUntilRefreshExpiry(unittest.TestCase):
    """Tests for SchwabAuth.seconds_until_refresh_expiry."""

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

    def test_returns_remaining_seconds(self):
        # Arrange
        auth = self._make_auth()
        deadline = time.time() + 3600
        auth._load_tokens = MagicMock(return_value={"refresh_expires_at": deadline})

        # Act
        remaining = auth.seconds_until_refresh_expiry()

        # Assert
        self.assertAlmostEqual(remaining, 3600, delta=2)

    def test_returns_zero_when_no_tokens(self):
        # Arrange
        auth = self._make_auth()
        auth._load_tokens = MagicMock(return_value=None)

        # Act / Assert
        self.assertEqual(auth.seconds_until_refresh_expiry(), 0.0)

    def test_returns_zero_when_deadline_missing(self):
        # Arrange
        auth = self._make_auth()
        auth._load_tokens = MagicMock(return_value={"access_token": "x"})

        # Act / Assert
        self.assertEqual(auth.seconds_until_refresh_expiry(), 0.0)

    def test_returns_negative_when_already_expired(self):
        # Arrange
        auth = self._make_auth()
        auth._load_tokens = MagicMock(return_value={"refresh_expires_at": time.time() - 100})

        # Act / Assert
        self.assertLess(auth.seconds_until_refresh_expiry(), 0)


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
