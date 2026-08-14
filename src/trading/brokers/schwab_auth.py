"""
Schwab OAuth 2.0 token management.

Credentials are read from environment variables SCHWAB_APP_KEY and
SCHWAB_APP_SECRET. Token state is persisted to the JSON file specified by
config['schwab']['token_file'] (default: schwab_tokens.json).

Typical usage::

    auth = SchwabAuth()
    auth.authorize()          # one-time interactive setup
    client = await auth.get_client()   # on every trading session start
"""

import base64
import json
import logging
import os
import time
import webbrowser
from typing import Any, Dict
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from src.utils.project_utilities import config

_log = logging.getLogger("trading.schwab_auth")

_AUTH_ENDPOINT = "https://api.schwabapi.com/v1/oauth/authorize"
_TOKEN_ENDPOINT = "https://api.schwabapi.com/v1/oauth/token"
_REFRESH_BUFFER_SECONDS = 300  # refresh access token 5 min before expiry
_REFRESH_TOKEN_BUFFER_SECONDS = 3600  # reauth 1 hour before refresh token expiry
_REFRESH_TOKEN_LIFETIME_SECONDS = 7 * 24 * 3600  # Schwab refresh tokens last 7 days


class SchwabAuthError(Exception):
    """Raised when an OAuth token operation fails."""


class SchwabAuth:
    """
    Manages Schwab OAuth 2.0 tokens.

    Reads SCHWAB_APP_KEY and SCHWAB_APP_SECRET from environment variables.
    Persists access and refresh tokens to a local JSON file.
    """

    def __init__(self) -> None:
        self._app_key = os.environ["SCHWAB_APP_KEY"]
        self._app_secret = os.environ["SCHWAB_APP_SECRET"]
        cfg = config["schwab"]
        self._token_file: str = cfg["token_file"]
        self._callback_url: str = cfg["callback_url"]

    def authorize(self) -> None:
        """
        Run the one-time interactive OAuth authorization flow.

        Opens a browser to Schwab's login page. After you authorize, Schwab
        redirects to the callback URL — the page will fail to load, which is
        expected. Copy the full URL from the browser's address bar and paste
        it when prompted. Tokens are saved to the configured token file.
        """
        auth_url = self._build_auth_url()
        _log.info("Opening Schwab authorization page")
        _log.info("If browser does not open, visit: %s", auth_url)
        webbrowser.open(auth_url)
        print(
            "After authorizing, you will see a connection error page.\n"
            "Copy the FULL URL from the browser address bar and paste it below.\n"
        )
        redirect_url = input("Paste redirect URL: ").strip()
        code = self._parse_code_from_url(redirect_url)
        tokens = self._exchange_code(code)
        self._save_tokens(tokens)
        _log.info("Authorization successful — tokens saved")

    async def refresh(self) -> None:
        """Exchange the refresh token for a new access token and save it.

        Raises:
            SchwabAuthError: If the refresh token is missing or expired.
                Run ``authorize()`` from a terminal to re-authenticate.
        """
        tokens = self._load_tokens()
        refresh_token = tokens.get("refresh_token") if tokens else None
        if not refresh_token or _is_refresh_expired(tokens):
            raise SchwabAuthError(
                "Schwab refresh token expired or missing. Run authorize() from a terminal."
            )
        new_tokens = await self._post_token(
            {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        new_tokens.setdefault("refresh_token", refresh_token)
        if new_tokens["refresh_token"] == refresh_token:
            # Schwab didn't rotate the refresh token, so it's still the same
            # fixed-lifetime token from the last authorize()/rotation — keep its
            # real deadline (guaranteed present: _is_refresh_expired above
            # already rejects any token missing it) instead of
            # _annotate_expiry's fresh "now + 7 days" guess, which would
            # otherwise reset every refresh and mask the actual deadline
            # until Schwab's hard rejection.
            new_tokens["refresh_expires_at"] = tokens["refresh_expires_at"]
        self._save_tokens(new_tokens)

    def seconds_until_refresh_expiry(self) -> float:
        """Seconds remaining until the refresh token's real (Schwab-side) deadline."""
        tokens = self._load_tokens()
        if not tokens or tokens.get("refresh_expires_at") is None:
            return 0.0
        return float(tokens["refresh_expires_at"]) - time.time()

    async def get_client(self) -> "SchwabClient":  # noqa: F821
        """Return an authenticated SchwabClient, refreshing the access token if needed.

        Raises:
            SchwabAuthError: If no token file exists or the refresh token is expired.
                Run ``authorize()`` from a terminal to re-authenticate.
        """
        from src.trading.brokers.schwab_client import SchwabClient

        tokens = self._load_tokens()
        if not tokens:
            raise SchwabAuthError(
                "No Schwab token file found. Run authorize() from a terminal."
            )
        if _is_expired(tokens):
            await self.refresh()
            tokens = self._load_tokens()
        return SchwabClient(access_token=tokens["access_token"])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_auth_url(self) -> str:
        """Construct the Schwab OAuth authorization URL."""
        params = urlencode(
            {
                "client_id": self._app_key,
                "redirect_uri": self._callback_url,
                "response_type": "code",
            }
        )
        return f"{_AUTH_ENDPOINT}?{params}"

    def _parse_code_from_url(self, url: str) -> str:
        """Extract the authorization code query parameter from the redirect URL."""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if "code" not in params:
            raise SchwabAuthError(f"No 'code' parameter in redirect URL: {url!r}")
        return params["code"][0]

    def _exchange_code(self, code: str) -> Dict[str, Any]:
        """Exchange an authorization code for tokens (synchronous)."""
        resp = httpx.post(
            _TOKEN_ENDPOINT,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._callback_url,
            },
            headers=self._auth_header(),
        )
        if not resp.is_success:
            raise SchwabAuthError(
                f"Token exchange failed {resp.status_code}: {resp.text[:500]}"
            )
        return _annotate_expiry(resp.json())

    async def _post_token(self, data: Dict[str, str]) -> Dict[str, Any]:
        """POST to the token endpoint asynchronously and return parsed response."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                _TOKEN_ENDPOINT, data=data, headers=self._auth_header()
            )
        if not resp.is_success:
            raise SchwabAuthError(
                f"Token request failed {resp.status_code}: {resp.text[:500]}"
            )
        return _annotate_expiry(resp.json())

    def _auth_header(self) -> Dict[str, str]:
        """Build the HTTP Basic Auth header for token endpoint requests."""
        credentials = base64.b64encode(
            f"{self._app_key}:{self._app_secret}".encode()
        ).decode()
        return {"Authorization": f"Basic {credentials}"}

    def _load_tokens(self) -> Dict[str, Any]:
        """Read token data from the token file."""
        if not os.path.exists(self._token_file):
            return None
        try:
            with open(self._token_file, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise SchwabAuthError(f"Invalid token file '{self._token_file}': {e}")

    def _save_tokens(self, tokens: Dict[str, Any]) -> None:
        """Write token data to the token file."""
        with open(self._token_file, "w") as f:
            json.dump(tokens, f, indent=2)


# ------------------------------------------------------------------
# Module-level helpers (no instance state needed)
# ------------------------------------------------------------------


def _annotate_expiry(token_data: Dict[str, Any]) -> Dict[str, Any]:
    """Add absolute expiry timestamps for both the access and refresh tokens."""
    now = time.time()
    token_data["expires_at"] = now + token_data.get("expires_in", 1800)
    refresh_expires_in = token_data.get("refresh_token_expires_in", _REFRESH_TOKEN_LIFETIME_SECONDS)
    token_data["refresh_expires_at"] = now + int(refresh_expires_in)
    return token_data


def _is_expired(tokens: Dict[str, Any]) -> bool:
    """Return True if the access token is expired or within the refresh buffer."""
    expires_at = tokens.get("expires_at", 0.0)
    return time.time() >= expires_at - _REFRESH_BUFFER_SECONDS


def _is_refresh_expired(tokens: Dict[str, Any]) -> bool:
    """Return True if the refresh token is expired (or expire time not available from older token storage format)
    """
    refresh_expires_at = tokens.get("refresh_expires_at")
    if refresh_expires_at is None:
        return True
    return time.time() >= float(refresh_expires_at) - _REFRESH_TOKEN_BUFFER_SECONDS