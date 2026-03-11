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
import os
import time
import webbrowser
from typing import Any, Dict
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from src.utils.project_utilities import config

_AUTH_ENDPOINT = "https://api.schwabapi.com/v1/oauth/authorize"
_TOKEN_ENDPOINT = "https://api.schwabapi.com/v1/oauth/token"
_REFRESH_BUFFER_SECONDS = 300  # refresh 5 min before expiry


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
        print("\nOpening Schwab authorization page...")
        print(f"If your browser does not open, visit:\n  {auth_url}\n")
        webbrowser.open(auth_url)
        print(
            "After authorizing, you will see a connection error page.\n"
            "Copy the FULL URL from the browser address bar and paste it below.\n"
        )
        redirect_url = input("Paste redirect URL: ").strip()
        code = self._parse_code_from_url(redirect_url)
        tokens = self._exchange_code(code)
        self._save_tokens(tokens)
        print("Authorization successful. Tokens saved.")

    async def refresh(self) -> None:
        """Exchange the refresh token for a new access token and save it."""
        tokens = self._load_tokens()
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            raise SchwabAuthError("No refresh token found. Run authorize() first.")
        new_tokens = await self._post_token(
            {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        new_tokens.setdefault("refresh_token", refresh_token)
        self._save_tokens(new_tokens)

    async def get_client(self) -> "SchwabClient":  # noqa: F821
        """
        Return an authenticated SchwabClient, refreshing the token if needed.

        Raises SchwabAuthError if no token file exists — run authorize() first.
        """
        from src.trading.brokers.schwab_client import SchwabClient

        tokens = self._load_tokens()
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
        try:
            with open(self._token_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            raise SchwabAuthError(
                f"Token file not found at '{self._token_file}'. Run authorize() first."
            )
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
    """Add an absolute expires_at timestamp to a token response dict."""
    expires_in = token_data.get("expires_in", 1800)
    token_data["expires_at"] = time.time() + expires_in
    return token_data


def _is_expired(tokens: Dict[str, Any]) -> bool:
    """Return True if the access token is expired or within the refresh buffer."""
    expires_at = tokens.get("expires_at", 0.0)
    return time.time() >= expires_at - _REFRESH_BUFFER_SECONDS
