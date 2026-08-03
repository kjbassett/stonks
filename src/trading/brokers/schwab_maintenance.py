from webrock.decorator import plugin

from src.trading.brokers.schwab_auth import SchwabAuth


@plugin()
async def refresh_schwab_token() -> None:
    """Refresh the Schwab access and refresh tokens.

    Schedule this every 6 days (518400 seconds) to keep the 7-day refresh
    token alive during idle periods. If the refresh token has already expired,
    this will raise SchwabAuthError — run authorize() from a terminal to
    re-authenticate.
    """
    await SchwabAuth().refresh()


@plugin()
def authorize_schwab() -> None:
    """Run the one-time interactive Schwab OAuth flow.

    Opens a browser to Schwab's login page. Paste the redirect URL into the
    terminal where the server is running when prompted.
    """
    SchwabAuth().authorize()
