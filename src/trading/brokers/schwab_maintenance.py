from webrock.decorator import plugin

from src.trading.brokers.schwab_auth import SchwabAuth, SchwabAuthError
from src.utils.email import send_email
from src.utils.project_utilities import config

_REAUTH_WARNING_THRESHOLD_SECONDS = 24 * 3600


@plugin()
async def refresh_schwab_token() -> None:
    """Refresh the Schwab access token.

    Schwab refresh tokens have a fixed 7-day lifetime from the last
    authorize() that no amount of refreshing can extend. This job emails
    config['notification_email_address'] if the refresh token is close to
    expiring or has already failed, since re-authenticating always requires
    a human running the interactive authorize_schwab flow.
    """
    auth = SchwabAuth()
    try:
        await auth.refresh()
    except SchwabAuthError as e:
        send_email(
            "Schwab re-authentication required",
            f"Scheduled Schwab token refresh failed: {e}\n\n"
            "Live Schwab trading is down until you run authorize_schwab "
            "(or SchwabAuth().authorize() from a terminal) to re-authenticate.",
            config["notification_email_address"],
        )
        raise

    remaining = auth.seconds_until_refresh_expiry()
    if remaining < _REAUTH_WARNING_THRESHOLD_SECONDS:
        send_email(
            "Schwab refresh token expiring soon",
            f"The Schwab refresh token expires in about {remaining / 3600:.1f} hours. "
            "Run authorize_schwab (or SchwabAuth().authorize() from a terminal) "
            "before then to avoid a live-trading interruption.",
            config["notification_email_address"],
        )


@plugin()
def authorize_schwab() -> None:
    """Run the one-time interactive Schwab OAuth flow.

    Opens a browser to Schwab's login page. Paste the redirect URL into the
    terminal where the server is running when prompted.
    """
    SchwabAuth().authorize()
