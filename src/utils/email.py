import logging
import smtplib
from email.mime.text import MIMEText

from src.utils.project_utilities import config
from webrock.decorator import plugin

_log = logging.getLogger("utils.email")


@plugin()
def send_email(subject: str, body: str, recipient: str):
    _log.info("Sending email to %s — subject: %s", recipient, subject)
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = config["gmail_address"]
    msg["To"] = recipient

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(config["gmail_address"], config["gmail_pw"])
        smtp_server.sendmail(config["gmail_address"], recipient, msg.as_string())
    _log.info("Email sent to %s", recipient)
