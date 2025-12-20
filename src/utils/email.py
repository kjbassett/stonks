import smtplib
from email.mime.text import MIMEText

from src.utils.project_utilities import config
from webrock.decorator import plugin


@plugin()
def send_email(subject: str, body: str, recipient: str):
    print(f"Sending message to {recipient}")
    print(f"Subject: {subject} Body: {body}")
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = config["gmail_address"]
    msg["To"] = recipient

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(config["gmail_address"], config["gmail_pw"])
        smtp_server.sendmail(config["gmail_address"], recipient, msg.as_string())
    print("Message sent!")
