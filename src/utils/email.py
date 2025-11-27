import smtplib
from email.mime.text import MIMEText

from config import CONFIG
from webrock.decorator import plugin


@plugin()
def send_email(subject: str, body: str, recipient: str):
    print(f"Sending message to {recipient}")
    print(f"Subject: {subject} Body: {body}")
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = CONFIG["gmail_address"]
    msg["To"] = recipient

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(CONFIG["gmail_address"], CONFIG["gmail_pw"])
        smtp_server.sendmail(CONFIG["gmail_address"], recipient, msg.as_string())
    print("Message sent!")
