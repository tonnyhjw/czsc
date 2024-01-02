import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(html_content, subject):
    sender_email = os.environ.get('EMAIL_USER')
    sender_password = os.environ.get('EMAIL_PASS')

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = os.environ.get('RECIPIENT_EMAIL')
    message["Subject"] = subject

    message.attach(MIMEText(html_content, "html"))

    server = smtplib.SMTP_SSL('smtp.126.com', 465)
    server.starttls()
    server.login(sender_email, sender_password)
    server.send_message(message)
    server.quit()
