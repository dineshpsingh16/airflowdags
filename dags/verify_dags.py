import os
import subprocess
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

DAGS_DIR = "/usr/local/airflow/dags"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587
SMTP_USERNAME = "your_email@example.com"
SMTP_PASSWORD = "your_password"
EMAIL_FROM = "your_email@example.com"
EMAIL_TO = "team@example.com"
EMAIL_SUBJECT = "Airflow DAG Verification Failed"

def send_email(failed_dags):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg['Subject'] = EMAIL_SUBJECT
    body = f"The following DAGs failed verification:\n\n{failed_dags}"
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_FROM, EMAIL_TO, text)

def verify_dags():
    failed_dags = []
    for root, _, files in os.walk(DAGS_DIR):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                try:
                    subprocess.check_call(["python", "-m", "py_compile", file_path])
                except subprocess.CalledProcessError:
                    failed_dags.append(file_path)
    
    if failed_dags:
        send_email(failed_dags)
        raise Exception(f"Verification failed for the following DAGs: {failed_dags}")
    else:
        print("All DAGs passed verification.")

if __name__ == "__main__":
    verify_dags()
