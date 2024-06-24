import datetime
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
def process_file_fn_download_and_transform():
    import pandas as pd
    import boto3

    # Define S3 paths and bucket names
    source_bucket = 'my-source-bucket'
    source_key = 'my-source-file.csv'
    dest_bucket = 'my-dest-bucket'
    dest_key = 'my-dest-file.csv'

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Download file from S3
    source_file_path = f'/{source_key}'
    s3_client.download_file(source_bucket, source_key, source_file_path)

    # Process the file (example: read CSV and transform)
    df = pd.read_csv(source_file_path)
    # Perform transformations on the DataFrame `df`
    df['new_column'] = df['existing_column'] * 2  # Example transformation

    # Save transformed file
    dest_file_path = f'{dags_folder}/{dest_key}'
    df.to_csv(dest_file_path, index=False)

    # Upload the transformed file back to S3
    s3_client.upload_file(dest_file_path, dest_bucket, dest_key)

def process_send_email():
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    sender_email = "your_email@example.com"
    receiver_email = "recipient@example.com"
    subject = "File Processing Complete"
    body = f"The file {source_key} has been processed and uploaded to {dest_bucket}/{dest_key}."

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Send the email using the SMTP server configured in Airflow
    smtp_server = "smtp.your-email-provider.com"
    smtp_port = 587
    smtp_user = "your_email@example.com"
    smtp_password = "your_password"

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],  # Replace with your recipient email(s)
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_timeout': timedelta(seconds=10),
}

# Define the DAG
with DAG(
    dag_id='example_dag_various_operators',
    default_args=default_args,
    description='DAG demonstrating various operators',
    start_date=days_ago(1),
    schedule_interval=None,  # Run manually
) as dag:

    # Wait for a specific time before proceeding
    wait_for_time = TimeSensor(
        task_id='wait_for_time',
        # target_time=(datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=5)).time(),
        target_time = datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=5)

    )

    # Download a file from S3 and transform it (replace placeholders)
    # download_and_transform = S3FileTransformOperator(
    #     task_id='download_and_transform',
    #     source_s3_key='data/raw/input.csv',
    #     source_bucket='my-source-bucket',
    #     dest_s3_key='data/processed/output.csv',
    #     dest_bucket='my-destination-bucket',
    #     transform_script='/path/to/transform_script.sh',  # Replace with your script path
    # )
# apache-airflow-providers-email
    download_and_transform = PythonVirtualenvOperator(
        task_id='download_and_transform',
        python_callable=process_file_fn_download_and_transform,
        requirements=["pandas","boto3"],  # Add other requirements if needed
        system_site_packages=False,
        dag=dag,
    )
    # Call a Python function to perform custom logic
    def process_data(**kwargs):
        # Your custom data processing logic here
        print("Processing data...")

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    # Make an HTTP request to an API endpoint
    call_api = HttpOperator(
        task_id='call_api',
        http_conn_id='my_api_connection',  # Replace with your connection ID
        endpoint='/api/v1/data',
        method='POST',
        data=json.dumps({'data': 'processed_data'}),  # Replace with your data
    )

    # Send an email notification based on DAG status
    def send_email_notification(context):
        if context['task_instance'].xcom_pull(task_ids='call_api') == 'success':
            message = 'DAG execution successful!'
        else:
            message = 'DAG execution failed!'
        send_email(message)

    # send_email = EmailOperator(
    #     task_id='send_email',
    #     to='recipient@example.com',  # Replace with recipient email
    #     subject='Airflow DAG: {{ ti.xcom_pull(task_ids="call_api") }}',
    #     html_content=send_email_notification,
    #     trigger_rule='all_done',  # Send email after all tasks are done
    # )
    send_email = PythonVirtualenvOperator(
        task_id='send_email_task',
        python_callable=process_send_email,
        requirements=["pandas","boto3"],  # Add other requirements if needed
        system_site_packages=False,
        dag=dag,
    )
# Set task dependencies
wait_for_time >> download_and_transform >> process_data_task >> call_api >> send_email