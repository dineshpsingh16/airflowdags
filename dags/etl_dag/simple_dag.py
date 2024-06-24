from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.email.operators.email import EmailOperator
from airflow.sensors.time import TimeSensor
from airflow.utils.dates import days_ago

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],  # Replace with your recipient email(s)
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
        target_time=datetime(year=2024, month=6, day=25, hour=8, minute=00),  # Adjust time as needed
    )

    # Download a file from S3 and transform it (replace placeholders)
    download_and_transform = S3FileTransformOperator(
        task_id='download_and_transform',
        source_s3_key='data/raw/input.csv',
        source_bucket='my-source-bucket',
        dest_s3_key='data/processed/output.csv',
        dest_bucket='my-destination-bucket',
        transform_script='/path/to/transform_script.sh',  # Replace with your script path
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

    send_email = EmailOperator(
        task_id='send_email',
        to='recipient@example.com',  # Replace with recipient email
        subject='Airflow DAG: {{ ti.xcom_pull(task_ids="call_api") }}',
        html_content=send_email_notification,
        trigger_rule='all_done',  # Send email after all tasks are done
    )

# Set task dependencies
wait_for_time >> download_and_transform >> process_data_task >> call_api >> send_email