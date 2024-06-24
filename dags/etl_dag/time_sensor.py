from datetime import timedelta, timezone, datetime
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

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

with DAG(
    dag_id='example_dag_time_sensor',
    default_args=default_args,
    description='DAG demonstrating various operators',
    start_date=days_ago(1),
    schedule_interval=None,  # Run manually
) as dag:
    wait_for_time = TimeSensor(
        task_id='wait_for_time',
        timeout=10,
        soft_fail=True,
        target_time=(datetime.now(timezone.utc) + timedelta(seconds=5)).time()
    )
    
    wait_for_time
