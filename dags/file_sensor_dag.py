# /usr/local/airflow/dags/file_sensor_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'file_sensor_dag',
    default_args=default_args,
    description='A simple DAG with a FileSensor',
    schedule_interval=timedelta(days=1),
)

# Define the FileSensor task
file_sensor = FileSensor(
    task_id='check_for_file',
    filepath='/usr/local/airflow/dags/data/file_sensor_dag_input.csv',  # Correct absolute path within the container
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=600,
    dag=dag,
)

# Optional: Define the end task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set the task dependencies
file_sensor >> end_task
