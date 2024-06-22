from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import logging
import os
from airflow.configuration import conf
import random
import string
chars = string.ascii_letters + string.digits
random_string = ''.join(random.choice(chars) for _ in range(8))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
dags_folder = conf.get('core', 'dags_folder')
# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),  # Set a recent start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'file_sensor_dag',
    default_args=default_args,
    description='A simple DAG with a FileSensor and a Bash task to process the file',
    schedule_interval=timedelta(days=1),
    catchup=False,  # Ensure that historical runs are not executed
)

# Define the FileSensor task
file_sensor = FileSensor(
    task_id='check_for_file',
    filepath='etl_file_input.csv',  # Filepath relative to the `fs_default` path
    fs_conn_id='fs_default',
    poke_interval=10,
    timeout=60,
    dag=dag,
)

# Define the BashOperator task to create a unique virtual environment, install Pandas, and process the file
process_file = BashOperator(
    task_id='process_file',
    bash_command=f"""
    source /home/airflow/.local/bin/activate && \
    pip install reportlab && \
    python {dags_folder}/etl_dag/process_file.py && \
    /home/airflow/.local/bin/deactivate 
    """,
    dag=dag,
)

# Optional: Define the end task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)
# Log the DAGs folder name
logger.info(f"DAGs folder: {dags_folder}")

# Log the list of files in the DAGs folder
logger.info(f"List of files in the DAGs folder: {os.listdir(dags_folder)}")


# Set the task dependencies
file_sensor >> process_file >> end_task
