from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import logging
import os
from airflow.configuration import conf
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
    bash_command="""
    VENV_DIR=/tmp/venv_{{ ti.dag_id }}_{{ ti.task_id }} && \
    python3 -m venv $VENV_DIR && \
    source $VENV_DIR/bin/activate && \
    pip install pandas reportlab && \
    pip install reportlab && \    
    python /usr/local/airflow/dags/etl_dag/process_file.py && \
    deactivate && \
    rm -rf $VENV_DIR
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
