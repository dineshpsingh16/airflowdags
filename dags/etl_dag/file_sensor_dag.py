from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
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
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'file_sensor_dag',
    default_args=default_args,
    description='A simple DAG with a FileSensor and a Python task to process the file',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the FileSensor task
file_sensor = FileSensor(
    task_id='check_for_file',
    filepath=f"{dags_folder}/data/etl_file_input.csv",
    fs_conn_id='fs_default',
    poke_interval=10,
    timeout=60,
    dag=dag,
)

# Function to call process_file.py
def process_file_fn(dags_folder):
    import subprocess
    import os
    script_path = os.path.join(dags_folder, 'etl_dag', 'process_file.py')
    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed with error: {result.stderr}")

# Define the PythonVirtualenvOperator task
process_file = PythonVirtualenvOperator(
    task_id='process_file',
    python_callable=process_file_fn,
    requirements=["pandas","reportlab"],  # Add other requirements if needed
    system_site_packages=False,
     op_args=[dags_folder],
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
