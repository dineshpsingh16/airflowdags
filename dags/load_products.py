from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'load_products_from_csv',
    default_args=default_args,
    description='Load products table from CSV to PostgreSQL',
    schedule_interval=None,
)

# Function to call the external script
def process_file_fn():
    import subprocess
    from airflow.configuration import conf
    dags_folder = conf.get('core', 'dags_folder')
    script_path = os.path.join(dags_folder, 'etl_dag', 'process_load_file.py')
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed with error: {result.stderr}")

# Define the task
load_csv_task = PythonOperator(
    task_id='loadcsvtopostgres',
    python_callable=process_file_fn,
    dag=dag,
)

load_csv_task
