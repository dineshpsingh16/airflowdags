from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.cncf.sensors.file import FileSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow import settings
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='validate_dags',
    default_args=default_args,
    schedule_interval=None,  # Disable schedule, rely on File Sensor
) as dag:

    dag_folder = settings.DAGS_FOLDER  # Replace with actual DAG folder path

    # Wait for new DAG files
    check_new_dags = FileSensor(
        task_id='check_new_dags',
        filepath=f"{dag_folder}/*.py",
        fs_conn_id='your_fs_conn_id',  # Optional connection for external storage
        poke_interval=30,
    )
    def get_dag(dag_id):
        """
        Custom function to retrieve DAG object based on ID.
        """
        dagbag = settings.dag_bag
        return dagbag.get_dag(dag_id)
    def validate_dags_func(dag_file_path):
        try:
            subprocess.check_call(["python", "-m", "py_compile", dag_file_path])
            print(f"DAG file {dag_file_path} compiled successfully")
        except subprocess.CalledProcessError as e:
            print(f"Error compiling DAG file {dag_file_path}: {e}")
            # Set DAG to error state (replace with actual method)
            dag = get_dag(dag_id=dag_file_path.split('/')[-1].replace('.py', ''))  # Extract DAG ID from filepath
            dag.set_is_paused(paused=False)  # Unpause first (if paused)
            dag.set_is_failed()

    validate_dags = PythonOperator(
        task_id='validate_dags',
        python_callable=validate_dags_func,
        provide_context=True,
        op_args=[ '{{ tix.xcom_pull(task_ids="check_new_dags") }}'],  # Pass filepath from FileSensor
        trigger_rule='one_success',  # Run only when File Sensor succeeds
    )

    check_new_dags >> validate_dags