from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.cncf.sensors.file import FileSensor
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow import settings
import subprocess
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
import os
import sys
import importlib
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
    def validate_dags_func1(dag_file_path):
        try:
            subprocess.check_call(["python", "-m", "py_compile", dag_file_path])
            print(f"DAG file {dag_file_path} compiled successfully")
        except subprocess.CalledProcessError as e:
            print(f"Error compiling DAG file {dag_file_path}: {e}")
            # Set DAG to error state (replace with actual method)
            dag = get_dag(dag_id=dag_file_path.split('/')[-1].replace('.py', ''))  # Extract DAG ID from filepath
            dag.set_is_paused(paused=False)  # Unpause first (if paused)
            dag.set_is_failed()
    def mark_dagrun_failed(dag_file_path):
        dag_id = os.path.basename(dag_file_path).replace('.py', '')
        dagbag = settings.DAGBAG
        if dag_id in dagbag.dags:
            dag = dagbag.get_dag(dag_id)
            for dr in DagRun.find(dag_id=dag_id, state=State.RUNNING):
                dr.set_state(State.FAILED)
                for ti in dr.get_task_instances():
                    ti.set_state(State.FAILED)
            print(f"DAG run for {dag_id} set to FAILED")
        else:
            print(f"DAG {dag_id} not found in DAG bag")

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
            mark_dagrun_failed(dag)
    def validate_dags_func2(dag_file_path):
        try:
            # Try to import the module
            spec = importlib.util.spec_from_file_location("module.name", dag_file_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["module.name"] = module
            spec.loader.exec_module(module)
            print(f"DAG file {dag_file_path} imported successfully")
            
            # Try to compile the DAG file
            subprocess.check_call(["python", "-m", "py_compile", dag_file_path])
            print(f"DAG file {dag_file_path} compiled successfully")
        
        except ModuleNotFoundError as e:
            missing_package = str(e).split("No module named ")[1].strip("'")
            print(f"Missing package: {missing_package}. Installing...")
            install_package(missing_package)
            
            # Retry importing after installing the package
            try:
                spec.loader.exec_module(module)
                print(f"DAG file {dag_file_path} imported successfully after installing {missing_package}")
                
                # Try to compile the DAG file again
                subprocess.check_call(["python", "-m", "py_compile", dag_file_path])
                print(f"DAG file {dag_file_path} compiled successfully")
            except Exception as retry_e:
                print(f"Failed to import/compile DAG file {dag_file_path} after installing {missing_package}: {retry_e}")
                mark_dagrun_failed(dag_file_path)
        except subprocess.CalledProcessError as e:
            print(f"Error compiling DAG file {dag_file_path}: {e}")
            mark_dagrun_failed(dag_file_path)

    validate_dags = PythonOperator(
        task_id='validate_dags',
        python_callable=validate_dags_func,
        provide_context=True,
        op_args=[ '{{ tix.xcom_pull(task_ids="check_new_dags") }}'],  # Pass filepath from FileSensor
        trigger_rule='one_success',  # Run only when File Sensor succeeds
    )

    check_new_dags >> validate_dags