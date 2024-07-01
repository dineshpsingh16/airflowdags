from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import os
import glob
import importlib
from airflow.models import Variable
from airflow.sensors.python import PythonSensor

# Global variables to hold the functions from wheeldagutil
global read_csv, task1_fun_operator, process_data, send_email
read_csv = None
task1_fun_operator = None
process_data = None
send_email = None

def check_installation_status():
    return Variable.get("install_packages_task_status") == 'True'

def check_tasks_loaded_status():
    return read_csv is not None and task1_fun_operator is not None and process_data is not None and send_email is not None

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'start_date': datetime.today(),
}

# Instantiate the DAG
dag = DAG(
    'new_example_dag_with_xcom_csv_pdf_email',
    default_args=default_args,
    description='An example DAG with Python operators, custom packages, XCom, CSV processing, PDF report, and email notification',
    schedule_interval=None,
)

install_packages_sensor = PythonSensor(
    task_id='install_packages_sensor',
    python_callable=check_installation_status,
    timeout=600,  # Timeout in seconds
    poke_interval=10,  # Time in seconds that the job should wait in between each try
    mode='poke',  # Mode can be poke or reschedule
    dag=dag,
)

# Define the function to install requirements
def install_requirements():
    import pkg_resources
    Variable.set("install_packages_task_status", False)
    # Path to the dist folder in the current DAG directory
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    print(f"dag_folder: {dag_folder}")
    dist_folder = os.path.join(dag_folder, 'dist')
    requirements_path = os.path.join(dag_folder, 'requirements.txt')
    print(f"requirements_path: {requirements_path}")
    
    # Install packages from the requirements.txt file
    if os.path.exists(requirements_path):
        print("requirements_path found")
        subprocess.check_call(['pip', 'install', '-r', requirements_path])
    
    if os.path.exists(dist_folder):
        print(f"dist folder found at: {dist_folder}")
        dist_files = os.listdir(dist_folder)
        if dist_files:
            print(f"Contents of dist folder: {dist_files}")
        else:
            print("dist folder is empty")
    else:
        print("dist folder not found")
    
    # Find all wheel files in the dist folder
    wheel_files = glob.glob(os.path.join(dist_folder, '*.whl'))
    print(f"dist_folder: {dist_folder}")
    print(f"wheel_files: ", wheel_files)
    
    # Install each wheel file
    for wheel_file in wheel_files:
        print(f"Installing wheel package: {wheel_file}")
        package_name = os.path.basename(wheel_file).split('-')[0]
        subprocess.check_call(['pip', 'uninstall', '-y', package_name])
        subprocess.check_call(['pip', 'install', wheel_file])
    
    # Verify installation by listing installed packages
    installed_packages = pkg_resources.working_set
    for package in installed_packages:
        print(package.key, package.version)
    Variable.set("install_packages_task_status", True)

# Task to log the contents of the DAG directory
def log_dags_directory_contents():
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    print(f"Current directory: {dag_folder}")
    print(f"Contents of the current directory: {os.listdir(dag_folder)}")

# Task to load the wheeldagutil tasks
def load_wheeldagutil_tasks():
    global read_csv, task1_fun_operator, process_data, send_email
    print("loading wheeldagutil tasks")
    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    print(f"wheeldagutil_tasks: {wheeldagutil_tasks}")
    read_csv = wheeldagutil_tasks.read_csv
    print(f"read_csv: {read_csv}")
    task1_fun_operator = wheeldagutil_tasks.task1_fun_operator
    print(f"task1_fun_operator: {task1_fun_operator}")
    process_data = wheeldagutil_tasks.process_data
    print(f"process_data: {process_data}")
    send_email = wheeldagutil_tasks.send_email
    print(f"send_email: {send_email}")

    print("Loaded wheeldagutil tasks successfully")

# Task to print loaded tasks
def print_loaded_tasks():
    global read_csv, task1_fun_operator, process_data, send_email
    print(f"read_csv: {read_csv}")
    print(f"task1_fun_operator: {task1_fun_operator}")
    print(f"process_data: {process_data}")
    print(f"send_email: {send_email}")

# Create the initial tasks
install_packages_task = PythonOperator(
    task_id='install_packages',
    python_callable=install_requirements,
    dag=dag,
)

load_tasks_task = PythonOperator(
    task_id='load_tasks',
    python_callable=load_wheeldagutil_tasks,
    dag=dag,
)

tasks_loaded_sensor = PythonSensor(
    task_id='tasks_loaded_sensor',
    python_callable=check_tasks_loaded_status,
    timeout=600,
    poke_interval=10,
    mode='poke',
    dag=dag,
)

log_dags_dir_task = PythonOperator(
    task_id='log_dags_dir',
    python_callable=log_dags_directory_contents,
    dag=dag,
)

print_tasks_task = PythonOperator(
    task_id='print_tasks',
    python_callable=print_loaded_tasks,
    dag=dag,
)

# Define the tasks that depend on wheeldagutil after ensuring it is installed
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=lambda **kwargs: read_csv(**kwargs),
    provide_context=True,
    dag=dag,
)

task1_fun_task = PythonOperator(
    task_id='task1_fun_task',
    python_callable=lambda **kwargs: task1_fun_operator(**kwargs),
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=lambda **kwargs: process_data(**kwargs),
    provide_context=True,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=lambda **kwargs: send_email(**kwargs),
    provide_context=True,
    dag=dag,
)

# Ensure correct order of task execution
install_packages_task >> install_packages_sensor >> load_tasks_task >> tasks_loaded_sensor >> log_dags_dir_task >> print_tasks_task >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task
