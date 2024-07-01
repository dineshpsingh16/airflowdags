from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import os
import glob
import importlib
from airflow.models import Variable

from airflow.sensors.python import PythonSensor

def check_installation_status():
    return Variable.get("install_packages_task_status") == 'True'

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
    print(f"dag_folder :{dag_folder}")
    dist_folder = os.path.join(dag_folder, 'dist')
    requirements_path = os.path.join(dag_folder, 'requirements.txt')
    print(f"requirements_path :{requirements_path}")
    
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
    print(f"dist_folder :{dist_folder} ")
    print(f"wheel_files : ", wheel_files)
    
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

    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    read_csv = wheeldagutil_tasks.read_csv
    task1_fun_operator = wheeldagutil_tasks.task1_fun_operator
    process_data = wheeldagutil_tasks.process_data
    send_email = wheeldagutil_tasks.send_email

    print("Loaded wheeldagutil tasks successfully")

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

log_dags_dir_task = PythonOperator(
    task_id='log_dags_dir',
    python_callable=log_dags_directory_contents,
    dag=dag,
)

# Create a function to create the dependent tasks
def create_dependent_tasks():
    from wheeldagutil.tasks import read_csv,process_data,send_email,task1_fun_operator
    # Read CSV task
    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        provide_context=True,
        dag=dag,
    )

    # Task 1 function operator task
    task1_fun_task = PythonOperator(
        task_id='task1_fun_task',
        python_callable=task1_fun_operator,
        provide_context=True,
        dag=dag,
    )

    # Process data task
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
        dag=dag,
    )

    # Send email task
    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email,
        provide_context=True,
        dag=dag,
    )

    # Set dependencies between the dynamically created tasks
    install_packages_task >> load_tasks_task >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task

# Create the task to create the dependent tasks
create_tasks_task = PythonOperator(
    task_id='create_tasks',
    python_callable=create_dependent_tasks,
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


# Set initial task dependencies
# install_packages_task >> load_tasks_task >> log_dags_dir_task >> create_tasks_task
# install_packages_task >> install_packages_sensor  >> create_tasks_task
install_packages_task >> install_packages_sensor >> load_tasks_task >> log_dags_dir_task >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task
