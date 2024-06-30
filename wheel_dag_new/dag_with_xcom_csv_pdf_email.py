# /path/to/your/dag_with_xcom_csv_pdf_email.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import subprocess
# from wheeldagutil.tasks import  install_requirements, read_csv, task1_fun_operator, process_data, send_email

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
    'start_date': datetime.today(),
}

# Instantiate the DAG
dag = DAG(
    'new_example_dag_with_xcom_csv_pdf_email',
    default_args=default_args,
    description='An example DAG with Python operators, custom packages, XCom, CSV processing, PDF report, and email notification',
    schedule_interval=None,
)

# Define Python functions

def install_requirements():
    import os
    import glob
    import pkg_resources
    # Path to the dist folder in the current DAG directory
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    print(f"dag_folder :{dag_folder}")
    dag_files = os.listdir(dag_folder)
    print("contents of dag folder")
    dist_folder = os.path.join(dag_folder, 'dist')
    requirements_path = os.path.join(dag_folder, 'requirements.txt')
    print(f" requirements_path :{requirements_path}")
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
    print(f"wheel_files : ",wheel_files)    
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


# Create Python operators
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=lambda **kwargs: read_csv(**kwargs),
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=lambda **kwargs: process_data(**kwargs),
    provide_context=True,
    dag=dag,
)

# Install custom packages using PythonVirtualenvOperator
install_packages_task = PythonOperator(
    task_id='install_packages',
    python_callable=install_requirements,
    dag=dag,
)
# Create Email operator

send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=lambda **kwargs: send_email(**kwargs),
    provide_context=True,
    dag=dag,
)

def task1_fun_operator(**kwargs):
    from wheeldagutil.tasks import task1_fun
    # Pull the CSV data from XCom
    csv_data_json = kwargs['ti'].xcom_pull(key='csv_data', task_ids='read_csv')
    
    # Call task1_fun to update the CSV data with balance column
    updated_csv_data_json = task1_fun(csv_data_json)
    
    # Push the updated CSV data back to XCom
    kwargs['ti'].xcom_push(key='csv_data', value=updated_csv_data_json)
    print("Task 1 fun executed, CSV data updated and pushed to XCom")
task1_fun_task = PythonOperator(
    task_id='task1_fun_task',
    python_callable=task1_fun_operator,
    provide_context=True,
    dag=dag,
)
import importlib

def load_wheeldagutil_tasks():
    global  read_csv, task1_fun_operator, process_data, send_email

    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    read_csv = wheeldagutil_tasks.read_csv
    # task1_fun_operator = wheeldagutil_tasks.task1_fun_operator
    process_data = wheeldagutil_tasks.process_data
    send_email = wheeldagutil_tasks.send_email

    print("Loaded wheeldagutil tasks successfully")

load_tasks_task = PythonOperator(
    task_id='load_tasks',
    python_callable=load_wheeldagutil_tasks,
    dag=dag,
)

# Set task dependencies
install_packages_task  >> load_tasks_task >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task
