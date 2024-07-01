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



# Create the initial tasks
install_packages_task = PythonOperator(
    task_id='install_packages',
    python_callable=install_requirements,
    dag=dag,
)


def execute_read_csv(**kwargs):
    global read_csv
    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    read_csv = wheeldagutil_tasks.read_csv    
    read_csv(**kwargs)

read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=execute_read_csv,
    provide_context=True,
    dag=dag,
)


def execute_task1_fun_operator(**kwargs):
    global task1_fun_operator
    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    task1_fun_operator = wheeldagutil_tasks.task1_fun_operator    
    task1_fun_operator(**kwargs)

task1_fun_task = PythonOperator(
    task_id='task1_fun_task',
    python_callable=execute_task1_fun_operator,
    provide_context=True,
    dag=dag,
)


def process_data_function(**kwargs):
    global task1_fun_operator
    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    process_data = wheeldagutil_tasks.process_data    
    process_data(**kwargs)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data_function,
    provide_context=True,
    dag=dag,
)

def send_email_function(**kwargs):
    global task1_fun_operator
    wheeldagutil_tasks = importlib.import_module('wheeldagutil.tasks')
    send_email = wheeldagutil_tasks.send_email    
    send_email(**kwargs)

send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email_function,
    provide_context=True,
    dag=dag,
)

install_packages_task >> install_packages_sensor  >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task
