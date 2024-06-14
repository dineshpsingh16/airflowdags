from datetime import datetime, timedelta
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to check if a package is installed
def check_package_installed(package_name):
    try:
        subprocess.check_output([f"pip show {package_name}"], shell=True)
        print(f"Package {package_name} is installed.")
    except subprocess.CalledProcessError:
        raise Exception(f"Package {package_name} is not installed.")

# Function to verify packages for the DAG
def verify_dag_packages():
    required_packages = [
        'apache-airflow',
        'apache-airflow-providers-smtp',
        'apache-airflow-providers-http',
        'requests'
    ]
    for package in required_packages:
        check_package_installed(package)

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Verification DAG definition
with DAG(
    dag_id='verify_email_sms_dag',
    default_args=default_args,
    description='A DAG to verify email_sms_dag packages',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['verification'],
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    verify_packages = PythonOperator(
        task_id='verify_packages',
        python_callable=verify_dag_packages
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> verify_packages >> end


