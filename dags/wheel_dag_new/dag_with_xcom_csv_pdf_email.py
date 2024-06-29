# /path/to/your/dag_with_xcom_csv_pdf_email.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.email import EmailOperator
import subprocess
from wheeldagutil.tasks import  install_requirements, read_csv, task1_fun_operator, process_data, send_email

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



# Create Python operators
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
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
    python_callable=send_email,
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

log_dags_dir_task = PythonOperator(
    task_id='log_dags_dir',
    python_callable=log_dags_directory_contents,
    dag=dag,
)

# Set task dependencies
log_dags_dir_task >> install_packages_task >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task
