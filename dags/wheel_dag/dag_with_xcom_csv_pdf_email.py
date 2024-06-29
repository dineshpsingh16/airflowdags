# /path/to/your/dag_with_xcom_csv_pdf_email.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.email import EmailOperator
import subprocess

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
    'example_dag_with_xcom_csv_pdf_email',
    default_args=default_args,
    description='An example DAG with Python operators, custom packages, XCom, CSV processing, PDF report, and email notification',
    schedule_interval=None,
)

# Define Python functions
def read_csv(**kwargs):
    import pandas as pd
    import os
    # Path to the CSV file in the current DAG directory
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_folder, 'data.csv')
    
    # Read the CSV file
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Failed to read CSV file: {e}")
        # Assign sample data if reading CSV fails
        sample_data = {
            'transaction details': ['Transaction 1', 'Transaction 2', 'Transaction 3'],
            'debit': [100, 200, None],
            'credit': [None, None, 300]
        }
        df = pd.DataFrame(sample_data)
    
    # Push the dataframe to XCom
    kwargs['ti'].xcom_push(key='csv_data', value=df.to_json())
    print("Task 1 executed, CSV data pushed to XCom")

def process_data(**kwargs):
    import matplotlib.pyplot as plt
    from fpdf import FPDF    
    import pandas as pd
    # Pull the dataframe from XCom
    csv_data_json = kwargs['ti'].xcom_pull(key='csv_data', task_ids='task1_fun_task')
    df = pd.read_json(csv_data_json)
    
    # Process the data (example: create a summary report)
    report_file = '/tmp/report.pdf'
    
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'CSV Report', 0, 1, 'C')
        
        def chapter_title(self, title):
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, title, 0, 1, 'L')
            self.ln(10)
        
        def chapter_body(self, body):
            self.set_font('Arial', '', 12)
            self.multi_cell(0, 10, body)
            self.ln()
        
        def add_report(self, title, df):
            self.add_page()
            self.chapter_title(title)
            for index, row in df.iterrows():
                self.chapter_body(f"{row['transaction details']} - Debit: {row['debit']} - Credit: {row['credit']} - Balance: {row['balance']}")
    
    pdf = PDF()
    pdf.add_report('Transaction Report', df)
    pdf.output(report_file)
    
    # Push the report file path to XCom
    kwargs['ti'].xcom_push(key='report_file', value=report_file)
    print("Task 2 executed, PDF report created and path pushed to XCom")

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
def send_email(**kwargs):
    # Pull the report file path from XCom
    report_file = kwargs['ti'].xcom_pull(key='report_file', task_ids='process_data')
    
    email = EmailOperator(
        task_id='send_email',
        to='dineshpsingh16@gmail.com',
        subject='Airflow DAG Report',
        html_content='Please find the attached report.',
        files=[report_file],
        dag=dag,
    )
    return email.execute(context=kwargs)

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
def log_dags_directory_contents():
    import os

    dag_folder = os.path.dirname(os.path.abspath(__file__))
    print(f"Current directory: {dag_folder}")
    print(f"Contents of the current directory: {os.listdir(dag_folder)}")

    dist_folder = os.path.join(dag_folder, 'dist')
    if os.path.exists(dist_folder):
        print(f"dist folder found at: {dist_folder}")
        dist_files = os.listdir(dist_folder)
        if dist_files:
            print(f"Contents of dist folder: {dist_files}")
        else:
            print("dist folder is empty")
    else:
        print("dist folder not found")
log_dags_dir_task = PythonOperator(
    task_id='log_dags_dir',
    python_callable=log_dags_directory_contents,
    dag=dag,
)

# Set task dependencies
log_dags_dir_task >> install_packages_task >> read_csv_task >> task1_fun_task >> process_data_task >> send_email_task
