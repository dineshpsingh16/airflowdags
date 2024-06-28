# /path/to/your/dag_with_xcom_csv_pdf_email.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.email import EmailOperator
import subprocess
import os
import glob

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
    # Path to the CSV file in the current DAG directory
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(dag_folder, 'data.csv')
    
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Push the dataframe to XCom
    kwargs['ti'].xcom_push(key='csv_data', value=df.to_json())
    print("Task 1 executed, CSV data pushed to XCom")

def process_data(**kwargs):
    import matplotlib.pyplot as plt
    from fpdf import FPDF    
    import pandas as pd
    # Pull the dataframe from XCom
    csv_data_json = kwargs['ti'].xcom_pull(key='csv_data', task_ids='read_csv')
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
        
        def add_report(self, title, body):
            self.add_page()
            self.chapter_title(title)
            self.chapter_body(body)
    
    pdf = PDF()
    pdf.add_report('Summary', df.describe().to_string())
    pdf.output(report_file)
    
    # Push the report file path to XCom
    kwargs['ti'].xcom_push(key='report_file', value=report_file)
    print("Task 2 executed, PDF report created and path pushed to XCom")

def install_wheel_packages():
    requirements_path = os.path.join(dag_folder, 'requirements.txt')
    # Install packages from the requirements.txt file
    if os.path.exists(requirements_path):
        subprocess.check_call(['pip', 'install', '-r', requirements_path])    
    # Path to the dist folder in the current DAG directory
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    dist_folder = os.path.join(dag_folder, 'dist')

    # Find all wheel files in the dist folder
    wheel_files = glob.glob(os.path.join(dist_folder, '*.whl'))

    # Install each wheel file
    for wheel_file in wheel_files:
        subprocess.check_call(['pip', 'install', wheel_file])

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
install_packages = PythonVirtualenvOperator(
    task_id='install_packages',
    python_callable=install_wheel_packages,
    requirements=['pip'],
    system_site_packages=True,
    dag=dag,
)

# Create Email operator
def send_email(**kwargs):
    # Pull the report file path from XCom
    report_file = kwargs['ti'].xcom_pull(key='report_file', task_ids='process_data')
    
    email = EmailOperator(
        task_id='send_email',
        to='your_email@example.com',
        subject='Airflow DAG Report',
        html_content='Please find the attached report.',
        files=[report_file],
        dag=dag,
    )
    return email.execute(context=kwargs)

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
install_packages >> read_csv_task >> process_data_task >> send_email_task
