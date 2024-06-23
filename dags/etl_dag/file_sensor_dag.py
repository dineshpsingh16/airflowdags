from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import logging
import os
from airflow.configuration import conf
import random
import string

chars = string.ascii_letters + string.digits
random_string = ''.join(random.choice(chars) for _ in range(8))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
dags_folder = conf.get('core', 'dags_folder')

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'file_sensor_dag',
    default_args=default_args,
    description='A simple DAG with a FileSensor and a Python task to process the file',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the FileSensor task
file_sensor = FileSensor(
    task_id='check_for_file',
    filepath='etl_file_input.csv',
    fs_conn_id='fs_default',
    poke_interval=10,
    timeout=60,
    dag=dag,
)

# Function to call process_file.py
def process_file_fn(dags_folder):
    import subprocess
    import os
    import pandas as pd
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email import encoders
    import os
    from airflow import secrets
    from airflow.configuration import conf
    from traceback import print_exc
    from reportlab.pdfgen import canvas  # Install reportlab library: pip install reportlab
    dags_folder = conf.get('core', 'dags_folder')
    VAR_ENV_PREFIX = "AIRFLOW_VAR_"
    EMAIL_SEND_PASSWORD=os.environ.get(VAR_ENV_PREFIX + "EMAIL_SEND_PASSWORD")
    EMAIL_SEND_LOGIN=os.environ.get(VAR_ENV_PREFIX + "EMAIL_SEND_LOGIN")    
    script_path = os.path.join(dags_folder, 'etl_dag', 'process_file.py')
    def generate_pdf_report(data,reportpath):
        """
        Generates a simple PDF report using reportlab
        You can customize this function to create your desired report layout
        """

        # Create a new PDF document
        pdf = canvas.Canvas(reportpath)

        # Set title and other formatting options
        pdf.setFont("Helvetica", 16)
        pdf.drawString(100, 700, "Processed CSV Data Report")

        # Add table headers
        pdf.drawString(50, 650, "Column 1")
        pdf.drawString(250, 650, "Column 2")
        # ... (add more headers if needed)

        # Write data from DataFrame
        y_pos = 600
        for index, row in data.iterrows():
            pdf.drawString(50, y_pos, str(row[0]))  # Access first column data
            pdf.drawString(250, y_pos, str(row[1]))  # Access second column data
            # ... (add more data access for other columns)
            y_pos -= 20

        # Save the PDF document
        pdf.save()

    def process_csv():
        input_path = f"{dags_folder}/data/etl_file_input.csv"
        output_path = f"{dags_folder}/data/etl_file_output.csv"
        reportpath = '/usr/local/airflow/dags/data/etl_file_output.csv'
        # Load the CSV file
        df = pd.read_csv(input_path)
        
        # Here you can add any processing to the dataframe if needed
        # For now, we just save it to the output file
        
        # Save the dataframe to a new CSV file
        # df.to_csv(output_path, index=False)
        generate_pdf_report(df.copy(),reportpath) 
        # Send email with attachment
        send_email_with_attachment(input_path)
        # os.remove(input_path)
        # os.remove(output_path)
        # os.remove(reportpath)
    def send_email_with_attachment(file_path):
        sender_email = "dineshpsingh16@gmail.com"  # Replace with your Yahoo email
        receiver_email = "dineshpsingh@yahoo.com"
        subject = "Processed CSV File"
        body = "Please find the attached processed CSV file."
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        
        
        try:
            server = smtplib.SMTP_SSL('smtp-relay.brevo.com', 465)
            # server.starttls()
            server.login(EMAIL_SEND_LOGIN, EMAIL_SEND_PASSWORD)  # Replace with generated App Password
            text = msg.as_string()
            # Attach CSV file
            with open(file_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file_path)}")
                msg.attach(part)        
            server.sendmail(sender_email, receiver_email, text)
            server.quit()
        except Exception as e:
            print(f"Failed to send email: {e}")
            print_exc()

        result = subprocess.run(['python', script_path], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Script {script_path} failed with error: {result.stderr}")
    process_csv()
# Define the PythonVirtualenvOperator task
process_file = PythonVirtualenvOperator(
    task_id='process_file',
    python_callable=process_file_fn,
    requirements=["pandas","reportlab"],  # Add other requirements if needed
    system_site_packages=False,
     op_args=[dags_folder],
    dag=dag,
)

# Optional: Define the end task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)
# Log the DAGs folder name
logger.info(f"DAGs folder: {dags_folder}")

# Log the list of files in the DAGs folder
logger.info(f"List of files in the DAGs folder: {os.listdir(dags_folder)}")

# Set the task dependencies
file_sensor >> process_file >> end_task
