# util/tasks.py


def task1_fun(csv_data_json):
    import pandas as pd

    # Convert JSON back to DataFrame
    df = pd.read_json(csv_data_json)
    
    # Initialize balance column
    df['balance'] = 0.0
    
    # Calculate balance based on debit and credit columns
    balance = 0.0
    for index, row in df.iterrows():
        if pd.notnull(row['debit']):
            balance -= row['debit']
        if pd.notnull(row['credit']):
            balance += row['credit']
        df.at[index, 'balance'] = balance
    
    # Print for debugging
    print("Updated DataFrame with balance column:")
    print(df)
    
    # Return updated DataFrame as JSON
    return df.to_json()

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

def send_email(**kwargs):
    from airflow.operators.email import EmailOperator
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
