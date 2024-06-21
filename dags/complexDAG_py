from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.sensors.sqlite import SqliteSensor  # Assuming SQLite DB
from random import randint

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 21),
}

with DAG(
    dag_id='multi_task_dag',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or with a scheduler
) as dag:

    # Define your operational tasks here (replace with actual functions)
    def send_email():
        # Implement logic to send email
        print("Sending email...")

    def generate_pdf():
        # Implement logic to generate PDF
        print("Generating PDF...")

    def send_wa_message():
        # Implement logic to send WhatsApp message
        print("Sending WhatsApp message...")


    def conditional_task():
        """
        This function randomly returns True or False.
        """
        # Generate a random integer between 0 and 1
        random_value = randint(0, 1)
    
        # Return True if the random value is 0, False otherwise
        return bool(random_value)
    # Define your conditional task logic here (replace with actual function)
    def conditional_task():
        # Implement logic to determine next task based on condition
        # This function should return the task_id of the next task to run
        if some_condition():
            return 'task_to_run_if_true'
        else:
            return 'task_to_run_if_false'

    # Define sensor task to wait for DB record update
    wait_for_db_update = SqliteSensor(
        task_id='wait_for_db_update',
        sql="SELECT COUNT(*) FROM your_table WHERE updated = 1",
        conn_id='your_sqlite_conn_id',  # Replace with your connection details
    )

    # Operational task example
    operational_task = PythonOperator(
        task_id='operational_task',
        python_callable=send_email,
    )

    # Conditional task example
    conditional_branch = PythonOperator(
        task_id='conditional_branch',
        python_callable=conditional_task,
        provide_context=True,  # Pass execution context to conditional function
    )

    # Downstream tasks based on conditional logic (replace with actual tasks)
    task_to_run_if_true = DummyOperator(
        task_id='task_to_run_if_true',
    )

    task_to_run_if_false = DummyOperator(
        task_id='task_to_run_if_false',
    )

    # Set up task dependencies
    wait_for_db_update >> operational_task >> conditional_branch
    conditional_branch >> [task_to_run_if_true, task_to_run_if_false]
