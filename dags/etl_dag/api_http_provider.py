from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash_operator import BashOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],  # Replace with your recipient email(s)
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_timeout': timedelta(minutes=1),  # Increased timeout for the task
}
# Set DAG details
with DAG(
    dag_id='http_dag',
    start_date=datetime(2024, 6, 25),  # Schedule to start tomorrow
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run daily
) as dag:
    
    # Wait for API to be available (optional, replace with actual endpoint)
    # api_available = HttpSensor(
    #     task_id='wait_for_api',
    #     http_conn_id='my_http_api',  # Replace with your HTTP connection ID
    #     endpoint='/health',
    # )

    # # Fetch data from the API (replace with actual endpoint and data format)
    # fetch_data = SimpleHttpOperator(
    #     task_id='fetch_data',
    #     http_conn_id='my_http_api',
    #     endpoint='/data',
    #     method='GET',
    #     response_check=lambda response: response.json()["status"] == "success",  # Check for success
    #     xcom_push=True,  # Push response to XCom for next task
    # )

    # # Process and store data in a file
    process_data = BashOperator(
        task_id='process_data',
        bash_command='''
        # Replace with your actual data processing and storage logic
        echo "{{ ti.xcom_pull(task_ids='fetch_data') }}" > /tmp/data.txt
        ''',
    )

# Define task dependencies
# api_available >> fetch_data >> process_data
process_data