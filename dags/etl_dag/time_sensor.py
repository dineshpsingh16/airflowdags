from datetime import timedelta, datetime
from pytz import timezone
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.dates import days_ago

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],  # Replace with your recipient email(s)
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'execution_timeout': timedelta(seconds=10),
}

with DAG(
    dag_id='example_dag_time_sensor',
    default_args=default_args,
    description='DAG demonstrating various operators',
    start_date=days_ago(1),
    schedule_interval=None,  # Run manually
) as dag:
    # Get current IST time
    ist_timezone = timezone('Asia/Kolkata')
    current_time_ist = datetime.now(ist_timezone)

    # Add 5 seconds to IST time
    target_time_ist = (current_time_ist + timedelta(seconds=5)).time()

    # Print the target time in IST format
    logger.info(f"Current time IST: {current_time_ist}")
    logger.info(f"Target time for TimeSensor: {target_time_ist}")

    wait_for_time = TimeSensor(
        task_id='wait_for_time',
        timeout=10,
        soft_fail=True,
        target_time=target_time_ist,
    )

    # Example downstream task (if any)
    dummy_task = BashOperator(
        task_id='dummy_task',
        bash_command='echo "Task executed"',
    )

    wait_for_time >> dummy_task
