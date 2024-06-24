"""
### Example HTTP operator and sensor
"""
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import json

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_http_operator', default_args=default_args)

dag.doc_md = __doc__

# t1, t2 and t3 are examples of tasks created by instatiating operators

# sensor = HttpSensor(
#     task_id='http_sensor_check',
#     conn_id='http_connection',
#     endpoint='',
#     params={},
#     response_check=lambda response: True if "Google" in response.content else False,
#     poke_interval=5,
#     dag=dag)

# sensor