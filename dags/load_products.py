# File path: dags/load_products.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import csv
import os
from airflow.configuration import conf
dags_folder = conf.get('core', 'dags_folder')
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'load_products_from_csv',
    default_args=default_args,
    description='Load products table from CSV to PostgreSQL',
    schedule_interval=None,
)

# Python function to load data from CSV to PostgreSQL
def fn_load_csv_to_postgres():
    # Path to the CSV file
    csv_file_path = f"{dags_folder}/data/etl_file_products.csv"
    # Establish connection to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='mynewpostgres_connection')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # Create table if not exists
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        price NUMERIC(10, 2),
        description TEXT,
        stock INT
    );
    '''
    cursor.execute(create_table_query)
    
    # Read CSV and insert data
    with open(csv_file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            cursor.execute(
                "INSERT INTO products (id, name, price, description, stock) VALUES (%s, %s, %s, %s, %s)",
                row
            )
    
    # Commit the transaction
    connection.commit()
    cursor.close()
    connection.close()

# Define the task
load_csv_task = PythonOperator(
    task_id='task_load_csv_to_postgres',
    python_callable=fn_load_csv_to_postgres,
    dag=dag,
)

load_csv_task
