# File path: dags/etl_dag/process_file.py

import csv
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.configuration import conf
dags_folder = conf.get('core', 'dags_folder')
def load_csv_to_postgres(csv_file_path, postgres_conn_id):
    connection = None
    try:
        # Establish connection to PostgreSQL using Airflow's PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
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
        print("Data loaded successfully.")
    except Exception as e:
       print(f"Error: {e}")
       if connection:
            connection.rollback()
       raise
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    # Define the path to the CSV file
    csv_file_path = os.path.join(dags_folder, '/data/etl_file_products.csv')
    
    # Define the PostgreSQL connection ID
    postgres_conn_id = 'mynewpostgres_connection'
    
    # Load the CSV data into PostgreSQL
    load_csv_to_postgres(csv_file_path, postgres_conn_id)
