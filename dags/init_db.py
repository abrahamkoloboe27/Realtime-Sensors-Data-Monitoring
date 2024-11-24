import psycopg2
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.decorators import (
    dag,
    task
)
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DB_NAME')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

@dag(
    start_date=datetime(2024, 11, 24),
    description='Sensor Data table creation',
    tags= ['kafka','sensor'],
    default_args={
        'owner': 'maodo',
        'depends_on_past':False,
        'backfill':False,
    })
def create_sensor_table():
    @task()
    def create_table():
        try:
            conn= psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensors(
                    sensor_id VARCHAR(40) UNIQUE,
                    timestamp TIMESTAMP,
                    temperature NUMERIC(5,2),
                    humidity NUMERIC(5,2),
                    pressure INTEGER,
                    location VARCHAR(40))
                        """)
            conn.commit()
            conn.close()
            print(f"Table successfully created !")
        except Exception as e:
            print(f"Failed to create table : {e}")
            conn.rollback()
            conn.close()
            raise
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    start >> create_table() >> end

create_sensor_table()