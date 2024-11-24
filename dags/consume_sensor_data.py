import random
import json
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import (
    dag
)
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import sql

load_dotenv()  # take environment variables from .env.

host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DB_NAME')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

topic_name = 'sensor_data'

def consume_data(message):
    key = json.loads(message.key())
    content = json.loads(message.value())
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        query = sql.SQL("""
                INSERT INTO sensors(sensor_id,timestamp,temperature,humidity,pressure,location)
                VALUES(%(sensor_id)s, %(timestamp)s, %(temperature)s, %(humidity)s, %(pressure)s,%(location)s)
                ON CONFLICT (sensor_id)
                DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    timestamp = EXCLUDED.timestamp,
                    pressure = EXCLUDED.pressure;
        """)
        with conn.cursor() as cursor:
            cursor.execute(query,content)
            conn.commit()
            print(f"Sensor data data inserted successfully for {message.key()}")
    except Exception as e:
        print(f"An error occured while consuming {e}")
        conn.rollback()
        conn.close()

        
@dag(
    dag_id='consume_data_from_kafka',
    start_date= datetime(2024,11,24),
    schedule_interval=timedelta(days=1),
    description='Consume data from Kafka',
    tags= ['kafka','sensor'],
    default_args={
        'owner': 'maodo',
        'depends_on_past':False,
        'backfill':False,
    }

)
def consume_sensor_data():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    consume_sensor_data = ConsumeFromTopicOperator(
        task_id='consume_data',
        topics=[topic_name],
        poll_timeout=20,
        apply_function=consume_data
    )
    start  >> consume_sensor_data >> end

consume_sensor_data()