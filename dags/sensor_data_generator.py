import random
import json
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.decorators import (
    dag
)
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from dotenv import load_dotenv
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

load_dotenv()  # take environment variables from .env.

host = os.getenv('HOST')
port = os.getenv('PORT')
dbname = os.getenv('DB_NAME')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

topic_name = 'sensor_data'


def generate_data(num_rows:int) :
    locations:list = ['Dakar', 'Thies', 'Tamba']
    for i in range(num_rows) :
        yield (
            json.dumps(i),
            json.dumps(
                {
                    "sensor_id" : f"sensor_0{i}",
                    "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "temperature" : round(random.uniform(20.0,80.0),2),
                    "humidity" : round(random.uniform(30.0,60.0),2),
                    "pressure" : random.randint(1, 300),
                    "location" : random.choice(locations)
                }
            )
        )

        
@dag(
    dag_id='stream_data_to_kafka',
    start_date= datetime(2024,11,24),
    schedule_interval=timedelta(minutes=10),
    description='Stream data to Kafka',
    tags= ['kafka','sensor'],
    default_args={
        'owner': 'maodo',
        'depends_on_past':False,
        'backfill':False,
    }

)
def produce_sensor_data():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    produce_data = ProduceToTopicOperator(
        task_id='produce_data',
        topic=topic_name,
        poll_timeout=10,
        producer_function=generate_data,
        producer_function_args=[100]
    )

    consume_data = TriggerDagRunOperator(
        task_id='consume_sensor_data',
        trigger_dag_id='consume_data_from_kafka'
        )
    start >> produce_data >> end >> consume_data

produce_sensor_data()