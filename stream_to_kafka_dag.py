from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from stream_to_kafka import start_streaming

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'random_people_names',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1
) as dag:
    data_stream_task = PythonOperator(
        task_id='kafka_data_stream',
        python_callable=start_streaming
    )
