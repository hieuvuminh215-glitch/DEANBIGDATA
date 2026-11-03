
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from activity_generator import start_activity_streaming


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="user_activity_stream",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1
) as dag:

    activity_stream_task = PythonOperator(
        task_id="generate_user_activity",
        python_callable=start_activity_streaming
    )
