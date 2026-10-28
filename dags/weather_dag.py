from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="morocco_weather_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["portfolio"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT} && python src/extract.py daily",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT} && python src/transform.py",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT} && python src/load.py",
    )

    extract >> transform >> load