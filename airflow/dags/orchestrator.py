import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/generate')
from insert_records import main

with DAG(
    dag_id='skills_matrix',
    start_date=datetime(2025, 11, 29),
    schedule=timedelta(minutes=1),
    catchup=False,
    tags=['skill_matrix'],
) as dag:
    
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=main
    )