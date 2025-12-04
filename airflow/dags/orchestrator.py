import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/generate')
from insert_records import main

with DAG(
    dag_id='skills_matrix',
    start_date=datetime(2025, 11, 29),
    schedule=timedelta(minutes=3),
    catchup=False,
    tags=['dev'],
) as dag:
    
    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=main
    )

    task2 = DockerOperator (
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts=[
            Mount(source='/home/agielso/repos/skill-matrix-project/dbt/my_matrix',
                target='/usr/app',
                type='bind'),
            Mount(source='/home/agielso/repos/skill-matrix-project/dbt/profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind'),
        ],
        network_mode='skill-matrix-project_my-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    task1 >> task2 

