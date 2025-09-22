from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount

with DAG('dbt_run_dag', start_date=datetime(2025,1,1), schedule_interval='@daily', catchup=False) as dag:
    start = EmptyOperator(task_id='start')

    run_dbt = DockerOperator(
        task_id='run_dbt_models',
        image='ghcr.io/dbt-labs/dbt-postgres:1.4.0',
        api_version='auto',
        auto_remove=True,
        command='dbt run --profiles-dir /usr/app/dbt',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[
            Mount(source='/var/run/docker.sock', target='/var/run/docker.sock', type='bind'),
            Mount(source='/home/airflow/dbt', target='/usr/app/dbt', type='bind')
        ]
    )

    start >> run_dbt
