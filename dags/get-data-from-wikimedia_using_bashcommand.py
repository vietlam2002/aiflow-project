from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# https://dumps.wikimedia.org/other/pageviews/2024/2024-07/pageviews-20240717-010000.gz
with DAG(
    dag_id='wikimedia-analyse',
    start_date=datetime(2024, 7,17),
    end_date=datetime(2024, 7, 18),
    schedule_interval='@hourly'
) as dag:

    data_from_wikimedia = BashOperator(
        task_id='get_data',
        bash_command="curl -o /var/tmp/wikipageviews.gz "
                     "https://dumps.wikimedia.org/other/pageviews/"
                     "{{ execution_date.year }}/"
                     "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
                     "pageviews-{{ execution_date.year }}{{ '{:02}'.format(execution_date.month) }}{{ execution_date.day}}-"
                     "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    )

    extract_gz = BashOperator(
        task_id='extract_gz',
        bash_command='gunzip --force /var/tmp/wikipageviews.gz',
    )

    create_table_postgres = PostgresOperator(
        task_id='create_table_postgres',
        postgres_conn_id='postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS page_view_counts (
                page_name VARCHAR(50) NOT NULL,
                page_view_count INT NOT NULL,
                date_time TIMESTAMP NOT NULL
            )
        """
    )
data_from_wikimedia >> extract_gz >> create_table_postgres







