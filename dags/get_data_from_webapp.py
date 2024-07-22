from airflow import DAG, macros

from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd


# SU DUNG OP_KWARGS
# def caculate_stats(input_path, output_path):
#     df = pd.read_json(input_path)
#     df = df.groupby(["date", "user"]).size().reset_index()
#     df.to_csv(output_path)

def caculate_stats(**context):
    df = pd.read_json(context["templates_dict"]["input_path"])
    df = df.groupby(["date", "user"]).size().reset_index()
    df.to_csv(context["templates_dict"]["output_path"])

with DAG(
        dag_id='get_data',
        start_date=datetime(2024, 7, 11),
        end_date=datetime(2024, 7, 16),
        schedule_interval='@daily',
) as dag:
    get_connection = BashOperator(
        task_id='get_connection',
        # bash_command='curl http://{ip container}:5500/events',
        bash_command=('curl -o /var/tmp/events-{{ds}}.json http://airflow-project-app-container-1:5500/events?start_date={{ds}}&end_date={{macros.ds_add(ds, 1)}}') #jinja
    )

    # SU DUNG OP_KWARGS
    # caculate_stats=PythonOperator(
    #     task_id='statistic',
    #     python_callable=caculate_stats,
    #     op_kwargs= {'input_path': '/var/tmp/events-{{ds}}.json', 'output_path': '/var/tmp/events-{{ds}}.csv'}
    # )
    caculate_stats = PythonOperator(
        task_id='statistic',
        python_callable=caculate_stats,
        templates_dict={'input_path': '/var/tmp/events-{{ds}}.json', 'output_path': '/var/tmp/events-{{ds}}.csv'}
    )

get_connection >> caculate_stats
