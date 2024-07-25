from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
import random

from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup


def get_decision():
    sum = 0
    for i in range(3):
        sum += random.randint(1,6)
    if sum <= 10:
        return 'taskgroup_1.task_1'
    else:
        return 'taskgroup_1.task_2'


with DAG(
    dag_id='dag_tutorial',
    start_date=datetime(2024, 7, 1),
    end_date=datetime(2024, 7, 24),
    schedule_interval='@daily'
) as dag:
    source_data = EmptyOperator(
        task_id='source_data'
    )

    with TaskGroup('taskgroup_1') as tg1:
        get_data = EmptyOperator(
            task_id='get_data'
        )

        get_decision = BranchPythonOperator(
            task_id='get_decision',
            python_callable=get_decision
        )

        task_1 = BashOperator(
            task_id='task_1',
            bash_command='echo task_1 selected'
        )

        task_2 = BashOperator(
            task_id='task_2',
            bash_command='echo task_2 selected'
        )

        finish = BashOperator(
            task_id='finished',
            bash_command='echo "Thanh cong !"',
            trigger_rule='none_failed_or_skipped'
        )

        task_A = EmptyOperator(
            task_id='task_A'
        )
        task_B = EmptyOperator(
            task_id='task_B'
        )
        get_data >> get_decision >> [task_1, task_2] >> finish >> [task_A, task_B]

    with TaskGroup('taskgroup_2') as tg2:
        task_C = EmptyOperator(
            task_id='task_C'
        )

        task_D = EmptyOperator(
            task_id='task_D'
        )

        sensing_task = FileSensor(
            task_id='sensing_task',
            filepath='abc.txt',
            fs_conn_id='my_system_file',
            poke_interval=30, # moi 30s tao ra mot bo tim kiem (sensor) 1 lan
            timeout=180 # sau 180s tim kiem 5 lan ( timeout lon qua thi tao ra nhieu sensor dan den xung dot)
        )

        task_C >> task_D >> sensing_task

    final_task = EmptyOperator(
        task_id='final_task'
    )
source_data >> [tg1, tg2] >> final_task




# Phan nhanh branch chay task_1 va task_2 theo ham dieu kien, den finish va chay dong thoi task_A va task B