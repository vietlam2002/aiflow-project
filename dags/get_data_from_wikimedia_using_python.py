from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from urllib import request

# def print_context(**kwargs):
#     print(f'Day la thong so cua kwargs: {kwargs}')

def get_data(**kwargs):
    year = kwargs["year"]
    month = kwargs["month"]
    day = kwargs["day"]
    hour = kwargs["hour"]
    output_path = kwargs["output_path"]

    url = (f"https://dumps.wikimedia.org/other/pageviews/"
           f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz")

    request.urlretrieve(url, output_path)

def fetch_data(**kwargs):
    pagenames = kwargs["pagenames"]
    fetch_path = kwargs["fetch_path"]
    path_sql_execute = kwargs["path_sql_execute"]
    _date = kwargs["_date"]

    result = dict.fromkeys(pagenames, 0)
    with open(fetch_path, 'r') as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == 'en' and page_title in pagenames:
                result[page_title] = view_counts


    with open(path_sql_execute, 'w') as f:
         for pagename, pageviewcounts in result.items():
             f.write(
                 "INSERT INTO page_view_counts VALUES ('" + pagename + "', " + pageviewcounts + ", '" + _date + "');\n"
             )



with DAG(
    dag_id='wikimedia-analyst',
    start_date=datetime(2024, 7,  17),
    end_date=datetime(2024, 7, 18),
    schedule_interval='@hourly',
    template_searchpath='/tmp/var/'
) as dag:

    data_wikimedia_from_pythoncode = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        op_kwargs={
            "year": "{{execution_date.year}}",
            "month": "{{execution_date.month}}",
            "day": "{{execution_date.day}}",
            "hour": "{{execution_date.hour}}",
            "output_path": "/tmp/var/wikipageviews.gz"
        }
    )

    extract_gz = BashOperator(
        task_id='extract_gz',
        bash_command='gunzip --force /tmp/var/wikipageviews.gz',
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

    fetch_data_into_postgres = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={
            "pagenames": ["Google", "Amazon", "Microsoft", "Apple", "Facebook"],
            "fetch_path": "/tmp/var/wikipageviews",
            "path_sql_execute": "/tmp/var/insert_query.sql",
            "_date": "{{execution_date}}"
        }
    )

    write_to_postgres = PostgresOperator(
        task_id='write_to_postgres',
        postgres_conn_id='postgres_conn',
        sql='insert_query.sql'
    )

data_wikimedia_from_pythoncode >> extract_gz >> create_table_postgres >> fetch_data_into_postgres >> write_to_postgres