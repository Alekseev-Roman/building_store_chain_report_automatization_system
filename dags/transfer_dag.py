import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from dds_transfer import dds_transfer


with DAG(
        dag_id='transfer_dag',
        description='transfer_data_to_dds',
        schedule='@once',
        start_date=datetime.datetime(2023, 7, 12),
        catchup=False
) as dag:

    start_step = EmptyOperator(task_id='transfer_start')

    clean_step = PostgresOperator(
        task_id='dds_cleaner',
        postgres_conn_id='internship_3_db',
        sql='sql/clean_dds.sql'
    )

    create_wrong_schema_step = PostgresOperator(
        task_id='create_wrong_schema',
        postgres_conn_id='internship_3_db',
        sql='sql/create_wrong_dds.sql'
    )

    extract_data_step = PythonOperator(
        task_id='extract_data',
        python_callable=dds_transfer
    )

    end_step = EmptyOperator(task_id='transfer_end')

    start_step >> clean_step >> create_wrong_schema_step >> extract_data_step >> end_step


