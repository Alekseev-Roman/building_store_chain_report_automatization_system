import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator


with DAG(
        dag_id='transfer_to_dds_dag',
        description='transfer_data_to_dds',
        schedule='@once',
        start_date=datetime.datetime(2023, 7, 12),
        catchup=False
) as dag:
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

    extract_data_step = BashOperator(
        task_id='extract_data',
        bash_command='python ${AIRFLOW_HOME}/scripts/dds_transfer.py'
    )

    clean_step >> create_wrong_schema_step >> extract_data_step


