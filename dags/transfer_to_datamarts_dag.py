import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

# wait_for_transfer_to_dds = ExternalTaskSensor(
#     task_id='wait_for_transfer_to_dds',
#     external_dag_id='transfer_to_dds_dag'
# )

with DAG(
        dag_id='transfer_to_datamarts_dag',
        description='transfer_data_to_datamarts',
        schedule='@once',
        start_date=datetime.datetime(2023, 7, 12),
        catchup=False
) as dag:
    clean_step = PostgresOperator(
        task_id='datamart_cleaner',
        postgres_conn_id='internship_3_db',
        sql='sql/clean_datamarts.sql'
    )

    create_wrong_step = PostgresOperator(
        task_id='create_wrong',
        postgres_conn_id='internship_3_db',
        sql='sql/create_wrong_datamarts.sql'
    )

    extract_data_step = BashOperator(
        task_id='extract_data',
        bash_command='python ${AIRFLOW_HOME}/scripts/datamarts_transfer.py'
    )

    # wait_for_transfer_to_dds >> clean_step >> create_wrong_step >> extract_data_step
    clean_step >> create_wrong_step >> extract_data_step

