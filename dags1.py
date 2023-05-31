from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'pablo',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'FlightDataDAG',
    default_args = default_args,
    description = 'Flight Data',
    schedule = '@daily'
)

task_getData = BashOperator(
    task_id = 'requestData',
    bash_command = 'python3 /home/pablo/final_project/getData.py',
    dag = dag
)

task_copyFile = BashOperator(
    task_id = 'copyFileFromLocal',
    bash_command = 'hadoop fs -copyFromLocal /home/pablo/final_project/data /user/pablo/final_project',
    dag = dag
)

task_extractData = HiveOperator(
    task_id = 'extractParquetFile',
    hql = open('/home/pablo/final_project/script1.sql', 'r').read(),
    hive_cli_conn_id = 'hiveserver2_default',
    dag = dag
)