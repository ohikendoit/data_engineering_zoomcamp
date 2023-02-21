import os
from datetime import datetime
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "LocalIngestionDag",
    #Interval to execute this job
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2021, 1, 1)
)

#Change the url to a functioning one
URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_2021-01.csv.gz'


with local_workflow:

    #first component - get data
    wget_task = BashOperator(
        task_id = 'wget',
        #bash_command = f'wget {url} -O {AIRFLOW_HOME}/output.csv"
        #bash_command='echo "{{ execution_date.strftime(\'%Y-%m') }}"'
    )

    #second component - ingest data
    ingest_task = BashOperator(
        task_id = 'ingest',
        bash_command = f'ls {AIRFLOW_HOME}'
    )

    #our workflow
    wget_task >> ingest_task