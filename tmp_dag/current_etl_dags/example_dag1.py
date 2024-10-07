from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
from airflow.utils.dates import days_ago

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='ets_demo_1',
        default_args=default_args,
        description='',
        schedule_interval=None,
        start_date=days_ago(2),
        catchup=False,
        tags=['demo']
) as dag1:

    start = EmptyOperator(task_id='start')

    task_1  =  BashOperator(
        task_id="procrastinate",
        bash_command="sleep 10m"
    )
    end = EmptyOperator(task_id='end')

    start >> task_1 >> end