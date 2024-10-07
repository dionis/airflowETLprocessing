from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow import settings
from airflow import models

dag_owner = ''

def get_execution_date(dt, **kwargs):
    session = settings.Session()
    dr = session.query(DagRun)\
        .filter(DagRun.dag_id == kwargs['task'].external_dag_id)\
        .order_by(DagRun.execution_date.desc())\
        .first()
        
    return dr.execution_date if dr != None else dt

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='ets_demo_2',
        default_args=default_args,
        description='',
        schedule_interval=None,
        start_date = days_ago(2),
        tags=['demo'],
        catchup=False,
) as dag2:

    start = EmptyOperator(task_id='start')

    wait = ExternalTaskSensor(
        task_id="wait_for_DAG1",
        external_dag_id='ets_demo_1',
        execution_date_fn=get_execution_date
    )

    something = BashOperator(
        task_id="do_someting",
        bash_command="echo something"
    )
    end = EmptyOperator(task_id='end')

    start >> wait >> something >> end