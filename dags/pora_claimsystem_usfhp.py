from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import time
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

PORA_CLAIMSYSTEM_NAME = 'pora_claysistem_usfhp_dag'

with DAG(
        dag_id = 'pora_claysistem_usfhp_dag',
        default_args = default_args,
        description='',
        start_date = datetime(day = 5, month = 9,year = 2024),
        schedule_interval = None,
        catchup=False,
        tags=['']
) as my_dag:

    start = EmptyOperator(task_id='start')

    child_start_task1 = ExternalTaskSensor(
        task_id = f"{PORA_CLAIMSYSTEM_NAME}_child_task1",
        external_dag_id = 'pora_preprocessor_dag',
        external_task_id = 'pora_preprocessor_dag_endtask',
        timeout=600,
        allowed_states = ["success"],
        failed_states = ["failed", "skipped"],
        mode = "reschedule",
    )

    def CLAIMSYSTEM_ehp_mc400_task():
        print(f"Running {PORA_CLAIMSYSTEM_NAME} tasks ")

    delay_python_task = PythonOperator(  task_id = f"delay_python_{PORA_CLAIMSYSTEM_NAME}_task",
                                         dag = my_dag,
                                         python_callable = lambda: time.sleep(seconds = 60*2))
    
    execute_python_task = PythonOperator(  
                                        task_id =f"{PORA_CLAIMSYSTEM_NAME}_task",
                                         dag = my_dag,
                                         python_callable = CLAIMSYSTEM_ehp_mc400_task)

    end = EmptyOperator( task_id= f"pora_{PORA_CLAIMSYSTEM_NAME}_endtask")

    child_start_task1 >> start >> delay_python_task >> execute_python_task >> end