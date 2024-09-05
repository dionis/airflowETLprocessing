from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import time
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from datetime import datetime, timedelta 

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='pora_preprocessor_dag',
        default_args=default_args,
        description='',
        start_date = datetime(day = 5, month = 9, year = 2024),
        schedule_interval = None,
        catchup = False,
        tags=['']
) as my_dag:

    start = EmptyOperator(task_id='start')

    def delay_time():
        return lambda: time.sleep(300)
    delay_python_task = PythonOperator(  task_id ="delay_python_task",
                                         dag = my_dag,
                                         python_callable =  delay_time)

    end = EmptyOperator( task_id = 'pora_preprocessor_dag_endtask')


    ###----------------------Markert to diferen DAG -------------------------
   
    pora_preprocess_parent_facet_task = ExternalTaskMarker(
        task_id="pora_preprocess_parent_facet_task",
        external_dag_id="pora_claysistem_facet_dag",
        external_task_id="pora_claysistem_facet_dag_child_task1",
    )

    pora_preprocess_parent_usfhp_task = ExternalTaskMarker(
        task_id="pora_preprocess_parent_usfhp_task",
        external_dag_id="pora_claysistem_usfhp_dag",
        external_task_id="pora_claysistem_usfhp_dag_child_task1",
    )

    start >> delay_python_task >> end