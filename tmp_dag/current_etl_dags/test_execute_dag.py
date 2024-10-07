

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from airflow.utils.dates import days_ago

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dag_python_execute',
        default_args=default_args,
        description='',
        start_date = days_ago(2),
        schedule_interval = '*/5 * * * *',
        catchup=False,
        tags=['']
) as my_dag:

    # start = EmptyOperator(task_id='start_execute_test')

    # @task
    # def task_1():
    #     return ''

    end = EmptyOperator(task_id='end')

    # start >> task_1() >> end


    def delay_time(**kwargs):
        minutes = kwargs['minutes']
        lambda: time.sleep(seconds = 60 * minutes)

    delay_python_task = PythonOperator(  task_id = f"delay_python_execute_task",
                                         dag = my_dag,
                                         python_callable = delay_time,
                                         op_kwargs={'minutes': 5}
                                        )

    delay_python_task >> end
    

