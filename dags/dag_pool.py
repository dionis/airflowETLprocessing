from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta 

dag_owner = 'DAG_POLL'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

table_info = {
    1: {"message":"I'm the task 1"},
    2: {"message":"I'm the task 2"},
    3: {"message":"I'm the task 3"},
    4: {"message":"I'm the task 4"},
    5: {"message":"I'm the task 5"},
}

def create_task(message):
    def task_callable():
        print(message)
    return task_callable



with DAG(dag_id='DAG_POLL',
        default_args=default_args,
        description='DAG_POLL',
        start_date=datetime(2024,8,14),
        schedule_interval='0 9 * * *',
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')


    # python_task_1 = PythonOperator(
    #     task_id="python_task_1",
    #     python_callable=lambda: print('Hi from python operator python_task_1'),
    #     pool = 'learn_pool',
    #     # op_kwargs: Optional[Dict] = None,
    #     # op_args: Optional[List] = None,
    #     # templates_dict: Optional[Dict] = None
    #     # templates_exts: Optional[List] = None
    # )

   
    # python_task_2 = PythonOperator(
    #     task_id="python_task_34",
    #     python_callable=lambda: print('Hi from python operator python_task_2'),
    #     pool = 'learn_pool',
    #     # op_kwargs: Optional[Dict] = None,
    #     # op_args: Optional[List] = None,
    #     # templates_dict: Optional[Dict] = None
    #     # templates_exts: Optional[List] = None
    # )

    # python_task_3 = PythonOperator(
    #     task_id="python_task_3",
    #     python_callable=lambda: print('Hi from python operator python_task_3'),
    #     pool = 'learn_pool',
    #     # op_kwargs: Optional[Dict] = None,
    #     # op_args: Optional[List] = None,
    #     # templates_dict: Optional[Dict] = None
    #     # templates_exts: Optional[List] = None
    # )

    # python_task_4 = PythonOperator(
    #     task_id="python_task_4",
    #     python_callable=lambda: print('Hi from python operator python_task_4'),
    #     pool = 'learn_pool',
    #     # op_kwargs: Optional[Dict] = None,
    #     # op_args: Optional[List] = None,
    #     # templates_dict: Optional[Dict] = None
    #     # templates_exts: Optional[List] = None
    # )

    # python_task_5 = PythonOperator(
    #     task_id="python_task_5",
    #     python_callable=lambda: print('Hi from python operator python_task_5'),
    #     pool = 'learn_pool',
    #     # op_kwargs: Optional[Dict] = None,
    #     # op_args: Optional[List] = None,
    #     # templates_dict: Optional[Dict] = None
    #     # templates_exts: Optional[List] = None
    # )
   

   
    end = EmptyOperator(task_id='end')

    #start >> [python_task_1, python_task_2, python_task_3, python_task_4, python_task_5] >> end

    #####
    ## How to use loop for task creator
    #
    #
    #####

    previos_task = start
    for tabla, info in table_info.items():
        task_id = f'task_{tabla}'
        message = info.get('message')
        python_task = PythonOperator(
            task_id=task_id,  python_callable = create_task(message),
            pool = 'learn_pool'
        )
        start >> python_task >> end
        
        #previos_task >> python_task
        #previos_task = python_task
    
    #python_task >> end

    ## On these form the task was created in serie

    ##If I want to create in parallel way 
    #
    #    Erase 
    #     previos_task >> python_task
    #      previos_task = python_task
    #
    #    python_task >> end
    #
    #  And set 
    #   start >> python_task >> end
    