from datetime import datetime, timedelta 

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 

#Exceptions in Airflow

 # - For omitting a task from the DAG
from airflow.exceptions import AirflowSkipException 

# - For control when task is failed
from airflow.exceptions import AirflowException

# - For control when task is timeout exception
from airflow.exceptions import AirflowTaskTimeout

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 4,
        'retry_delay': timedelta(minutes=5)
        }

#Use enviromental variables

ENV = Variable.get('env')
ID = Variable.get('id')

#For DAG parameters configurations
params =  {'Manual': True, 'Date': '2024-07-30'}

with DAG(dag_id='my_first_day',
        default_args=default_args,
        description= f"My first dag for airflow course with variables ${ENV} and ${ID}",
        start_date=datetime(year=2024, month=7, day=30),
        schedule_interval='0 9 * * *',
        catchup=False,
        tags=['PythonDataFlow'],
        max_active_runs = 2,
        params = params
):

    start = EmptyOperator(task_id='start')

    def first_task_function(**kwargs):
        print(f"Hi from first task function {ENV} and {ID}")
        print('The context im params.Manual is :')
        print(f"{kwargs['params']['Manual']}")

        params = kwargs.get('params', {})
        manual = params.get('Manual', False)

        #if manual:
        #    raise AirflowSkipException("Manual is True and the task was omitted")


    first_task = PythonOperator(
        task_id="first_task",
        python_callable = first_task_function,
        retries = default_args['retries'],
        retry_delay = default_args['retry_delay'],
        provide_context = True
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=lambda: print('Hi from second python operator'),
        retries = default_args['retries'],
        retry_delay = default_args['retry_delay']
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    third_task = PythonOperator(
        task_id="third_task",
        python_callable=lambda: print('Hi from third python operator'),
        retries = default_args['retries'],
        retry_delay = default_args['retry_delay']
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    four_task = PythonOperator(
        task_id="four_task",
        python_callable=lambda: print(f"Hi from four python operator {ENV} and {ID}"),
        retries = default_args['retries'],
        retry_delay = default_args['retry_delay']
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    five_task = PythonOperator(
        task_id="five_task",
        python_callable=lambda: print('Hi from five python operator'),
        retries = default_args['retries'],
        retry_delay = default_args['retry_delay']
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    @task
    def task_1():
        return ''

    end = EmptyOperator(task_id='end')

    # Dependency relation between tasks
    #  serie realtions: first start task , second task_1 definition, third end ta
    #start >> task_1() >> end

    # Parallel relation between tasks
    #start >> first_task >> end
    #start >> second_task >> third_task>>  end

    #Another parallel relation between tasks
    start >> [first_task, second_task] >> third_task >> [four_task, five_task] >> end