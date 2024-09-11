from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta 

def success_email(context):
    task_instance = context['task_instance']
    task_status = 'Success' 
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'inoid2007@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'inoid2007@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': False,
        }

with DAG(dag_id ='pora_mergerplans',
        default_args=default_args,
        description='',
        start_date = datetime(day = 5, month = 9,year = 2024),
        schedule_interval = None,
        catchup=False,
        tags=['']
) as my_dag:

    start = EmptyOperator(task_id='start')

    ##Sensor waiting for end task in claimsystem ehp-mc400
    
    ##Sensor waiting for end task in claimsystem facet

    ##Sensor waiting for end task in claimsystem pp-mc400

    ##Sensor waiting for end task in claimsystem usfhp
    def merger_task():
        print(f"Running mergerplans tasks ")

    execute_python_task = PythonOperator(  
                                        task_id =f"mergerplan_continue_task",
                                         dag = my_dag,
                                         python_callable = merger_task)
    end = EmptyOperator(task_id='end')

    

    #Set in paralel execution of sensor task and another task notified continue execution of the da

    child_parent_facet_task1 = ExternalTaskSensor(
        task_id = f"child_parent_facet_task1",
        external_dag_id = 'pora_claysistem_facet_dag',
        external_task_id = 'delay_python_pora_claysistem_facet_dag_task',
        execution_delta = timedelta(minutes = 2)
    )

    # child_parent_usfhp_task1 = ExternalTaskSensor(
    #     task_id = f"child_parent_usfhp_task1",
    #     external_dag_id = 'pora_claysistem_usfhp_dag',
    #     external_task_id = 'pora_claysistem_usfhp_dag_endtask',
    #     allowed_states = ["success"],
    #     failed_states = ["failed", "skipped"],
    #     mode = "reschedule",
    # )

    child_parent_facet_task1  >> start >> execute_python_task >> end