from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta 
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow import settings
from airflow import models

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
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': False,
        }

with DAG(dag_id ='pora_mergeplans',
        default_args = default_args,
        description = '',
        start_date = days_ago(2),
        schedule_interval = None,
       # schedule_interval = '*/5 * * * *',
        catchup = False,
        tags = ['']
) as my_dag:

    start = EmptyOperator(task_id = 'start')

    ##Sensor waiting for end task in claimsystem ehp-mc400
    
    ##Sensor waiting for end task in claimsystem facet

    ##Sensor waiting for end task in claimsystem pp-mc400

    ##Sensor waiting for end task in claimsystem usfhp
    def merger_task():
        print(f" Running mergeplans tasks ")

    execute_python_task = PythonOperator(  
                                        task_id =f"mergerplan_continue_task",
                                         dag = my_dag,
                                         python_callable = merger_task)
   # end = EmptyOperator(task_id='end')

    # end_mergeplan = PythonOperator(  
    #                     task_id =f"end_mergeplan_task",
    #                     dag = my_dag,
    #                     provide_context = True,
    #                     python_callable = success_email)

    end_mergeplan = BashOperator(
        task_id = 'end_mergeplan_task',
        bash_command = 'echo "Send notifications!!!"',
        on_success_callback = lambda context: success_email(context),      
        dag = my_dag
    )

    #Set in paralel execution of sensor task and another task notified continue execution of the da

    # child_parent_facet_task1 = ExternalTaskSensor(
    #     task_id = f"child_parent_facet_task1",
    #     external_dag_id = 'pora_claysistem_facet_dag',
    #     external_task_id = 'delay_python_pora_claysistem_facet_dag_task',
    #     execution_date_fn=lambda dt: dt,
    #     timeout=600,
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     mode='reschedule',
    # )

    # child_parent_usfhp_task1 = ExternalTaskSensor(
    #     task_id = f"child_parent_usfhp_task1",
    #     external_dag_id = 'pora_claysistem_usfhp_dag',
    #     external_task_id = 'pora_claysistem_usfhp_dag_endtask',
    #     execution_date_fn=lambda dt: dt,
    #     timeout=600,
    #     allowed_states=['success'],
    #     failed_states=['failed', 'skipped'],
    #     mode='reschedule',
    # )


    child_parent_facet_task_sensor = ExternalTaskSensor(
        task_id = f"pora_claysistem_facet_dag_endtask",
        external_dag_id = 'pora_claysistem_facet_dag',
        #external_task_id = 'delay_python_execute_task',
        check_existence = True,
        execution_date_fn = get_execution_date,
        #allowed_states=['success'],
        #failed_states=['failed', 'skipped'],
        #mode='reschedule',
    )

    child_parent_usfhp_task = ExternalTaskSensor(
        task_id = f"pora_claysistem_usfhp_dag_endtask",
        external_dag_id = 'pora_claysistem_usfhp_dag',
        #external_task_id = 'delay_python_execute_task',
        check_existence = True,
        execution_date_fn = get_execution_date,
        #allowed_states=['success'],
        #failed_states=['failed', 'skipped'],
        #mode='reschedule',
    )


    [ child_parent_facet_task_sensor, child_parent_usfhp_task ] >> start >> execute_python_task >> end_mergeplan