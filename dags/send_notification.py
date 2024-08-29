from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

#######
#  Bibliografy
#    Apache Airflow Email Operator Guide - August 2024
#      https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html
#     
#   Airflow Email Operator Guide - August 2024
#      https://www.restack.io/docs/airflow-knowledge-airflow-email-operator-guide
#
#
#   https://www.restack.io/docs/airflow-knowledge-email-operator-example
#
#
#   EMAIL ALERTING WITH AIRFLOW
#      https://medium.com/@chibuokejuliet/email-alerting-with-airflow-c0a5a1f413b4
#
#
#   Outlook 
#       https://www.saleshandy.com/smtp/office-365-smtp-settings/
#
#
#    Airflow send mail source code 
#
#       https://github.com/apache/airflow/blob/main/airflow/utils/email.py
#
#      https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/_modules/airflow/providers/smtp/notifications/smtp.html
#
#
#####

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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'schedule_interval' : 'None',
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('email_notification_example',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:

#Define python operator to execute simple python command
    def python_command1():
      print("How are you?!")

    task1 = PythonOperator(
        task_id = 'execute_python_command',
        python_callable = python_command1,
        on_success_callback = success_email,
        on_failure_callback = failure_email,
        provide_context = True,
        dag = dag
    )

    # Define BashOperators to execute simple bash commands
    task2 = BashOperator(
        task_id = 'execute_bash_command',
        bash_command = 'echo "Hello, world!"',
        on_success_callback = lambda context: success_email(context),
        on_failure_callback = lambda context: failure_email(context),
        dag = dag
    )

    # Define tasks dependencies
    task1 >> task2