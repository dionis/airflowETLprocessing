# from airflow.decorators import task
# from airflow.operators.empty import EmptyOperator
# import time
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta 

# dag_owner = ''

# default_args = {'owner': dag_owner,
#         'depends_on_past': False,
#         'retries': 2,
#         'retry_delay': timedelta(minutes=5)
#         }

# PORA_CLAIMSYSTEM_NAME = 'pora_claysistem_ehp-mc400_dag'

# with DAG(
#         dag_id = f"delay_python_{PORA_CLAIMSYSTEM_NAME}_task",
#         default_args=default_args,
#         description='',
#         start_date=datetime(),
#         schedule_interval= None,
#         catchup=False,
#         tags=['']
# ) as my_dag:

#     start = EmptyOperator(task_id='start')

    

#     def CLAIMSYSTEM_ehp_mc400_task():
#         print(f"Running {PORA_CLAIMSYSTEM_NAME} tasks ")

#     delay_python_task = PythonOperator(  task_id ="delay_python_task",
#                                          dag = my_dag,
#                                          python_callable = lambda: time.sleep(seconds = 60*2))
    
#     execute_python_task = PythonOperator(  task_id =f"{PORA_CLAIMSYSTEM_NAME}_task",
#                                          dag = my_dag,
#                                          python_callable = CLAIMSYSTEM_ehp_mc400_task)

#     end = EmptyOperator( task_id= f"pora_{PORA_CLAIMSYSTEM_NAME}_endtask")

#     start >> delay_python_task >> execute_python_task >> end