from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator  import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='example_postgres_dag',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=8, day=26),
        schedule_interval = None,
        catchup=False,
        tags=['']
):

    create_table_task =  PostgresOperator(
        task_id='PostgresOperator_create_table_task',
        postgres_conn_id='airflow_postgres_id',
        sql="""
            create table if not exists example_table (
               id serial primary key,
               value  Text not null
            );
          """
        )
    
    intert_table_task =  PostgresOperator(
        task_id='PostgresOperator_insert_table_task',
       postgres_conn_id='airflow_postgres_id',
        sql="""
            insert into example_table (value) values ('example@values');

          """
        )
    
    def print_query_result(**context):
           query_result = context['ti'].xcom_pull(task_ids='run_select_query')

           if query_result != None:
            for row in query_result:
                    print(row)
   
    run_select_query =  PostgresOperator(
        task_id='run_select_query',
        postgres_conn_id='airflow_postgres_id',
        sql="""
           select * from example_table;                        
          """
       )
    
    print_onLocal_result =  PythonOperator(
        task_id="print_result",
        python_callable = print_query_result,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    start = EmptyOperator(task_id='start')


    end = EmptyOperator(task_id='end')

    start >> create_table_task >> intert_table_task >> run_select_query >> print_onLocal_result >> end