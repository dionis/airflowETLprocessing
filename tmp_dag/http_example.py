from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime, timedelta 

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='HTTP_TEST',
        default_args=default_args,
        description='',
        start_date = datetime(year=2024, month=8, day=26),
        schedule_interval= None,
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    simplehttp_task = SimpleHttpOperator(
       task_id="simplehttp_task",
       http_conn_id="airflow_http_id",
       method='GET',
       endpoint='posts',
       data={"id": "4"},
       headers={"Content-Type": "application/json"},
       log_response = True
       # response_check=lambda response: response.json()['json']['priority'] == 5,
       # response_filter=lambda response: json.loads(response.text),
       # extra_options: Optional[Dict[str, Any]] = None,
       # log_response: bool = False,
       # auth_type: Type[AuthBase] = HTTPBasicAuth,
   )

    end = EmptyOperator(task_id='end')

    start >> simplehttp_task  >> end