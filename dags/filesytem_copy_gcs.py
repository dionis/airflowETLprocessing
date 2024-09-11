from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

############################################################
#
#  Bibliografy:
#     https://medium.com/@ossama.assaghir/uploading-multiple-files-to-google-cloud-storage-using-airflow-a19c9126bcd9
#
#    Create a GCP conection for working with BigQuery and Google Cloud Storage (GCS)
#
#      https://data-ai.theodo.com/blog-technique/create-bigquery-data-pipelines-with-airflow-and-docker
############################################################
with DAG(dag_id='upload_cv_files_to_gcs_rawdata',
        default_args=default_args,
        description='',
        start_date = datetime(2024, 9, 10),
        schedule_interval = None,
        catchup=False,
        tags=['']
) as myDag:
    def upload_to_gcs(data_folder,gcs_path,**kwargs):
        data_folder = your-local-dir
        bucket_name = 'your-bucket-name'  # Your GCS bucket name
        gcs_conn_id = 'your-gcp-connection-name'

        # List all CSV files in the data folder
        # Note : you can filter the files extentions with file.endswith('.csv')
        # Examples : file.endswith('.csv')
        #            file.endswith('.json')
        #            file.endswith('.csv','json')

        csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

        # Upload each CSV file to GCS
        for csv_file in csv_files:
            local_file_path = os.path.join(data_folder, csv_file)
            gcs_file_path = f"{gcs_path}/{csv_file}"

            #######################################################################
            #
            # The LocalFilesystemToGCSOperator is an Airflow operator designed specifically 
            # for uploading files from a local filesystem to a GCS bucket.
            #
            ###################################################################

            upload_task = LocalFilesystemToGCSOperator(
                task_id = f'upload_to_gcs',
                src = local_file_path,
                dst = gcs_file_path,
                bucket = bucket_name,
                gcp_conn_id = gcs_conn_id,
            )
            upload_task.execute(context=kwargs) 

    start = EmptyOperator(task_id='start')

    # @task
    # def task_1():
    #     return ''
    
    #['your-local-dir', 'gcs-destination'],
    upload_to_gcs = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable=upload_to_gcs,
        op_args = ['../datasets', 'pora_rawdata'],
        provide_context = True,
    )

    end = EmptyOperator(task_id='end')

    start >> upload_to_gcs >> end#