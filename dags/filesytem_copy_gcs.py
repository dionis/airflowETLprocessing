from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

from google.cloud import storage

dag_owner = ''

###
#  Get local store file and Google Cloude Store addresss
#
###

DATASET_ADDRESS = Variable.has_key('PORA_LOCAL_FILES_ADDRESS').get('PORA_LOCAL_FILES_ADDRESS') if  Variable.get('PORA_LOCAL_FILES_ADDRESS', default_var = None) not in ['', None] else '/opt/airflow/datasets/PORA/'

GOOGLE_CLOUD_STORE_DIRECTORY_PORA = Variable.get('GOOGLE_CLOUD_STORE_DIRECTORY_PORA') if  Variable.get('GOOGLE_CLOUD_STORE_DIRECTORY_PORA', default_var = None) not in ['', None]  else 'rawdata'

GOOGLE_CLOUDE_STORE_BUCKET_NAME_PORA = Variable.get('GOOGLE_CLOUDE_STORE_BUCKET_NAME_PORA') if  Variable.get('GOOGLE_CLOUDE_STORE_BUCKET_NAME_PORA', default_var = None) not in ['', None]  else 'pora_jhhc_data_develop'
                       
PORA_DATASET_BIGQUERY_NAME = Variable.get('PORA_DATASET_BIGQUERY_NAME') if  Variable.get('PORA_DATASET_BIGQUERY_NAME', default_var = None) not in ['', None] else 'import_dev'


default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(seconds=6000)
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
with DAG(dag_id='upload_cv_files_to_gcs_rawdata_test',
        default_args=default_args,
        description='',
        start_date = datetime(2024, 9, 10),
        schedule_interval = None,
        catchup=False,
        tags=['']
) as myDag:

    def upload_to_gcs(data_folder,gcs_path,**kwargs):
        data_folder = data_folder
        bucket_name = 'pora_jhhc_data_develop'  # Your GCS bucket name
        gcs_conn_id = 'gbq_key_in_connection'

        # List all CSV files in the data folder
        # Note : you can filter the files extentions with file.endswith('.csv')
        # Examples : file.endswith('.csv')
        #            file.endswith('.json')
        #            file.endswith('.csv','json')

        #print(f"File location using __file__ variable: {os.path.realpath(os.path.dirname(__file__))}")      

        # Upload each CSV file to GCS
        storage.blob._DEFAULT_CHUNKSIZE = 20 * 1024* 1024  # 5 MB
        storage.blob._MAX_MULTIPART_SIZE = 20 * 1024* 1024  # 5 MB

        if not (os.path.exists(data_folder)):
            print(f"Not  exits address in {data_folder}")
        else:
            csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]
            
            for csv_file in csv_files:
                local_file_path = os.path.join(data_folder, csv_file)
                gcs_file_path = f"{gcs_path}/{csv_file}"

                print(f"Copy files from {local_file_path} to {gcs_file_path}...  ")

            

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
                    execution_timeout = timedelta(minutes = 10)
                )
                upload_task.execute( context = kwargs) 

    start = EmptyOperator(task_id='start')

    # @task
    # def task_1():
    #     return ''
    
    #['your-local-dir', 'gcs-destination'],
    upload_to_gcs = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable = upload_to_gcs,
        op_args = ['/opt/airflow/datasets', 'rawdata'],
        provide_context = True,
    )
    
     
    load_csv = GCSToBigQueryOperator(
        
        task_id="gcs_to_bigquery_example",
        bucket="pora_jhhc_data_develop",
        source_objects=["rawdata/insurance_claims_data.csv"],
        destination_project_dataset_table='health_claim.insurance_claims_data',      
        write_disposition="WRITE_TRUNCATE",
        source_format = "csv",
        external_table = False,
        autodetect = True,
        skip_leading_rows = 1
    )
    
    end = EmptyOperator(task_id='end')

    start >> upload_to_gcs >> load_csv >> end#