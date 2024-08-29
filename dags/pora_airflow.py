from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryTableCheckOperator,
    BigQueryValueCheckOperator,
)

dag_owner = 'PORA_AIRFLOW'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='pora_bigquery_airflow',
        default_args=default_args,
        description='Execute several task for updating tables on PORA Biquery squeme',
        start_date=datetime(day = 29,month = 8, year = 2024),
        schedule_interval = None,
        catchup=False,
        tags=['PORA_BIGQUERY']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''
    #####
    #  Bibliografy about BigQueryJob
    #     https://registry.astronomer.io/providers/apache-airflow-providers-google/versions/latest/modules/BigQueryInsertJobOperator
    #
    #     https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
    #
    #     https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html
    #
    #####
    task_vw_fa_lines_predata = BigQueryInsertJobOperator (
                                    task_id = "task_vw_fa_lines_predata",
                                    configuration = {
                                        "query": {
                                            "query": "select * from vw_fa_lines_predata",
                                            "useLegacySql": False,
                                            "priority": "BATCH",
                                            "destinationTable": {
                                                "projectId": "gen-lang-client-0781718982",
                                                "datasetId": "pora_import",
                                                "tableId": "tb_fa_lines_predata_vw"
                                                },
                                                "defaultDataset": {
                                                     "projectId": "gen-lang-client-0781718982",
                                                     "datasetId": "pora_import",
                                                },
                                                "writeDisposition":"WRITE_APPEND"
                                        }
                                     },
                                   # gcp_conn_id = 'gbq_key_in_connection'   
                                )

    # task_vw_fa_lines_v2 = BigQueryInsertJobOperator(
    #                         task_id = 'task_vw_fa_lines_v2',
    #                         configuration = {
    #                                     "query": {
    #                                         "query": "select * from import_pora.vw_fa_lines_v2",
    #                                         "useLegacySql": False,
    #                                         "priority": "BATCH",
    #                                         "destinationTable": "pora_exports.PORAClient_ClaimDetails_v2_fa",
    #                                     }
    #                                  },
    #                         gcp_conn_id = 'gbq_key_in_connection'
    #                         )

    # task_vw_fa_lines = BigQueryInsertJobOperator(
    #                                 task_id = 'task_vw_fa_lines',
    #                                 configuration = {
    #                                     "query": {
    #                                         "query": "select * from import_pora.vw_fa_lines",
    #                                         "useLegacySql": False,
    #                                         "priority": "BATCH",
    #                                         "destinationTable": "pora_exports.PORAClient_ClaimDetails_fa",
    #                                     }
    #                                  },
    #                         gcp_conn_id = 'gbq_key_in_connection')

    # task_vw_fa_header_props = BigQueryInsertJobOperator(
    #                                 task_id = 'task_vw_fa_header_props',
    #                                 configuration = {
    #                                     "query": {
    #                                         "query": "select * from import.vw_fa_header_props",
    #                                         "useLegacySql": False,
    #                                         "priority": "BATCH",
    #                                         "destinationTable": "pora_import.tb_fa_header_props",
    #                                     }
    #                                  },
    #                                 gcp_conn_id = 'gbq_key_in_connection'
    #                             )

    end = EmptyOperator(task_id='end')

    #[task_vw_fa_lines_v2, task_vw_fa_lines] >> task_vw_fa_header_props >>

    start >> task_vw_fa_lines_predata >>   end