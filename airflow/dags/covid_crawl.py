from bs4 import BeautifulSoup 
import requests
import pytz
import re
import pandas as pd
import glob
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage
from covid_crawl_schema import schema
from task_templates import (create_empty_table, 
                            insert_job)


default_args ={
    'owner' : 'airflow'
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

URL="https://www.worldometers.info/coronavirus/#main_table"

def get_covid_data_updated_date(url):
    response = requests.get(URL).text
    soup = BeautifulSoup(response, 'lxml')
    last_updated_date_label = soup.find_all(text=re.compile("Last updated"))[0]
    last_updated_date_fulltext = last_updated_date_label.split(': ')[1]
    last_updated_date_str, timezone = last_updated_date_fulltext.rsplit(maxsplit=1)
    last_updated_date = datetime.strptime(last_updated_date_str, '%B %d, %Y, %H:%M').replace(tzinfo=pytz.timezone(timezone))
    return last_updated_date

LAST_UPDATED_DATE = get_covid_data_updated_date(URL)
LAST_UPDATED_DATE_FORMAT = LAST_UPDATED_DATE.strftime('%Y-%m-%d_%H:%M:%S')
PARQUET_FILENAME = f'covid_crawl_{LAST_UPDATED_DATE_FORMAT}.parquet'

PARQUET_OUTFILE = f'{AIRFLOW_HOME}/{PARQUET_FILENAME}'


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'covid_crawl_stg')

EVENTS = ['covid_crawl_events']
TABLE_MAP = { f"{event.upper()}_TABLE" : event for event in EVENTS}

TABLE_NAME = 'covid_crawl'
EXTERNAL_TABLE_MAP = {f"{TABLE_NAME.upper()}_EXTERNAL_TABLE": TABLE_NAME}

MACRO_VARS = {"GCP_PROJECT_ID":GCP_PROJECT_ID, 
              "BIGQUERY_DATASET": BIGQUERY_DATASET
              }

MACRO_VARS.update(TABLE_MAP)
MACRO_VARS.update(EXTERNAL_TABLE_MAP)

def crawl_covid_data_to_parquet(url, last_updated_date, parquet_file):
    # Make a GET request to fetch the raw HTML content
    response = requests.get(url).text    

    # convert html to pandas dataframe, source: https://python.tutorialink.com/a-problem-with-web-scraping-using-python-beautifulsoup-and-pandas-read_html/
    covid_live_df = pd.read_html(response, attrs={'id': 'main_table_countries_today'}, displayed_only=False)[0]
    covid_live_df['TotalCases'] = covid_live_df['TotalCases'].astype('float')
    covid_live_df.rename(columns={'1 Caseevery X ppl': 'one_case_every_x_ppl'}, inplace=True)
    covid_live_df.rename(columns={'1 Deathevery X ppl': 'one_death_every_x_ppl'}, inplace=True)
    covid_live_df.rename(columns={'1 Testevery X ppl': 'one_test_every_x_ppl'}, inplace=True)
    covid_live_df['last_updated_date'] = last_updated_date

    # convert to parquet
    covid_live_df.to_parquet(parquet_file)

def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload the downloaded file to GCS
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

with DAG(
    dag_id = f'covid_crawl_dag',
    default_args = default_args,
    description = f'Data pipeline to crawl covid live data from worldometers.info/coronavirus',
    schedule_interval="*/10 * * * *", #At the n-th minute of every hour
    start_date=datetime(2022,5,4),
    catchup=False,
    user_defined_macros=MACRO_VARS,
    tags=['streamify']
) as dag:

    event = EVENTS[0]
    staging_table_name = event
    events_schema = schema[event]
    insert_query = f"{{% include 'sql/{event}.sql' %}}" #extra {} for f-strings escape

    crawl_covid_data_to_parquet_task = PythonOperator(
        task_id = 'crawl_covid_data_to_parquet',
        python_callable = crawl_covid_data_to_parquet,
        op_kwargs = {
            'url' : URL,
            'last_updated_date' : LAST_UPDATED_DATE,
            'parquet_file' : PARQUET_OUTFILE
        }
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable = upload_to_gcs,
        op_kwargs = {
            'file_path' : PARQUET_OUTFILE,
            'bucket_name' : GCP_GCS_BUCKET,
            'blob_name' : f'{TABLE_NAME}/{PARQUET_FILENAME}'
        }
    )

    remove_files_from_local_task=BashOperator(
        task_id='remove_files_from_local',
        bash_command=f'rm {PARQUET_OUTFILE}'
    )

    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id = f'create_external_table',
        table_resource = {
            'tableReference': {
            'projectId': GCP_PROJECT_ID,
            'datasetId': BIGQUERY_DATASET,
            'tableId': TABLE_NAME,
            },
            'externalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{GCP_GCS_BUCKET}/{TABLE_NAME}/*.parquet'],
            },
        }
    )

    execute_insert_query_task = insert_job(event,
                                    insert_query,
                                    BIGQUERY_DATASET,
                                    GCP_PROJECT_ID)

    initate_dbt_task = BashOperator(
        task_id = 'dbt_covid_analysis_initiate',
        bash_command = 'cd /dbt && dbt deps && dbt seed --select country_exclusion --profiles-dir . --target prod'
    )

    execute_dbt_task = BashOperator(
        task_id = 'dbt_covid_analysis_run',
        bash_command = 'cd /dbt && dbt deps && dbt run --profiles-dir . --target prod'
    )

    crawl_covid_data_to_parquet_task >> \
    upload_to_gcs_task >> \
    remove_files_from_local_task >> \
    create_external_table_task >> \
    execute_insert_query_task >> \
    initate_dbt_task >> \
    execute_dbt_task 
