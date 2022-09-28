import os
import logging

from datetime import datetime

# inbuilt airflow operators
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# to interact with GCS storage
from google.cloud import storage

# for processing csv to parquet file
import pyarrow.csv as pv
import pyarrow.parquet as pq

# import some env variables that we have in docker-compose.yml 
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
# The second argument of each `.get` is what it will default to if it's empty.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# url prefix for yellow, green and FHV taxi rides
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

# a function to convert csv file to parquet, will be used for processing taxi zones data
def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)

# a function to upload parquet file to google cloud storage
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# a function to donwload, parquetize and upload yellow, green and FHV taxi data to google cloud storage
def donwload_parquetize_upload_dag(
    dag,
    url_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_parquet_path_template}"
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_parquet_path_template}"
        )

        download_dataset_task >> local_to_gcs_task >> rm_task



# processing yellow_taxi
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_v1",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_URL_TEMPLATE,
    local_parquet_path_template=YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=YELLOW_TAXI_GCS_PATH_TEMPLATE
)

# processing green_taxi
GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_GCS_PATH_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

green_taxi_data_dag = DAG(
    dag_id="green_taxi_data_v1",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=green_taxi_data_dag,
    url_template=GREEN_TAXI_URL_TEMPLATE,
    local_parquet_path_template=GREEN_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=GREEN_TAXI_GCS_PATH_TEMPLATE
)

# processing FVH_taxi
FHV_TAXI_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_taxi_data_dag = DAG(
    dag_id="hfv_taxi_data_v1",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_TAXI_URL_TEMPLATE,
    local_parquet_path_template=FHV_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=FHV_TAXI_GCS_PATH_TEMPLATE
)

# processing zones_data
ZONES_URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"

with DAG(
    dag_id="zones_data_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {ZONES_URL_TEMPLATE} > {ZONES_CSV_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": ZONES_CSV_FILE_TEMPLATE,
            "dest_file": ZONES_PARQUET_FILE_TEMPLATE,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": ZONES_GCS_PATH_TEMPLATE,
            "local_file": ZONES_PARQUET_FILE_TEMPLATE,
        },
    )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {ZONES_CSV_FILE_TEMPLATE} {ZONES_PARQUET_FILE_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task