import os
import logging
import requests
import json
import time
from typing import Optional, Any
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from callables import  fetch_all_data, disk_to_s3, s3_to_snowflake

# Airflow DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_to_snowflake_pipeline',
    default_args=default_args,
    description='Fetch data from API, upload to S3, and copy to Snowflake',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_all_data',
        python_callable=fetch_all_data,
        op_kwargs={
            'api_base_url': 'https://api.example.com/',
            'resources': ['resource1', 'resource2'],
            'templates_dict': {'ds_nodash': '{{ ds_nodash }}'},
            'out_path': '/filler/api_data',
        },
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=disk_to_s3,
        op_kwargs={
            's3_conn_id': 's3_conn',
            'local_path': '/filler/api_data',
            'base_dir': '/filler',
            'bucket': 'my-s3-bucket',
            'delete_local': False,
        },
    )

    copy_to_snowflake_task = PythonOperator(
        task_id='copy_to_snowflake',
        python_callable=s3_to_snowflake,
        op_kwargs={
            'snowflake_conn_id': 'snowflake_conn',
            'schema': 'RAW_SCHEMA',
            'table_name': 'example_table',
            's3_destination_key': 'my-s3-bucket/api_data/'
        },
    )

    fetch_task >> upload_to_s3_task >> copy_to_snowflake_task
