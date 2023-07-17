from datetime import datetime, timedelta
import logging
from typing import Dict, Any

import requests

import boto3
from botocore.errorfactory import ClientError

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.exceptions import AirflowSkipException

from airflow.decorators import task

logging.basicConfig(level=logging.INFO)

bucket = "sandbox-data-pipeline"
prefix = "snowflake"
run_hr = 0

cities = ['San Francisco', 'Los Angeles', 'New York', 'Chicago', 'London', 'Paris', 'Tokyo']

api_url = "https://weatherapi-com.p.rapidapi.com/current.json"

api_headers = {
    "X-RapidAPI-Key": "7ac551d32emsh7d8a58ece3dba3dp1f6799jsn6c356eacccc5",
    "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}

s3 = boto3.client('s3')


def s3_object_exists(bucket, key):
    exists = True

    try:
        s3.get_object(
            Bucket=bucket,
            Key=key
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            exists = False
        else:
            raise e
    return exists


with DAG(
        dag_id="sandbox_data_pipeline__get_weather",
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        # [END default_args]
        description="Get current weather conditions from weatherAPI.com (via RapidAPI) and send to Snowflake",
        schedule=timedelta(hours=1),
        start_date=datetime(2023, 7, 13, 17),
        catchup=False,
        tags=["sandbox"],
) as dag:
    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish")


    @task(task_id='get_run_hr')
    def push_run_hr_to_xcom(*args, **kwargs):
        ts = kwargs['ts']
        run_hr = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y%m%d%H00')
        return run_hr

    run_hr_task = push_run_hr_to_xcom()

    write_to_snowflake_task = SnowflakeOperator(
        task_id=f'write_conditions',
        snowflake_conn_id='snowflake_admin',
        sql='sql/write_weather.sql',
        params={'bucket': bucket,
                'prefix': prefix
                }
    )

    for city in cities:
        city = city.lower().replace(' ', '_')




        @task(task_id=f"fetch_conditions_for__{city}")
        def fetch_conditions(_city, **kwargs):
            ts = kwargs['ts']
            run_hr = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y%m%d%H00')
            api_querystring = {"q": _city}

            response = requests.get(api_url, headers=api_headers, params=api_querystring)

            s3_key = f"{prefix}/weather/{run_hr}/{_city}.json"

            if not s3_object_exists(bucket, s3_key):
                s3.put_object(
                    Body=str(response.json()),
                    Bucket=bucket,
                    Key=s3_key
                )
                logging.info(f"{s3_key} written to s3://{bucket}")
            else:
                logging.info(f"{s3_key} already exists in s3://{bucket}. Skipping write")
                raise AirflowSkipException


        fetch_task = fetch_conditions(city)
        start_task >> run_hr_task >> fetch_task >> write_to_snowflake_task >> finish_task

    # write_to_snowflake_task >> finish_task
