import logging
from datetime import datetime, timedelta
from typing import Optional

import boto3
import requests
import str2bool
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from botocore.errorfactory import ClientError

logging.basicConfig(level=logging.INFO)

bucket = "sandbox-data-pipeline"
prefix = "snowflake"
run_hr = 0

api_gateway_url = "https://w51pflsz4l.execute-api.us-west-2.amazonaws.com/test"
api_url = "https://weatherapi-com.p.rapidapi.com/current.json"

api_headers = {
    "X-RapidAPI-Key": "7ac551d32emsh7d8a58ece3dba3dp1f6799jsn6c356eacccc5",
    "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com",
}

s3 = boto3.client("s3")


class SQLExecuteQueryOptionalOperator(SQLExecuteQueryOperator):
    def __init__(self, skip=False, **kwargs):
        super().__init__(**kwargs)
        self.skip = skip

    def execute(self, context):
        logging.info("SKIP=====>" + str(self.skip))
        if self.skip:
            logging.info("received skip=True arg, skipping task")
            raise AirflowSkipException
        else:
            super().execute(context)


def s3_object_exists(bucket: str, key: str, s3: Optional[boto3.client] = None) -> bool:
    """
    Check if an object exists in an S3 bucket.

    Args:
        bucket: The name of the S3 bucket.
        key: The key of the object in the S3 bucket.
        s3: An optional pre-initialized boto3 S3 client.

    Returns:
        True if the object exists, False otherwise.
    """
    if s3 is None:
        s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise e

    return True


@task
def get_run_hr(**kwargs):
    """get run hour from ts. Used in downstream tasks to enforce idempotency"""

    ts = kwargs["ts"]
    run_hr = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y%m%d%H00")
    return run_hr


@task
def get_top_5_cities():
    cities = requests.get(api_gateway_url).json()
    return cities


@task
def fetch_weather(city: str, **kwargs):
    """fetch weather data from weatherapi.com and write to s3
    Args: city: city name to fetch weather data for"""

    ti = kwargs["ti"]
    run_hr = ti.xcom_pull(task_ids="get_run_hr")
    api_querystring = {"q": city}

    response = requests.get(api_url, headers=api_headers, params=api_querystring)

    city = city.lower().replace(" ", "_")
    s3_key = f"{prefix}/weather/{run_hr}/{city}.json"

    if not s3_object_exists(bucket, s3_key):
        s3.put_object(Body=str(response.json()), Bucket=bucket, Key=s3_key)
        logging.info(f"{s3_key} written to s3://{bucket}")
    else:
        logging.info(f"{s3_key} already exists in s3://{bucket}. Skipping write")
        raise AirflowSkipException


@dag(
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Get current weather conditions from weatherAPI.com (via RapidAPI) and send to Snowflake",
    schedule=timedelta(hours=1),
    start_date=datetime(2023, 7, 13, 17),
    catchup=False,
    tags=["sandbox"],
)
def sandbox_data_pipeline__get_weather():

    get_top_5_cities_task = get_top_5_cities()
    fetch_weather_task = fetch_weather.expand(city=get_top_5_cities_task)

    get_run_hr_task = get_run_hr()
    skip_snowflake_write = str2bool.str2bool(
        Variable.get("sandbox_data_pipeline__skip_snowflake_write", default_var="False")
    )

    write_to_snowflake_task = SQLExecuteQueryOptionalOperator(
        task_id=f"write_conditions_to_snowflake",
        conn_id="snowflake_admin",
        sql="sql/write_weather.sql",
        params={"bucket": bucket, "prefix": prefix},
        trigger_rule="none_failed",
        skip=skip_snowflake_write,
    )

    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish")

    (
        start_task
        >> [get_top_5_cities_task, get_run_hr_task]
        >> fetch_weather_task
        >> write_to_snowflake_task
        >> finish_task
    )


sandbox_data_pipeline__get_weather()
