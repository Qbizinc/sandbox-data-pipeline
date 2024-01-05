import ast
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import boto3
import requests
import str2bool
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from botocore.errorfactory import ClientError

from utils.operators import SQLExecuteQueryOptionalOperator, GCSObjectListExistenceSensor

logging.basicConfig(level=logging.INFO)

bucket = "sandbox-data-pipeline"
gcs_bucket = "qbiz-sandbox-data-pipeline"
prefix = "snowflake"
gcs_prefix = "s3-transfer/snowflake/weather"
region = "us-west-2"

s3 = boto3.client("s3")


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


def get_aws_parameter(parameter_name: str, ssm: Optional[boto3.client] = None) -> str:
    """
    Get a parameter from AWS Systems Manager Parameter Store.

    Args:
        parameter_name: The name of the parameter to retrieve.
        ssm: An optional pre-initialized boto3 SSM client.

    Returns:
        The value of the parameter.
    """
    if ssm is None:
        ssm = boto3.client("ssm", region_name=region)

    response = ssm.get_parameter(Name=parameter_name)
    return response["Parameter"]["Value"]


def fetch_rapid_api_data(url: str, key: str, host: str, s3_bucket: str, s3_key: str, querystring: Optional[dict] = None,
                         transform_callback: Optional[callable] = None
                         ) -> None:
    """
    Make a call to a RapidAPI endpoint.
    Args:
        url: The URL of the endpoint.
        key: The API key.
        host: The API host.
        s3_bucket: The S3 bucket to write the response to.
        s3_key: The S3 key to write the response to.
        querystring: Optional query parameters.
        transform_callback: Optional callback function to transform the response before writing to S3.
    """
    # Check if the data already exists in S3
    if s3_object_exists(s3_bucket, s3_key):
        raise AirflowSkipException

    # Set headers for RapidAPI
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": host,
    }

    # Fetch data from the API
    response = requests.get(url, headers=headers, params=querystring)
    response = str(response.json())
    if transform_callback:
        response = transform_callback(response)

    # Store data in S3
    s3.put_object(Body=response, Bucket=bucket, Key=s3_key)

    # Log successful write to S3
    logging.info(f"{s3_key} written to s3://{bucket}")


@task
def get_run_hr(**kwargs):
    """get run hour from ts. Used in downstream tasks to enforce idempotency"""

    ts = kwargs["ts"]
    run_hr = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y%m%d%H00")
    return run_hr


@task
def get_top_5_cities():
    api_gateway_url = get_aws_parameter(
        "sandbox_data_pipeline__weather_api_gateway_url"
    )
    cities = requests.get(api_gateway_url).json()
    return cities


@task
def fetch_weather(city: str, **kwargs):
    """
    Fetch weather data from weatherapi.com and write to S3.

    Args:
        city (str): City name to fetch weather data for.
    """

    # Get RapidAPI credentials and URL from AWS parameters
    rapid_api_url = get_aws_parameter("sandbox_data_pipeline__weather_rapid_api_url")
    rapid_api_key = get_aws_parameter("sandbox_data_pipeline__weather_rapid_api_key")
    rapid_api_host = get_aws_parameter("sandbox_data_pipeline__weather_rapid_api_host")

    # Retrieve run hour from task instance
    ti = kwargs["ti"]
    run_hr = ti.xcom_pull(task_ids="get_run_hr")

    # Define query parameters
    city_lower = city.lower().replace(" ", "_")
    querystring = {"q": city}

    # Define S3 key
    s3_key = f"{prefix}/weather/{run_hr}/{city_lower}.json"

    fetch_rapid_api_data(url=rapid_api_url, key=rapid_api_key, host=rapid_api_host, s3_bucket=bucket, s3_key=s3_key,
                         querystring=querystring)


def clean_cocktail_json(cocktail_json: str) -> str:
    cocktail_json = re.sub(r"[\n\r]", "", cocktail_json)
    cocktail_json = ast.literal_eval(cocktail_json)
    return json.dumps(cocktail_json)


@task
def fetch_cocktails(**kwargs):
    """
    Fetch latest cocktails
    """

    # Get RapidAPI credentials and URL from AWS parameters
    rapid_api_url = get_aws_parameter("sandbox_data_pipeline__cocktails_rapid_api_url")
    rapid_api_key = get_aws_parameter("sandbox_data_pipeline__cocktails_rapid_api_key")
    rapid_api_host = get_aws_parameter("sandbox_data_pipeline__cocktails_rapid_api_host")

    # Retrieve run hour from task instance
    ti = kwargs["ti"]
    run_hr = ti.xcom_pull(task_ids="get_run_hr")

    # Define S3 key
    s3_key = f"{prefix}/cocktails/{run_hr}/cocktails.json"

    fetch_rapid_api_data(url=rapid_api_url, key=rapid_api_key, host=rapid_api_host, s3_bucket=bucket, s3_key=s3_key,
                         transform_callback=clean_cocktail_json)


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
    fetch_cocktails_task = fetch_cocktails()

    get_run_hr_task = get_run_hr()

    skip_snowflake_write = str2bool.str2bool(
        Variable.get("sandbox_data_pipeline__skip_snowflake_write", default_var="False")
    )

    wait_for_files_in_gcs_task = GCSObjectListExistenceSensor(
        google_cloud_conn_id="sandbox-data-pipeline-gcp",
        task_id="wait_for_files_in_gcs",
        bucket=gcs_bucket,
        prefix=gcs_prefix + "/{{ ti.xcom_pull(task_ids='get_run_hr') }}",
        object_list="{{ ti.xcom_pull(task_ids='get_top_5_cities') }}",
        timeout=600,
        trigger_rule="none_failed",
    )

    write_to_bigquery_task = BigQueryInsertJobOperator(
        task_id=f"write_conditions_to_bigquery",
        gcp_conn_id="sandbox-data-pipeline-gcp",
        params={"bucket": gcs_bucket,
                "prefix": gcs_prefix},
        configuration={
            "query": {
                "query": "{% include 'sql/write_weather_bigquery.sql' %}",
                "useLegacySql": False,
            }
        }
    )

    write_weather_to_snowflake_task = SQLExecuteQueryOptionalOperator(
        task_id=f"write_conditions_to_snowflake",
        conn_id="qbiz_snowflake_admin",
        sql="sql/write_weather.sql",
        params={"bucket": bucket, "prefix": prefix},
        trigger_rule="none_failed",
        skip=skip_snowflake_write,
    )

    write_cocktails_to_snowflake_task = SQLExecuteQueryOptionalOperator(
        task_id=f"write_cocktails_to_snowflake",
        conn_id="qbiz_snowflake_admin",
        sql="sql/write_cocktails.sql",
        params={"bucket": bucket, "prefix": prefix},
        trigger_rule="none_failed",
        skip=skip_snowflake_write,
    )

    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    (
            start_task
            >> get_run_hr_task
            >> [get_top_5_cities_task, fetch_cocktails_task]
    )

    get_top_5_cities_task >> fetch_weather_task

    (
            [fetch_weather_task, fetch_cocktails_task]
            >> wait_for_files_in_gcs_task
            >> [write_weather_to_snowflake_task, write_cocktails_to_snowflake_task, write_to_bigquery_task]
            >> finish_task
    )


sandbox_data_pipeline__get_weather()
