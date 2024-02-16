import ast
import json
import logging
import re
import sys
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

sys.path.append("/home/airflow/airflow")
from include.utils.operators import SQLExecuteQueryOptionalOperator, GCSObjectListExistenceSensor
from include.utils.helpers import s3_object_exists, get_aws_parameter

logging.basicConfig(level=logging.INFO)

s3_bucket = "sandbox-data-pipeline"
s3_prefix = "api_data"
gcs_bucket = "qbiz-sandbox-data-pipeline"
gcs_prefix = "s3-transfer/api_data"

s3 = boto3.client("s3")


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
    s3.put_object(Body=response, Bucket=s3_bucket, Key=s3_key)

    # Log successful write to S3
    logging.info(f"{s3_key} written to s3://{s3_bucket}")


@task
def get_run_hr(**kwargs):
    """get run hour from ts. Used in downstream tasks to enforce idempotency"""

    ts = kwargs["ts"]
    run_hr = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y%m%d%H00")
    return run_hr


@task
def get_top_5_cities():
    api_gateway_url = get_aws_parameter(
        "sandbox_data_pipeline__weather_api_gateway_url",
        region="us-west-2"
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
    s3_key = f"{s3_prefix}/weather/{run_hr}/{city_lower}.json"

    fetch_rapid_api_data(url=rapid_api_url, key=rapid_api_key, host=rapid_api_host, s3_bucket=s3_bucket, s3_key=s3_key,
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
    s3_key = f"{s3_prefix}/cocktails/{run_hr}/cocktails.json"

    fetch_rapid_api_data(url=rapid_api_url, key=rapid_api_key, host=rapid_api_host,
                         s3_bucket=s3_bucket, s3_key=s3_key,
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
    description="Fetch data from RapidAPI and write to Snowflake/Bigquery using S3 and GCP",
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 4, 11),
    catchup=False,
    tags=["sandbox"],
)
def sandbox_data_pipeline():
    get_top_5_cities_task = get_top_5_cities()
    fetch_weather_task = fetch_weather.expand(city=get_top_5_cities_task)
    fetch_cocktails_task = fetch_cocktails()

    get_run_hr_task = get_run_hr()

    skip_snowflake_write = str2bool.str2bool(
        Variable.get("sandbox_data_pipeline__skip_snowflake_write", default_var="False")
    )

    wait_for_weather_files_in_gcs_task = GCSObjectListExistenceSensor(
        google_cloud_conn_id="sandbox-data-pipeline-gcp",
        task_id="wait_for_weather_files_in_gcs",
        bucket=gcs_bucket,
        prefix=gcs_prefix + "/weather/{{ ti.xcom_pull(task_ids='get_run_hr') }}",
        object_list="{{ ti.xcom_pull(task_ids='get_top_5_cities') }}",
        timeout=600,
        trigger_rule="none_failed",
    )

    wait_for_cocktail_files_in_gcs_task = GCSObjectListExistenceSensor(
        google_cloud_conn_id="sandbox-data-pipeline-gcp",
        task_id="wait_for_cocktail_files_in_gcs",
        bucket=gcs_bucket,
        prefix=gcs_prefix + "/cocktails/{{ ti.xcom_pull(task_ids='get_run_hr') }}",
        object_list="['cocktails']",
        timeout=600,
    )

    write_weather_to_bigquery_stage_task = BigQueryInsertJobOperator(
        task_id=f"write_weather_to_bigquery_stage",
        gcp_conn_id="sandbox-data-pipeline-gcp",
        params={"bucket": gcs_bucket,
                "prefix": f"{gcs_prefix}/weather"},
        configuration={
            "query": {
                "query": "sql/write_weather_to_bigquery_stage.sql",
                "useLegacySql": False,
            }
        },
    )

    write_weather_to_bigquery_task = BigQueryInsertJobOperator(
        task_id=f"write_weather_to_bigquery",
        gcp_conn_id="sandbox-data-pipeline-gcp",
        params={"bucket": gcs_bucket,
                "prefix": f"{gcs_prefix}/weather"},
        configuration={
            "query": {
                "query": "sql/write_weather_bigquery.sql",
                "useLegacySql": False,
            }
        },
    )

    write_cocktails_to_bigquery_stage_task = BigQueryInsertJobOperator(
        task_id=f"write_cocktails_to_bigquery_stage",
        gcp_conn_id="sandbox-data-pipeline-gcp",
        params={"bucket": gcs_bucket,
                "prefix": f"{gcs_prefix}/cocktails"},
        configuration={
            "query": {
                "query": "sql/write_cocktails_to_bigquery_stage.sql",
                "useLegacySql": False,
            }
        }
    )

    write_cocktails_to_bigquery_task = BigQueryInsertJobOperator(
        task_id=f"write_cocktails_to_bigquery",
        gcp_conn_id="sandbox-data-pipeline-gcp",
        params={"bucket": gcs_bucket,
                "prefix": f"{gcs_prefix}/cocktails"},
        configuration={
            "query": {
                "query": "sql/write_cocktails_bigquery.sql",
                "useLegacySql": False,
            }
        }
    )

    create_snowflake_storage_integration_task = SQLExecuteQueryOptionalOperator(
        task_id=f"create_snowflake_storage_integration",
        conn_id="qbiz_snowflake_admin",
        sql="sql/create_snowflake_storage_integration.sql",
        params={"bucket": s3_bucket, "prefix": s3_prefix},
        trigger_rule="none_failed",
        skip=skip_snowflake_write,
    )

    write_weather_to_snowflake_task = SQLExecuteQueryOptionalOperator(
        task_id=f"write_conditions_to_snowflake",
        conn_id="qbiz_snowflake_admin",
        sql="sql/write_weather_to_snowflake.sql",
        params={"bucket": s3_bucket, "prefix": s3_prefix},
        skip=skip_snowflake_write,
    )

    write_cocktails_to_snowflake_task = SQLExecuteQueryOptionalOperator(
        task_id=f"write_cocktails_to_snowflake",
        conn_id="qbiz_snowflake_admin",
        sql="sql/write_cocktails_to_snowflake.sql",
        params={"bucket": s3_bucket, "prefix": s3_prefix},
        skip=skip_snowflake_write,
    )

    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    start_task >> get_run_hr_task

    get_run_hr_task >> get_top_5_cities_task >> fetch_weather_task
    get_run_hr_task >> fetch_cocktails_task

    fetch_weather_task >> wait_for_weather_files_in_gcs_task >> write_weather_to_bigquery_task
    fetch_cocktails_task >> wait_for_cocktail_files_in_gcs_task >> write_cocktails_to_bigquery_task

    [fetch_weather_task, fetch_cocktails_task] >> create_snowflake_storage_integration_task
    create_snowflake_storage_integration_task >> [write_weather_to_snowflake_task, write_cocktails_to_snowflake_task]

    [write_weather_to_bigquery_task,
     write_cocktails_to_bigquery_task,
     write_weather_to_snowflake_task,
     write_cocktails_to_snowflake_task] >> finish_task


sandbox_data_pipeline()
