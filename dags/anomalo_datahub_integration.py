import logging
import sys
from datetime import datetime, timedelta
from typing import Optional

import anomalo
from airflow.decorators import dag, task
from airflow.macros import ds_add
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue

sys.path.append("/home/airflow/airflow")
from include.utils.helpers import get_aws_parameter

logging.basicConfig(level=logging.INFO)

s3_bucket = "sandbox-data-pipeline"
s3_prefix = "api_data"

bigquery_cocktails_prod_table = 'qbiz-bigquery-sandbox-pipeline.sandbox_data_pipeline.cocktails'
bigquery_weather_prod_table = 'qbiz-bigquery-sandbox-pipeline.sandbox_data_pipeline.weather'
snowflake_weather_prod_table = 'qbiz-snowflake-sandbox-pipeline.public.weather'
#TODO: Add Snowflake cocktails table once Anomalo <> Snowflake issues are figured out

@task
def get_anomalo_creds():
    # Get Anomalo API credentials from AWS parameter store, return as XCOMs
    anomalo_instance_host = get_aws_parameter("sandbox_data_pipeline__anomalo_api_host")
    anomalo_api_secret_token = get_aws_parameter("sandbox_data_pipeline__anomalo_api_token")

    return anomalo_instance_host, anomalo_api_secret_token


@task.sensor(poke_interval=30, timeout=3600, mode="poke")
def anomalo_check_sensor(table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> PokeReturnValue:
    """
    Check if Anomalo checks for a given table are completed via Anomalo API
    Args:
        table_name: Table name (as seen in Anomalo) to check for a completed Anomalo run
        start_date: Optional start date of Anomalo check run interval to be inserted into Datahub (in "%Y-%m-%d" format)
        end_date: Optional end date of Anomalo check run interval to be inserted into Datahub (in "%Y-%m-%d" format)

    Returns:
        PokeReturnValue: Boolean dictating whether the sensor keeps checking Anomalo check results or ends and is marked as successful
    """

    # Grab credentials from above task and create Anomalo client
    ti = kwargs["ti"]
    anomalo_creds = ti.xcom_pull(task_ids="get_anomalo_creds")
    anomalo_instance_host = anomalo_creds[0]
    anomalo_api_secret_token = anomalo_creds[1]
    anomalo_client = anomalo.Client(host=anomalo_instance_host, api_token=anomalo_api_secret_token)

    # Use Anomalo API client to get table ID from table name
    table_id = anomalo_client.get_table_information(table_name=table_name)["id"]

    # Default Behavior (if no dates supplied): Fill in start date (yesterday's date) and end date (today's date)
    if start_date is None and end_date is None:
        start_date = kwargs["ds"]
        end_date = ds_add(kwargs["ds"], 1)

    # Get most recent check run within specified interval and check status
    check_run_status = anomalo_client.get_check_intervals(table_id=table_id, start=start_date, end=end_date)[0]["status"]

    if check_run_status == 'pending':
        print(f"Checks not yet complete for table {table_name} for date {start_date}, will check again soon.")
        checks_complete = False
    elif check_run_status == 'skipped':
        print(f"Checks were skipped for table {table_name} for date {start_date}, please investigate.")
        logging.info(f"Checks were skipped for table {table_name} for date {start_date}, please investigate.")
        checks_complete = True
    elif check_run_status == 'complete':
        logging.info(f"Checks completed for table {table_name} for date {start_date}.")
        checks_complete = True
    # Any other statuses that are not in the above (there may be other statuses requiring custom handling)
    else:
        print(f"Unknown check status for table {table_name} for date {start_date}, please investigate.")
        logging.info(f"Unknown check status for table {table_name} for date {start_date}, please investigate.")
        checks_complete = True

    return PokeReturnValue(is_done=checks_complete)


@task
def write_anomalo_data_to_datahub(start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs):
    '''
    Simple task wrapper function to run below function imported from anomalo_datahub.py
    Args:
        start_date: Optional start date of Anomalo check interval to be inserted into Datahub (in "%Y-%m-%d" format)
        end_date: Optional end date of Anomalo check interval to be inserted into Datahub (in "%Y-%m-%d" format)
    '''
    
    # Moving "expensive" import inside task to reduce DAG parsing time
    from include.utils.anomalo_datahub import anomalo_to_datahub

    # Grab credentials from above task and create Anomalo client
    ti = kwargs["ti"]
    anomalo_creds = ti.xcom_pull(task_ids="get_anomalo_creds")
    anomalo_instance_host = anomalo_creds[0]
    anomalo_api_secret_token = anomalo_creds[1]
    anomalo_client = anomalo.Client(host=anomalo_instance_host, api_token=anomalo_api_secret_token)

    # Default Behavior (if no dates supplied): Fill in start date (yesterday's date) and end date (today's date)
    if start_date is None and end_date is None:
        start_date = kwargs["ds"]
        end_date = ds_add(kwargs["ds"], 1)
    anomalo_to_datahub(api_client=anomalo_client, start_date=start_date, end_date=end_date)

@dag(
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch check run result data from Anomalo and insert into Datahub",
    schedule='00 18 * * *', # 6:00pm UTC every day
    start_date=datetime(2024, 3, 4),
    catchup=False,
    tags=["sandbox", "anomalo"],
)
def anomalo_datahub_integration():
    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    get_anomalo_creds_task = get_anomalo_creds()

    bigquery_cocktails_anomalo_check_sensor_task = anomalo_check_sensor.override(task_id='bigquery_cocktails_anomalo_check_sensor')(table_name=bigquery_cocktails_prod_table)
    bigquery_weather_anomalo_check_sensor_task = anomalo_check_sensor.override(task_id='bigquery_weather_anomalo_check_sensor')(table_name=bigquery_cocktails_prod_table)
    snowflake_weather_anomalo_checks_sensor_task = anomalo_check_sensor.override(task_id='snowflake_weather_anomalo_checks_sensor')(table_name=bigquery_cocktails_prod_table)
    
    write_anomalo_data_to_datahub_task = write_anomalo_data_to_datahub()

    start_task >> get_anomalo_creds_task >> [bigquery_cocktails_anomalo_check_sensor_task, 
                                             bigquery_weather_anomalo_check_sensor_task, 
                                             snowflake_weather_anomalo_checks_sensor_task] >> write_anomalo_data_to_datahub_task
    write_anomalo_data_to_datahub_task >> finish_task

anomalo_datahub_integration()
