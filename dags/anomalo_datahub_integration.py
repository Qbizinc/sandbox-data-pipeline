import ast
import json
import logging
import sys
from datetime import date, datetime, timedelta
import time
from typing import Optional

import anomalo
from airflow.decorators import dag, task
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

'''
Steps:
1. Dummy start task [x]
2. Sensor tasks to check that all Anomalo checks have completed for all configured tables [x]
    - Note: Will work better if Anomalo checks are configured to run at a set time every day rather than "upon arrival"
3. Run Anomalo <> Datahub script
    - TODO: Update Anomalo <> Datahub script to update proper DB section of Datahub
4. Dummy end task [x]
'''

@task.sensor(poke_interval=30, timeout=3600, mode="poke")
def anomalo_check_sensor(table_name: str, run_date: Optional[date] = None, **kwargs) -> PokeReturnValue:
    """
    Check if Anomalo check for a given table are completed via Anomalo API
    Args:
        table_name (str): Table name (as seen in Anomalo) to check for a completed Anomalo run
    """

    # Get Anomalo API credentials from AWS parameters
    anomalo_instance_host = get_aws_parameter("sandbox_data_pipeline__anomalo_api_host")
    anomalo_api_secret_token = get_aws_parameter("sandbox_data_pipeline__anomalo_api_token")

    # Use Anomalo API to get table ID from table name
    anomalo_client = anomalo.Client(host=anomalo_instance_host, api_token=anomalo_api_secret_token)
    table_id = anomalo_client.get_table_information(table_name=table_name)["id"]

    # Specify run date so we don't accidentally check yesterday's Anomalo checks
    # NOTE: Default API behavior is to check PREVIOUS day's data; changing default behavior to be TODAY's data
    if run_date is None:
        run_date_str = date.today().strftime("%Y-%m-%d")
    else:
        run_date_str = run_date.strftime("%Y-%m-%d")

    # Get most recent check run within specified interval and check status
    check_run_status = anomalo_client.get_check_intervals(table_id=table_id, start=run_date_str, end=None)[0]["status"]

    if check_run_status == 'pending':
        checks_complete = False
    elif check_run_status == 'skipped':
        print(f"Checks were skipped for table {table_name} on run date {run_date_str}, please investigate.")
        logging.info(f"Checks were skipped for table {table_name} on run date {run_date_str}, please investigate.")
        checks_complete = True
    elif check_run_status == 'complete':
        logging.info(f"Checks completed for table {table_name} on run date {run_date_str}.")
        checks_complete = True
    # Any other statuses that are not in the above
    else:
        print(f"Unknown check status for table {table_name} on run date {run_date_str}, please investigate.")
        logging.info(f"Unknown check status for table {table_name} on run date {run_date_str}, please investigate.")
        checks_complete = True

    return PokeReturnValue(is_done=checks_complete)


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
    schedule='0 1 * * *', # 1am UTC every day
    start_date=datetime(2024, 2, 29),
    catchup=False,
    tags=["sandbox", "anomalo"],
)
def anomalo_datahub_integration():
    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    bigquery_cocktails_anomalo_check_sensor_task = anomalo_check_sensor.override(task_id='bigquery_cocktails_anomalo_check_sensor')(table_name=bigquery_cocktails_prod_table)
    bigquery_weather_anomalo_check_sensor_task = anomalo_check_sensor.override(task_id='bigquery_weather_anomalo_check_sensor')(table_name=bigquery_weather_prod_table)
    snowflake_weather_anomalo_checks_sensor_task = anomalo_check_sensor.override(task_id='snowflake_weather_anomalo_checks_sensor')(table_name=snowflake_weather_prod_table)

    start_task >> [bigquery_cocktails_anomalo_check_sensor_task, bigquery_weather_anomalo_check_sensor_task, snowflake_weather_anomalo_checks_sensor_task] >> finish_task

anomalo_datahub_integration()
