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

sys.path.append("/home/airflow/airflow")
from include.utils.helpers import get_aws_parameter

logging.basicConfig(level=logging.INFO)

'''
Steps:
1. Dummy start task [x]
2. Sensor tasks to check that all Anomalo checks have completed for all configured tables
    - Note: Will work better if Anomalo checks are configured to run at a set time every day rather than "upon arrival"
3. Run Anomalo <> Datahub script
    - TODO: Update Anomalo <> Datahub script to update proper DB section of Datahub
4. Dummy end task [x]
'''

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
    tags=["anomalo"],
)
def anomalo_datahub_integration():
    start_task = EmptyOperator(task_id="start")
    finish_task = EmptyOperator(task_id="finish", trigger_rule="none_failed")

    start_task >> finish_task

anomalo_datahub_integration()
