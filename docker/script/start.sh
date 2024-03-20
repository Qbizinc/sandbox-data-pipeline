#!/usr/bin/bash

airflow db init

airflow connections add 'sandbox-data-pipeline-gcp' \
    --conn-json '{"conn_type": "google_cloud_platform", "extra": {"key_path": "/opt/airflow/sandbox-data-pipeline-bigquery.json", "project": "sandbox-data-pipeline", "num_retries": 5}}'

airflow connections add 'qbiz_snowflake_admin' \
    --conn-json '{
    "conn_type": "snowflake",
    "login": "'$SNOWFLAKE_QBIZ_LOGIN'",
    "password": "'$SNOWFLAKE_QBIZ_PASSWORD'",
    "extra": {
        "account": "a4877751795961-qbiz_partner",
        "warehouse": "COMPUTE_WH",
        "region": "us-west-2",
        "role": "ACCOUNTADMIN",
        "insecure_mode": false
    }
}'

airflow users create -u airflow -p airflow -f airflow -l airflow -r Admin -e airflow@gmail.com

airflow webserver --port 8080 &
airflow scheduler