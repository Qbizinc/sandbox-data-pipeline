# sandbox_data_pipeline

## Docker container deployement

To build the container

```sh
docker build -t qbiz/airflow:v2.8.2 --file docker/Dockerfile-airflow-282 .
```

To run the container

```sh
docker run -it -p 8080:8080 qbiz/airflow:v2.8.2 bash -c "/start.sh"
```

## Overview

Note: for instructions on setting up Airflow on EC2, see [this wiki page](https://qbiz-wiki.com/en/technology/airflow/airflow_on_ec2).

The purpose of this project is to demonstrate a realistic end-to-end data pipeline
spanning multiple cloud services, orchestrated by Airflow running on an EC2 instance.
Geography-specific weather condition data is pulled from an API, 
stored in an AWS S3 bucket, and then loaded into Google BigQuery and (optionally) Snowflake.

In addition, the Airflow DAG demonstrates
[custom operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html),
the [taskflow paradigm](https://docs.astronomer.io/learn/airflow-decorators),
[jina templating](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html),
[xcoms](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html),
and [dynamic task mapping](https://docs.astronomer.io/learn/dynamic-tasks).



Pipeline Overview:
![pipeline overview](/diagram_images/sandbox_data_pipeline.png)

Airflow DAG:
![airflow DAG](diagram_images/airflow_dag.png)


---

Services utilized include:

- AWS
  - EC2
  - Parameter Store
  - S3
  - SQS
  - API Gateway


- Google Cloud Platform (GCP)
  - BigQuery
  - Cloud Storage
  - GCS Transfer Service


- Snowflake


- RapidAIP:
- - weatherapi.com

---

## Steps

1. **start**: empty task
2. **get_top_5_cities**: get top 5 cities from API
   1. Get API endpoint from AWS Parameter Store
   2. Make HTTP request to API endpoint
   3. Return list of 5 cities and pass to xcom (via return)
3.  **get_run_hr** (parallel with 2): get run hour from DAG run timestamp and pass to xcom
4. **fetch_weather**: fetch weather data for each city
   1. Get city list and run_hr from xcom
   2. Using dynamic task mapping, **for each city**, run in parallel:
      1. Check S3 to see if data for city already exists for run_hr
      2. If so, skip (raise AirflowSkipException)
      3. If not, write JSON response to S3
      4. [**S3 BUCKET TRIGGERED EVENT**]: write JSON filename to SQS queue
      5. [**GCP TRANSFER SERVICE**]: pull JSON file from S3 and write to GCS bucket
5. **wait_for_files_in_gcs**:
   1. get file list from xcom
   2. wait for all files to appear in GCS bucket (custom Airflow GCS sensor)
6. **write_to_snowflake**:
   1. skip if variable `sandbox_data_pipeline__skip_snowflake_write` is set to `True` (custom operator)
   2. Execute `sql/write_weather_to_snowflake.sql` to load data from GCS into Snowflake idempotently
7. **write_to_bigquery**:
   1. Execute `sql/write_weather_to_bigquery.sql` to load data from GCS into BigQuery idempotently
8. **finish**: empty task

---

## Code summary ##
This Python code defines an Apache Airflow DAG (Directed Acyclic Graph) for a data pipeline that retrieves current weather conditions from weatherAPI.com (via RapidAPI) and sends this data to both Snowflake and Google BigQuery. Below is a summary of the key components and functionality of the code:

1. **Imports**: The code imports various Python libraries and modules, including logging, AWS SDK (Boto3), requests for making HTTP requests, and Airflow-related modules for task scheduling and management.

2. **Configuration**: It sets up several configuration variables such as S3 bucket names, AWS region, and various API endpoints and keys required for fetching weather data.

3. **Utility Functions**:
   - `s3_object_exists`: Checks if an object exists in an S3 bucket.
   - `get_aws_parameter`: Retrieves a parameter from AWS Systems Manager Parameter Store.

4. **Airflow Tasks**:
   - `get_run_hr`: A task that parses the timestamp of the DAG run and formats it as 'YYYYMMDDHH00'. This is used for enforcing idempotency in downstream tasks.
   - `get_top_5_cities`: A task that retrieves the top 5 cities from an API endpoint.
   - `fetch_weather`: A task that fetches weather data for a city from weatherAPI.com via RapidAPI, and then writes this data to an S3 bucket. It checks if the data for a city already exists in S3 and skips the task if it does.
   
5. **Airflow DAG Definition**:
   - `sandbox_data_pipeline__get_weather`: Defines the Airflow DAG.
   - Default parameters for the DAG are specified, including email notification settings and retry behavior.
   - The DAG is scheduled to run every hour (`timedelta(hours=1)`) starting from a specific date (`datetime(2023, 7, 13, 17)`).
   - Catchup is set to `False`, meaning it won't execute missed DAG runs.
   - Tags are added to the DAG for categorization.

6. **Task Dependencies**:
   - The DAG defines task dependencies using `>>` and `[...]` notation, indicating the execution order of tasks.
   - It starts with an empty "start" task and ends with an empty "finish" task.

7. **Additional Notes**:
   - The DAG includes conditional logic (`skip_snowflake_write`) to optionally skip writing data to Snowflake based on a variable's value.
   - It also includes a Google Cloud Storage (GCS) sensor (`wait_for_files_in_gcs_task`) to wait for files to appear in a GCS bucket.
   - Data is written to both Snowflake and BigQuery using specific operators (`write_to_snowflake_task` and `write_to_bigquery_task`) with associated SQL queries.

Overall, this code defines an Airflow DAG that orchestrates the collection of weather data for the top 5 cities, checks for idempotency, and then loads this data into Snowflake and BigQuery for further analysis. The DAG runs on a regular schedule, ensuring up-to-date weather data is available in the data warehouse systems.
