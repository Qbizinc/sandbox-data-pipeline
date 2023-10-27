create database if not exists SANDBOX_DATA_PIPELINE;

use schema SANDBOX_DATA_PIPELINE.public;

CREATE STORAGE INTEGRATION IF NOT EXISTS sandbox_data_pipeline
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::907770664110:role/sandbox-dpl-snowflake'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sandbox-data-pipeline/snowflake/');

CREATE STAGE IF NOT EXISTS sandbox_data_pipeline
    STORAGE_INTEGRATION = sandbox_data_pipeline
    URL = 's3://{{ params["bucket"] }}/{{ params["prefix"] }}/'
    FILE_FORMAT = (TYPE = JSON FILE_EXTENSION = '.json');

-- list @sandbox_data_pipeline;

create or replace table weather_stage
(
    json_data variant
);

truncate weather_stage;

COPY INTO weather_stage
    FROM @sandbox_data_pipeline/weather/{{ task_instance.xcom_pull(task_ids='get_run_hr') }}/
FILE_FORMAT = (TYPE = JSON);


create or replace table weather
(
    run_hr      int,
    location_name string,
    local_time   timestamp,
    temp_c      int,
    temp_f      int,
    is_day      boolean,
    condition   variant,
    wind_kph    float,
    wind_mph    float,
    gust_kph    float,
    gust_mph    float,
    pressure_mb int,
    pressure_in int,
    humidity    int,
    cloud       int,
    feelslike_c int,
    feelslike_f int,
    vis_km      int,
    vis_miles   int,
    uv          int
);

delete
from weather
where run_hr = {{ task_instance.xcom_pull(task_ids='get_run_hr') }};

insert into weather
select {{ task_instance.xcom_pull(task_ids='get_run_hr') }},
       json_data: location : name,
       date_trunc('hour', json_data:location:localtime::timestamp) as local_time,
       json_data: current :temp_c,
       json_data: current :temp_f,
       json_data: current :is_day::int::boolean,
       json_data: current :condition,
       json_data: current :wind_kph,
       json_data: current :wind_mph,
       json_data: current :gust_kph,
       json_data: current :gust_mph,
       json_data: current :pressure_mb,
       json_data: current :pressure_in,
       json_data: current :humidity,
       json_data: current :cloud,
       json_data: current :feelslike_c,
       json_data: current :feelslike_f,
       json_data: current :vis_km,
       json_data: current :vis_miles,
       json_data: current :uv
FROM weather_stage;
