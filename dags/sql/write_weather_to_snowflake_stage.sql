use schema SANDBOX_DATA_PIPELINE.public;

create or replace table weather_stage
(
    json_data variant
);

truncate weather_stage;

COPY INTO weather_stage
    FROM @sandbox_data_pipeline/{{ params['prefix']}}/weather/{{ task_instance.xcom_pull(task_ids='get_run_hr') }}/
FILE_FORMAT = (TYPE = JSON);
