use schema SANDBOX_DATA_PIPELINE.public;

create or replace table cocktails_stage
(
    json_data variant
);

truncate cocktails_stage;

COPY INTO cocktails_stage
    FROM @sandbox_data_pipeline/cocktails/{{ task_instance.xcom_pull(task_ids='get_run_hr') }}/
FILE_FORMAT = (TYPE = JSON);
