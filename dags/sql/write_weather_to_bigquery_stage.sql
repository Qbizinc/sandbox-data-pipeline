{% set run_hr = task_instance.xcom_pull(task_ids = "get_run_hr") %}

drop table if exists sandbox_data_pipeline.weather_stage;

LOAD DATA OVERWRITE sandbox_data_pipeline.weather_stage (
  location
    STRUCT<
      name string,
      region string,
      country string,
      lat numeric,
      lon numeric,
      tz_id string,
      localtime_epoch int64,
      localtime string
      >,
  `current`
    STRUCT<
      last_updated_epoch int64,
      last_updated string,
      temp_c numeric,
      temp_f numeric,
      is_day int64,
      condition
        STRUCT<
          `text` string,
          icon string,
          code int64
          >,
      wind_mph numeric,
      wind_kph numeric,
      wind_degree int64,
      wind_dir string,
      pressure_mb numeric,
      pressure_in numeric,
      precip_mm numeric,
      precip_in numeric,
      humidity int64,
      cloud int64,
      feelslike_c numeric,
      feelslike_f numeric,
      vis_km numeric,
      vis_miles numeric,
      uv numeric,
      gust_mph numeric,
      gust_kph numeric
      >
) FROM FILES (
  format='JSON',
  uris=['gs://{{ params["bucket"] }}/{{ params["prefix"] }}/{{ run_hr }}/*.json']
);
