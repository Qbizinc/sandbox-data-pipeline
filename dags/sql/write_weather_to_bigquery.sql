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



create table if not exists sandbox_data_pipeline.weather
(
    location
        STRUCT <
                name string,
                region string,
                country string,
                lat numeric,
                lon numeric,
                tz_id string,
                localtime_epoch int64,
                localtime timestamp>,
    `current`
        STRUCT <
                last_updated_epoch int64,
                last_updated timestamp,
                temp_c numeric,
                temp_f numeric,
                is_day int64,
                condition
                    STRUCT <
                            `text` string,
                            icon string,
                            code int64>,
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
                gust_kph numeric>,
    run_hr        int64 NOT NULL,
    created_at_ts timestamp default current_timestamp NOT NULL

);



delete
from sandbox_data_pipeline.weather
where run_hr = {{ run_hr }};


insert into sandbox_data_pipeline.weather
    (location, `current`, run_hr)
SELECT STRUCT (
           location.`name`,
           location.region,
           location.country,
           location.lat,
           location.lon,
           location.tz_id,
           location.localtime_epoch,
           timestamp(location.localtime || ':00') as localtime
           )      as location,
       STRUCT (
           `current`.last_updated_epoch ,
           timestamp(`current`.last_updated || ':00') as last_updated,
           `current`.temp_c,
           `current`.temp_f ,
           `current`.is_day ,
           STRUCT (
               `current`.condition.text,
               `current`.condition.icon,
               `current`.condition.code
               ) as condition,
           `current`.wind_mph,
           `current`.wind_kph,
           `current`.wind_degree,
           `current`.wind_dir,
           `current`.pressure_mb,
           `current`.pressure_in,
           `current`.precip_mm,
           `current`.precip_in,
           `current`.humidity,
           `current`.cloud,
           `current`.feelslike_c,
           `current`.feelslike_f,
           `current`.vis_km,
           `current`.vis_miles,
           `current`.uv,
           `current`.gust_mph,
           `current`.gust_kph
           )      as `current`,
    {{ run_hr }} as run_hr
from sandbox_data_pipeline.weather_stage;
