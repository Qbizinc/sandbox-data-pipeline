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

create or replace table cocktails_stage
(
    json_data variant
);

truncate cocktails_stage;

COPY INTO cocktails_stage
    FROM @sandbox_data_pipeline/cocktails/{{ task_instance.xcom_pull(task_ids='get_run_hr') }}/
FILE_FORMAT = (TYPE = JSON);


create table if not exists cocktails
(
    run_hr      int,
    dateModified timestamp,
    idDrink int,
    strAlcoholic string,
    strCategory string,
    strCreativeCommonsConfirmed string,
    strDrink string,
    strDrinkAlternate string,
    strDrinkThumb string,
    strGlass string,
    strIBA string,
    strImageAttribution string,
    strImageSource string,
    strIngredient1 string,
    strIngredient10 string,
    strIngredient11 string,
    strIngredient12 string,
    strIngredient13 string,
    strIngredient14 string,
    strIngredient15 string,
    strIngredient2 string,
    strIngredient3 string,
    strIngredient4 string,
    strIngredient5 string,
    strIngredient6 string,
    strIngredient7 string,
    strIngredient8 string,
    strIngredient9 string,
    strInstructions string,
    strInstructionsDE string,
    strInstructionsES string,
    strInstructionsFR string,
    strInstructionsIT string,
    strInstructionsZH_HANS string,
    strInstructionsZH_HANT string,
    strMeasure1 string,
    strMeasure10 string,
    strMeasure11 string,
    strMeasure12 string,
    strMeasure13 string,
    strMeasure14 string,
    strMeasure15 string,
    strMeasure2 string,
    strMeasure3 string,
    strMeasure4 string,
    strMeasure5 string,
    strMeasure6 string,
    strMeasure7 string,
    strMeasure8 string,
    strMeasure9 string,
    strTags string,
    strVideo string
);

delete
from cocktails
where run_hr = {{ task_instance.xcom_pull(task_ids='get_run_hr') }};

insert into cocktails
select {{ task_instance.xcom_pull(task_ids='get_run_hr') }},
    value: dateModified,
    value: idDrink,
    value: strAlcoholic,
    value: strCategory,
    value: strCreativeCommonsConfirmed,
    value: strDrink,
    value: strDrinkAlternate,
    value: strDrinkThumb,
    value: strGlass,
    value: strIBA,
    value: strImageAttribution,
    value: strImageSource,
    value: strIngredient1,
    value: strIngredient10,
    value: strIngredient11,
    value: strIngredient12,
    value: strIngredient13,
    value: strIngredient14,
    value: strIngredient15,
    value: strIngredient2,
    value: strIngredient3,
    value: strIngredient4,
    value: strIngredient5,
    value: strIngredient6,
    value: strIngredient7,
    value: strIngredient8,
    value: strIngredient9,
    value: strInstructions,
    value: strInstructionsDE,
    value: strInstructionsES,
    value: strInstructionsFR,
    value: strInstructionsIT,
    value: `strInstructionsZH-HANS`,
    value: `strInstructionsZH-HANT`,
    value: strMeasure1,
    value: strMeasure10,
    value: strMeasure11,
    value: strMeasure12,
    value: strMeasure13,
    value: strMeasure14,
    value: strMeasure15,
    value: strMeasure2,
    value: strMeasure3,
    value: strMeasure4,
    value: strMeasure5,
    value: strMeasure6,
    value: strMeasure7,
    value: strMeasure8,
    value: strMeasure9,
    value: strTags,
    value: strVideo
    from cocktails_stage,
    LATERAL FLATTEN( INPUT => json_data: drinks );
