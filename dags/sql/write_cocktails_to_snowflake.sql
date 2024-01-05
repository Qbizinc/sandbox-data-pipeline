use schema SANDBOX_DATA_PIPELINE.public;

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
    date_modified timestamp,
    drink_id int,
    alcoholic string,
    category string,
    creative_commons_confirmed string,
    drink string,
    drink_alternate string,
    drink_thumb string,
    glass string,
    iba string,
    image_attribution string,
    image_source string,
    ingredient1 string,
    ingredient10 string,
    ingredient11 string,
    ingredient12 string,
    ingredient13 string,
    ingredient14 string,
    ingredient15 string,
    ingredient2 string,
    ingredient3 string,
    ingredient4 string,
    ingredient5 string,
    ingredient6 string,
    ingredient7 string,
    ingredient8 string,
    ingredient9 string,
    instructions string,
    instructions_de string,
    instructions_es string,
    instructions_fr string,
    instructions_it string,
    instructions_zh_hans string,
    instructions_zh_hant string,
    measure1 string,
    measure10 string,
    measure11 string,
    measure12 string,
    measure13 string,
    measure14 string,
    measure15 string,
    measure2 string,
    measure3 string,
    measure4 string,
    measure5 string,
    measure6 string,
    measure7 string,
    measure8 string,
    measure9 string,
    tags string,
    video string
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
