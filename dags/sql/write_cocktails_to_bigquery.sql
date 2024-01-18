{% set run_hr = task_instance.xcom_pull(task_ids = "get_run_hr") %}

drop table if exists sandbox_data_pipeline.cocktails_stage;

LOAD DATA OVERWRITE sandbox_data_pipeline.cocktails_stage (
  drinks
      ARRAY<
        STRUCT<
          dateModified timestamp,
          idDrink numeric,
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
          `strInstructionsZH-HANS` string,
          `strInstructionsZH-HANT` string,
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
          >
        >
) FROM FILES (
  format='JSON',
  uris=['gs://{{ params["bucket"] }}/{{ params["prefix"] }}/{{ run_hr }}/*.json']
);



create table if not exists sandbox_data_pipeline.cocktails
(
   date_modified		TIMESTAMP,
id_drink		NUMERIC,
alcoholic		STRING,
category		STRING,
creative_commons_confirmed		STRING,
drink		STRING,
drink_alternate		STRING,
drink_thumb		STRING,
glass		STRING,
iba		STRING,
image_attribution		STRING,
image_source		STRING,
ingredient1		STRING,
ingredient10		STRING,
ingredient11		STRING,
ingredient12		STRING,
ingredient13		STRING,
ingredient14		STRING,
ingredient15		STRING,
ingredient2		STRING,
ingredient3		STRING,
ingredient4		STRING,
ingredient5		STRING,
ingredient6		STRING,
ingredient7		STRING,
ingredient8		STRING,
ingredient9		STRING,
instructions		STRING,
instructions_de		STRING,
instructions_es		STRING,
instructions_fr		STRING,
instructions_it		STRING,
instructions_zh_hans		STRING,
instructions_zh_hant		STRING,
measure1		STRING,
measure10		STRING,
measure11		STRING,
measure12		STRING,
measure13		STRING,
measure14		STRING,
measure15		STRING,
measure2		STRING,
measure3		STRING,
measure4		STRING,
measure5		STRING,
measure6		STRING,
measure7		STRING,
measure8		STRING,
measure9		STRING,
tags		STRING,
video		STRING,
run_hr	INTEGER NOT NULL,
    created_at_ts timestamp default current_timestamp NOT NULL
);



delete
from sandbox_data_pipeline.cocktails
where run_hr = {{ run_hr }};

insert into sandbox_data_pipeline.cocktails
(
          date_modified,
          id_drink,
          alcoholic,
          category,
          creative_commons_confirmed,
          drink,
          drink_alternate,
          drink_thumb,
          glass,
          iba,
          image_attribution,
          image_source,
          ingredient1,
          ingredient10,
          ingredient11,
          ingredient12,
          ingredient13,
          ingredient14,
          ingredient15,
          ingredient2,
          ingredient3,
          ingredient4,
          ingredient5,
          ingredient6,
          ingredient7,
          ingredient8,
          ingredient9,
          instructions,
          instructions_de,
          instructions_es,
          instructions_fr,
          instructions_it,
          `instructions_zh_hans`,
          `instructions_zh_hant`,
          measure1,
          measure10,
          measure11,
          measure12,
          measure13,
          measure14,
          measure15,
          measure2,
          measure3,
          measure4,
          measure5,
          measure6,
          measure7,
          measure8,
          measure9,
          tags,
          video,
          run_hr
)
SELECT
         drink.dateModified,
         drink.idDrink,
         drink.strAlcoholic,
         drink.strCategory,
         drink.strCreativeCommonsConfirmed,
         drink.strDrink,
         drink.strDrinkAlternate,
         drink.strDrinkThumb,
         drink.strGlass,
         drink.strIBA,
         drink.strImageAttribution,
         drink.strImageSource,
         drink.strIngredient1,
         drink.strIngredient10,
         drink.strIngredient11,
         drink.strIngredient12,
         drink.strIngredient13,
         drink.strIngredient14,
         drink.strIngredient15,
         drink.strIngredient2,
         drink.strIngredient3,
         drink.strIngredient4,
         drink.strIngredient5,
         drink.strIngredient6,
         drink.strIngredient7,
         drink.strIngredient8,
         drink.strIngredient9,
         drink.strInstructions,
         drink.strInstructionsDE,
         drink.strInstructionsES,
         drink.strInstructionsFR,
         drink.strInstructionsIT,
         drink.`strInstructionsZH-HANS`,
         drink.`strInstructionsZH-HANT`,
         drink.strMeasure1,
         drink.strMeasure10,
         drink.strMeasure11,
         drink.strMeasure12,
         drink.strMeasure13,
         drink.strMeasure14,
         drink.strMeasure15,
         drink.strMeasure2,
         drink.strMeasure3,
         drink.strMeasure4,
         drink.strMeasure5,
         drink.strMeasure6,
         drink.strMeasure7,
         drink.strMeasure8,
         drink.strMeasure9,
         drink.strTags,
         drink.strVideo,
        {{ run_hr }}
from sandbox_data_pipeline.cocktails_stage c
cross join unnest(c.drinks) as drink;
