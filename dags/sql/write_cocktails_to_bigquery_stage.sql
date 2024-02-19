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
