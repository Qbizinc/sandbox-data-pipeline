{% set s3_location = 's3://' + params.bucket + '/' + params.prefix + '/'%}

create database if not exists SANDBOX_DATA_PIPELINE;

use schema SANDBOX_DATA_PIPELINE.public;

-- drop storage integration sandbox_data_pipeline;
-- ONLY do this when you absolutely need to redefine the storage integration
-- (e.g., you changed S3 locations)
-- see note below

CREATE STORAGE INTEGRATION IF NOT EXISTS sandbox_data_pipeline
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::907770664110:role/sandbox-dpl-snowflake'
  STORAGE_ALLOWED_LOCATIONS = ('{{ s3_location }}');

-- desc integration sandbox_data_pipeline;
-- you will need the STORAGE_AWS_EXTERNAL_ID for the AWS IAM trust policy
-- *** this changes *** every time the storage integration is created/recreated
-- to update, go to IAM > Roles > sandbox-data-pipeline-gcp > Trust relationships

CREATE OR REPLACE STAGE sandbox_data_pipeline
    STORAGE_INTEGRATION = sandbox_data_pipeline
    URL = '{{ s3_location }}'
    FILE_FORMAT = (TYPE = JSON FILE_EXTENSION = '.json');
