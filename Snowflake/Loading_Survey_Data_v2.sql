USE ROLE ACCOUNTADMIN;
SELECT
    SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();
CREATE WAREHOUSE IF NOT EXISTS my_warehouse WITH WAREHOUSE_SIZE = 'X-SMALL' -- Options: X-SMALL, SMALL, MEDIUM, LARGE, X-LARGE, etc.
    AUTO_SUSPEND = 60 -- Auto-pause after 60 seconds of inactivity (saves money!)
    AUTO_RESUME = TRUE -- Auto-start when a query runs
    INITIALLY_SUSPENDED = TRUE;
-- Start in suspended state (saves money!)
    USE WAREHOUSE my_warehouse;
CREATE DATABASE IF NOT EXISTS customer_surveys COMMENT = 'Database for customer survey data from S3';
USE DATABASE customer_surveys;
CREATE SCHEMA IF NOT EXISTS customer_surveys.raw_data COMMENT = 'Raw data from S3 - no transformations';
USE SCHEMA customer_surveys.raw_data;
CREATE SCHEMA IF NOT EXISTS customer_surveys.public COMMENT = 'public from S3 - no transformations';
    /*create or replace storage integration s3_int_customer_survey
      type = external_stage
      storage_provider = s3
      enabled = true
      storage_aws_role_arn = 'arn:aws:iam::463734035383:role/snowflake-s3-role'
      storage_allowed_locations = ('s3://customer-survey-bucket-dp2-v1/');
    
    DESC INTEGRATION s3_int_customer_survey;
    /*
    // id like to come back and pull these with SQL but will copy for now 
    
    SELECT
        property,
        property_value
    FROM
        table(result_scan(last_query_id()))
    WHERE
        property IN ('STORAGE_AWS_IAM_USER_ARN', 'STORAGE_AWS_EXTERNAL_ID');
    */
SELECT
    CURRENT_REGION();
CREATE
    OR REPLACE STORAGE INTEGRATION integration_of_surveys TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3' ENABLED = TRUE STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::463734035383:role/mysnowflakerole_forcustomersurveys' STORAGE_ALLOWED_LOCATIONS = ('s3://customer-survey-bucket-dp2-v1/') STORAGE_BLOCKED_LOCATIONS = ('s3://jacks-bucket-dp1/');
DESC INTEGRATION integration_of_surveys;
CREATE ROLE IF NOT EXISTS mysnowflakerole_forcustomersurveys COMMENT = 'Role for customer survey data operations';
GRANT CREATE STAGE ON SCHEMA customer_surveys.raw_data TO ROLE mysnowflakerole_forcustomersurveys;
GRANT USAGE ON INTEGRATION integration_of_surveys TO ROLE mysnowflakerole_forcustomersurveys;
USE SCHEMA customer_surveys.public;
USE SCHEMA customer_surveys.raw_data;
CREATE FILE FORMAT IF NOT EXISTS customer_surveys.raw_data.survey_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '');
CREATE STAGE my_s3_stage STORAGE_INTEGRATION = integration_of_surveys URL = 's3://customer-survey-bucket-dp2-v1/' FILE_FORMAT = customer_surveys.raw_data.survey_csv_format;
DESC INTEGRATION integration_of_surveys;
LIST @my_s3_stage;
LIST @customer_surveys.raw_data.my_s3_stage;
SELECT
    $1,
    $2,
    $3,
    $4,
    $5
FROM
    @customer_surveys.raw_data.my_s3_stage (
        FILE_FORMAT => customer_surveys.raw_data.survey_csv_format
    )
LIMIT
    10;
DROP TABLE customer_surveys.raw_data.PAK_survey_data;
CREATE TABLE customer_surveys.raw_data.PAK_survey_data (
        survey_date VARCHAR,
        app_oppinion VARCHAR,
        band_oppinion VARCHAR,
        experience INT,
        current_gym_frequency VARCHAR,
        ideal_gym_frequency VARCHAR,
        motivations VARCHAR,
        stats VARCHAR,
        competetive VARCHAR(1000),
        leaderboard VARCHAR(1000),
        app VARCHAR(255),
        reason VARCHAR,
        feedback VARCHAR(1000),
        attendance VARCHAR(1000)
    );
COPY INTO customer_surveys.raw_data.PAK_survey_data
FROM
    @my_s3_stage PATTERN = '.*Weightlifting.*.csv';
DESCRIBE TABLE customer_surveys.raw_data.PAK_survey_data;
CREATE SCHEMA IF NOT EXISTS customer_surveys.cleaned;
USE SCHEMA customer_surveys.cleaned;
-- Create cleaned weightlifting survey table
    DROP TABLE IF EXISTS customer_surveys.cleaned.PAK_survey_cleaned;
CREATE
    OR REPLACE TABLE customer_surveys.cleaned.PAK_survey_cleaned CLONE customer_surveys.raw_data.PAK_survey_data;
SELECT
    TOP 15 *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
SELECT
    TOP 15 *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
ORDER BY
    experience ASC;
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS min_days_curr NUMBER;
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS max_days_curr NUMBER;
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS min_days_id NUMBER;
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS max_days_id NUMBER;
    /*- trim() removes leading/trailing spaces
    - replace(..., ' Days', '') removes the suffix
    - split_part(..., '-', n) splits safely
    - try_cast() returns NULL instead of error when the value is empty or invalid
    */
UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned
SET
    min_days_curr = try_cast(
        split_part(
            replace(trim(current_gym_frequency), ' Days', ''),
            '-',
            1
        ) as number
    ),
    max_days_curr = try_cast(
        split_part(
            replace(trim(current_gym_frequency), ' Days', ''),
            '-',
            2
        ) as number
    ),
    min_days_id = try_cast(
        split_part(
            replace(trim(ideal_gym_frequency), ' Days', ''),
            '-',
            1
        ) as number
    ),
    max_days_id = try_cast(
        split_part(
            replace(trim(ideal_gym_frequency), ' Days', ''),
            '-',
            2
        ) as number
    );
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned DROP COLUMN mot_arr;
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS mot_arr ARRAY;
UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned
SET
    mot_arr = CASE
        WHEN motivations IS NULL
        OR trim(motivations) = '' THEN ARRAY_CONSTRUCT()
        ElSE split(motivations, ';')
    END;
SELECT
    TOP 15 *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
ORDER BY
    experience ASC;
//stats
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS stats_arr ARRAY;
UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned
SET
    stats_arr = CASE
        WHEN stats IS NULL
        OR trim(stats) = '' THEN ARRAY_CONSTRUCT()
        ElSE split(stats, ';')
    END;
SELECT
    TOP 15 *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
ORDER BY
    experience ASC;
//checking for abnormal values for increased staging
SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.app_oppinion,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    app_oppinion
ORDER BY
    freq ASC;

SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.app_oppinion,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    app_oppinion
ORDER BY
    freq ASC;

SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.band_oppinion,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    band_oppinion
ORDER BY
    freq ASC;

UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned
SET 
    band_oppinion = 'Maybe'
WHERE
    band_oppinion = 'maybe'

SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.experience,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    experience
ORDER BY
    freq ASC;

SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.competetive,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    competetive
ORDER BY
    freq ASC;

//cleaning and dimensionings
