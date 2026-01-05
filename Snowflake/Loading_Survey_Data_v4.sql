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
-- IMPORTANT: Get values needed for AWS IAM role trust policy
-- Run this query separately to get the required values:
-- DESC INTEGRATION integration_of_surveys;
-- Then manually copy the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values
-- Update your AWS IAM role trust policy at:
-- AWS Console → IAM → Roles → mysnowflakerole_forcustomersurveys → Trust relationships
-- Use this trust policy template (replace with your values):
-- {
--   "Version": "2012-10-17",
--   "Statement": [{
--     "Effect": "Allow",
--     "Principal": {"AWS": "PASTE_STORAGE_AWS_IAM_USER_ARN_HERE"},
--     "Action": "sts:AssumeRole",
--     "Condition": {"StringEquals": {"sts:ExternalId": "PASTE_STORAGE_AWS_EXTERNAL_ID_HERE"}}
--   }]
-- }
DESC INTEGRATION integration_of_surveys;
CREATE ROLE IF NOT EXISTS mysnowflakerole_forcustomersurveys COMMENT = 'Role for customer survey data operations';
GRANT CREATE STAGE ON SCHEMA customer_surveys.raw_data TO ROLE mysnowflakerole_forcustomersurveys;
GRANT USAGE ON INTEGRATION integration_of_surveys TO ROLE mysnowflakerole_forcustomersurveys;
CREATE FILE FORMAT IF NOT EXISTS customer_surveys.raw_data.survey_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' TRIM_SPACE = TRUE NULL_IF = ('NULL', 'null', '');
CREATE OR REPLACE STAGE my_s3_stage STORAGE_INTEGRATION = integration_of_surveys URL = 's3://customer-survey-bucket-dp2-v1/' FILE_FORMAT = customer_surveys.raw_data.survey_csv_format;
LIST @customer_surveys.raw_data.my_s3_stage;
LIST @my_s3_stage;
LIST @customer_surveys.raw_data.my_s3_stage;
-- Preview Weightlifting survey data only (exclude Smartbox files)
SELECT
    $1,
    $2,
    $3,
    $4,
    $5
FROM
    @customer_surveys.raw_data.my_s3_stage (
        FILE_FORMAT => customer_surveys.raw_data.survey_csv_format,
        PATTERN => '.*[Ww]eightlifting.*\\.csv'  -- Only read Weightlifting files, exclude Smartbox
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
    @my_s3_stage 
    -- Only load Weightlifting survey files - explicitly exclude Smartbox
    -- Pattern requires "Weightlifting" (case-insensitive) and excludes "Smartbox"
    PATTERN = '.*[Ww]eightlifting.*\\.csv';
    -- This pattern will match files with "Weightlifting" or "weightlifting" in the name
    -- It will NOT match "Smartbox Survey.csv" since that doesn't contain "Weightlifting"
    -- 
    -- Alternative: If you know the exact filename(s), replace PATTERN with:
    -- FILES = ('Weightlifting_Survey.csv')  -- list exact filename(s)
DESCRIBE TABLE customer_surveys.raw_data.PAK_survey_data;
CREATE SCHEMA IF NOT EXISTS customer_surveys.cleaned;
USE SCHEMA customer_surveys.cleaned;
-- Create cleaned weightlifting survey table for manual processing/checking
-- NOTE: This is a working copy - raw_data.PAK_survey_data stays UNTOUCHED for dbt
-- dbt will read directly from raw_data.PAK_survey_data with all original columns
DROP TABLE IF EXISTS customer_surveys.cleaned.PAK_survey_cleaned;
CREATE OR REPLACE TABLE customer_surveys.cleaned.PAK_survey_cleaned CLONE customer_surveys.raw_data.PAK_survey_data;
SELECT
    TOP 15 *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned;
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
    band_oppinion = 'maybe';
    
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
    //cleaning and dimensioning
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS response_id NUMBER;
UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned t
SET
    response_id = seq.seq_id
FROM
    (
        SELECT
            survey_date,
            row_number() OVER (
                ORDER BY
                    survey_date
            ) AS seq_id
        FROM
            customer_surveys.cleaned.PAK_survey_cleaned
    ) seq
WHERE
    t.survey_date = seq.survey_date;
SELECT
    TOP 15 *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
ORDER BY
    response_id ASC;
    //competetive
    //NOTE: Keeping raw_data.PAK_survey_data UNTOUCHED for dbt
    //The raw table will remain in its original state with all columns intact
    //dbt will read from raw_data.PAK_survey_data and do all transformations
    -- 
    -- For manual processing/checking, continue working with cleaned.PAK_survey_cleaned
    -- DO NOT SWAP or DROP columns from raw_data.PAK_survey_data
    --
    -- If you need to drop columns for manual processing, do it on the cleaned table:
    -- ALTER TABLE customer_surveys.cleaned.PAK_survey_cleaned DROP COLUMN current_gym_frequency, ideal_gym_frequency, motivations, stats;
    
    -- View the cleaned table (for manual checks)
    SELECT TOP 15 * FROM customer_surveys.cleaned.PAK_survey_cleaned ORDER BY response_id ASC;
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS compt_clean VARCHAR(1000);
UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned
SET
    compt_clean = regexp_replace(LOWER(trim(competetive)), '[^a-z0-9 ]', '');
    // catagorizing by join - inceased durability and usability
    -- Create the dimension table in the cleaned schema
    CREATE
    OR REPLACE TABLE customer_surveys.cleaned.dim_competitive_map (raw_value STRING, category STRING);
-- Insert values into the table (note: using cleaned schema)
INSERT INTO
    customer_surveys.cleaned.dim_competitive_map
VALUES
    -- exact matches
    ('yes', 'yes'),
    ('no', 'no'),
    ('idc', 'no'),
    ('maybe', 'no'),
    ('neutral', 'no'),
    -- pattern-based values (edge cases)
    ('depends', 'conditional'),
    ('option', 'conditional'),
    ('compete', 'conditional'),
    ('leaderboard', 'conditional'),
    -- fallback catch-all
    ('*', 'other');
-- Query with joins (all references use cleaned schema)
    WITH cleaned AS (
        SELECT
            response_id,
            LOWER(TRIM(competetive)) AS text_clean
        FROM
            customer_surveys.cleaned.PAK_survey_cleaned
    )
SELECT
    c.response_id,
    c.text_clean,
    COALESCE(
        m.category,
        -- exact match
        r.category,
        -- pattern match
        f.category -- fallback
    ) AS preference_category
FROM
    cleaned c -- exact matches
    LEFT JOIN customer_surveys.cleaned.dim_competitive_map m ON c.text_clean = m.raw_value
    AND m.raw_value NOT IN ('depends', 'option', 'compete', 'leaderboard', '*') -- pattern matches
    LEFT JOIN customer_surveys.cleaned.dim_competitive_map r ON c.text_clean ILIKE '%' || r.raw_value || '%'
    AND r.raw_value IN ('depends', 'option', 'compete', 'leaderboard') -- fallback
    LEFT JOIN customer_surveys.cleaned.dim_competitive_map f ON f.raw_value = '*';
SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.leaderboard,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    leaderboard
ORDER BY
    freq ASC;
-- ============================================
    -- 1. CHECK RAW LEADERBOARD DATA
    -- ============================================
SELECT
    COUNT(*) AS total_rows,
    COUNT(leaderboard) AS rows_with_leaderboard,
    COUNT(*) - COUNT(leaderboard) AS null_rows
FROM
    customer_surveys.cleaned.PAK_survey_cleaned;
-- ============================================
    -- 2. ADD leader_clean COLUMN IF NOT EXISTS
    -- ============================================
ALTER TABLE
    customer_surveys.cleaned.PAK_survey_cleaned
ADD
    COLUMN IF NOT EXISTS leader_clean VARCHAR(1000);
-- ============================================
    -- 3. CLEAN RAW TEXT INTO leader_clean
    -- ============================================
UPDATE
    customer_surveys.cleaned.PAK_survey_cleaned
SET
    leader_clean = regexp_replace(LOWER(trim(leaderboard)), '[^a-z0-9 ]', '')
WHERE
    leaderboard IS NOT NULL;
-- ============================================
    -- 4. CREATE DIMENSION TABLE FOR MAPPING
    -- ============================================
    CREATE
    OR REPLACE TABLE customer_surveys.cleaned.dim_leader_map (raw_value STRING, category STRING);
INSERT INTO
    customer_surveys.cleaned.dim_leader_map
VALUES
    -- exact matches
    ('yes', 'yes'),
    ('no', 'no'),
    ('neutral', 'no'),
    -- pattern-based values
    ('dont care', 'no'),
    ('middle', 'no'),
    ('not', 'no'),
    -- conditional
    ('depends', 'conditional'),
    ('option', 'conditional'),
    ('compete', 'conditional'),
    ('leaderboard', 'conditional'),
    -- fallback
    ('*', 'other');
-- ============================================
    -- 5. UPDATE leader_clean TO FINAL CATEGORY
    -- ============================================
    -- Use CTE to calculate categories, then MERGE
    MERGE INTO customer_surveys.cleaned.PAK_survey_cleaned t USING (
        WITH categorized AS (
            SELECT
                t.response_id,
                t.leader_clean,
                -- exact match
                m.category AS exact_category,
                -- pattern match
                r.category AS pattern_category,
                -- fallback
                f.category AS fallback_category
            FROM
                customer_surveys.cleaned.PAK_survey_cleaned t
                LEFT JOIN customer_surveys.cleaned.dim_leader_map m ON t.leader_clean = m.raw_value
                AND m.raw_value IN ('yes', 'no', 'neutral')
                LEFT JOIN customer_surveys.cleaned.dim_leader_map r ON t.leader_clean ILIKE '%' || r.raw_value || '%'
                AND r.raw_value IN (
                    'dont care',
                    'middle',
                    'not',
                    'depends',
                    'option',
                    'compete',
                    'leaderboard'
                )
                LEFT JOIN customer_surveys.cleaned.dim_leader_map f ON f.raw_value = '*'
            WHERE
                t.leader_clean IS NOT NULL
        )
        SELECT
            response_id,
            COALESCE(
                exact_category,
                pattern_category,
                fallback_category
            ) AS new_category
        FROM
            categorized
    ) src ON t.response_id = src.response_id
    WHEN MATCHED THEN
UPDATE
SET
    leader_clean = src.new_category;
-- ============================================
    -- 6. VERIFY FINAL CATEGORY DISTRIBUTION
    -- ============================================
SELECT
    customer_surveys.cleaned.PAK_survey_cleaned.leader_clean,
    COUNT(*) AS freq
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
GROUP BY
    leader_clean
ORDER BY
    freq ASC;
SELECT
    *
FROM
    customer_surveys.cleaned.PAK_survey_cleaned
ORDER BY
    response_id ASC;

SELECT
    *
FROM
customer_surveys.raw_data.PAK_survey_data


--cleaned app 

SELECT * FROM CUSTOMER_SURVEYS.DBT_JACK.stg_survey LIMIT 10;
//dbt success :)




