{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Survey Date Parsing and Analysis
-- Parses raw survey_date format: "2023/02/15 1:23:40 PM MST"
-- Extracts date/time components for analysis

WITH base AS (
    SELECT 
        * EXCLUDE survey_date,
        survey_date AS survey_date_raw
    FROM {{ ref('stg_survey') }}
),

parsed AS (
    SELECT
        *,
        -- Parse the timestamp (handles format: "2023/02/15 1:23:40 PM MST")
        TRY_TO_TIMESTAMP(survey_date_raw, 'YYYY/MM/DD HH12:MI:SS AM TZ') AS survey_timestamp,
        TRY_TO_TIMESTAMP(survey_date_raw, 'YYYY/MM/DD HH12:MI:SS AM') AS survey_timestamp_no_tz
    FROM base
),

enriched AS (
    SELECT
        *,
        -- Use timestamp with timezone if available, otherwise without
        COALESCE(survey_timestamp, survey_timestamp_no_tz) AS survey_datetime,
        
        -- Extract date components
        DATE(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_date,
        YEAR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_year,
        MONTH(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_month,
        DAY(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_day,
        DAYOFWEEK(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_day_of_week,
        DAYNAME(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_day_name,
        
        -- Extract time components
        HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_hour,
        MINUTE(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_minute,
        SECOND(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_second,
        
        -- Extract time period
        CASE 
            WHEN HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) < 12 THEN 'AM'
            ELSE 'PM'
        END AS survey_am_pm,
        
        -- Time of day category
        CASE 
            WHEN HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) BETWEEN 5 AND 11 THEN 'Morning (5-11)'
            WHEN HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
            WHEN HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) BETWEEN 18 AND 21 THEN 'Evening (18-21)'
            WHEN HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) BETWEEN 22 AND 23 
                 OR HOUR(COALESCE(survey_timestamp, survey_timestamp_no_tz)) BETWEEN 0 AND 4 THEN 'Night (22-4)'
            ELSE 'Unknown'
        END AS survey_time_of_day,
        
        -- Week and quarter indicators
        WEEK(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_week,
        QUARTER(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_quarter,
        
        -- Month name
        MONTHNAME(COALESCE(survey_timestamp, survey_timestamp_no_tz)) AS survey_month_name,
        
        -- Year-Month for grouping
        TO_CHAR(COALESCE(survey_timestamp, survey_timestamp_no_tz), 'YYYY-MM') AS survey_year_month,
        
        -- Year-Week for grouping
        TO_CHAR(COALESCE(survey_timestamp, survey_timestamp_no_tz), 'YYYY-WW') AS survey_year_week,
        
        -- Is weekend flag
        CASE 
            WHEN DAYOFWEEK(COALESCE(survey_timestamp, survey_timestamp_no_tz)) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        
        -- Days since epoch (for calculations)
        DATEDIFF('day', '1970-01-01', DATE(COALESCE(survey_timestamp, survey_timestamp_no_tz))) AS days_since_epoch
        
    FROM parsed
    WHERE COALESCE(survey_timestamp, survey_timestamp_no_tz) IS NOT NULL
)

SELECT
    *
FROM enriched
ORDER BY survey_datetime
