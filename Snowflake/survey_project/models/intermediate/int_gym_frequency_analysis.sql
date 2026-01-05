{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

-- Intermediate model: Gym Frequency Analysis
-- Analyzes gap between current and ideal gym frequency
-- Calculates frequency gap metrics

SELECT
    survey_date,
    experience,
    response_id,
    min_days_curr,
    max_days_curr,
    min_days_id,
    max_days_id,
    current_gym_frequency,
    ideal_gym_frequency,
    
    -- Calculate average days
    (min_days_curr + max_days_curr) / 2.0 AS avg_days_current,
    (min_days_id + max_days_id) / 2.0 AS avg_days_ideal,
    
    -- Calculate frequency gap
    (min_days_id + max_days_id) / 2.0 - (min_days_curr + max_days_curr) / 2.0 AS frequency_gap,
    
    -- Categorize gap
    CASE
        WHEN (min_days_id + max_days_id) / 2.0 = (min_days_curr + max_days_curr) / 2.0 THEN 'Met Goal'
        WHEN (min_days_id + max_days_id) / 2.0 > (min_days_curr + max_days_curr) / 2.0 THEN 'Below Goal'
        WHEN (min_days_id + max_days_id) / 2.0 < (min_days_curr + max_days_curr) / 2.0 THEN 'Above Goal'
        ELSE 'Unknown'
    END AS frequency_gap_category

FROM {{ ref('stg_survey_cleanish') }}
