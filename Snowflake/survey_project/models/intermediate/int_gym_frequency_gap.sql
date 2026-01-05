{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

-- Intermediate model: Analyze gap between current and ideal gym frequency
-- This identifies customers who have a gap between their current and ideal frequency

SELECT 
    *,
    CASE 
        WHEN current_gym_frequency = ideal_gym_frequency THEN 'Met Goal'
        WHEN current_gym_frequency IS NULL OR ideal_gym_frequency IS NULL THEN 'Unknown'
        WHEN current_gym_frequency < ideal_gym_frequency THEN 'Below Goal'
        WHEN current_gym_frequency > ideal_gym_frequency THEN 'Above Goal'
        ELSE 'Gap Exists'
    END AS frequency_gap_status,
    
    -- Calculate gap (simplified - you might want to map frequencies to numbers)
    CASE 
        WHEN current_gym_frequency IS NOT NULL 
         AND ideal_gym_frequency IS NOT NULL
         AND current_gym_frequency != ideal_gym_frequency
        THEN TRUE
        ELSE FALSE
    END AS has_frequency_gap

FROM {{ ref('stg_survey_cleanish') }}

