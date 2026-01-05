{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Categorize experience levels
-- This creates a reusable categorization of experience levels
-- Experience is typically a numeric field (0-10 scale)

SELECT 
    *,
    CASE 
        WHEN experience >= 7 THEN 'High Experience (7+)'
        WHEN experience >= 4 THEN 'Medium Experience (4-6)'
        WHEN experience >= 1 THEN 'Low Experience (1-3)'
        WHEN experience = 0 THEN 'No Experience (0)'
        WHEN experience IS NULL THEN 'Unknown'
        ELSE 'Unknown'
    END AS experience_level,
    
    CASE 
        WHEN experience >= 7 THEN 3
        WHEN experience >= 4 THEN 2
        WHEN experience >= 1 THEN 1
        WHEN experience = 0 THEN 0
        ELSE NULL
    END AS experience_level_numeric

FROM {{ ref('stg_survey') }}
WHERE experience IS NOT NULL

