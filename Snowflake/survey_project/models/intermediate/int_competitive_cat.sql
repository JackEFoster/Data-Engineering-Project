{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

-- Intermediate model: Competitive Categories
-- Categorizes competitive field using dimension table logic
-- Matches the MERGE logic from Loading_Survey_Data (2).sql

WITH dim_competitive_map AS (
    SELECT * FROM (VALUES
        ('yes', 'yes'),
        ('no', 'no'),
        ('idc', 'no'),
        ('maybe', 'no'),
        ('neutral', 'no'),
        ('depends', 'conditional'),
        ('option', 'conditional'),
        ('compete', 'conditional'),
        ('leaderboard', 'conditional'),
        ('*', 'other')
    ) AS t(raw_value, category)
),

stg_with_clean AS (
    SELECT
        *,
        -- Create cleaned version for categorization
        regexp_replace(lower(trim(competetive)), '[^a-z0-9 ]', '') AS competitive_clean
    FROM {{ ref('stg_survey') }}
),

categorized AS (
    SELECT
        stg.*,
        -- Exact match (excluding wildcard)
        m.category AS exact_category,
        -- Pattern match
        r.category AS pattern_category,
        -- Fallback (only when no exact or pattern match)
        f.category AS fallback_category
    FROM stg_with_clean stg
    LEFT JOIN dim_competitive_map m 
        ON stg.competitive_clean = m.raw_value
        AND m.raw_value != '*'
    LEFT JOIN dim_competitive_map r 
        ON stg.competitive_clean ILIKE '%' || r.raw_value || '%'
        AND r.raw_value IN ('depends', 'option', 'compete', 'leaderboard')
        AND m.raw_value IS NULL  -- Only use pattern match if no exact match
    LEFT JOIN dim_competitive_map f 
        ON f.raw_value = '*'
        AND m.raw_value IS NULL  -- No exact match
        AND r.raw_value IS NULL  -- No pattern match
    WHERE stg.competitive_clean IS NOT NULL
)

SELECT
    *,
    COALESCE(
        exact_category,
        pattern_category,
        fallback_category,
        'other'
    ) AS competitive_category
FROM categorized