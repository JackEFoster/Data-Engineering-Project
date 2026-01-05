{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: App Categories
-- Categorizes app field with yes/no/conditional logic
-- Similar pattern to leaderboard and competitive fields

WITH dim_app_map AS (
    SELECT * FROM (VALUES
        ('yes', 'yes'),
        ('no', 'no'),
        ('neutral', 'no'),
        ('maybe', 'no'),
        ('depends', 'conditional'),
        ('option', 'conditional'),
        ('if', 'conditional'),
        ('*', 'other')
    ) AS t(raw_value, category)
),

stg_with_clean AS (
    SELECT
        *,
        -- Create cleaned version for categorization
        lower(regexp_replace(trim(app), '[^a-z0-9 ]', '')) AS app_clean
    FROM {{ ref('stg_survey') }}
),

categorized AS (
    SELECT
        stg.*,
        -- Exact match
        m.category AS exact_category,
        -- Pattern match
        r.category AS pattern_category,
        -- Fallback
        f.category AS fallback_category
    FROM stg_with_clean stg
    LEFT JOIN dim_app_map m 
        ON stg.app_clean = m.raw_value
        AND m.raw_value IN ('yes', 'no', 'neutral', 'maybe')
    LEFT JOIN dim_app_map r 
        ON stg.app_clean ILIKE '%' || r.raw_value || '%'
        AND r.raw_value IN ('depends', 'option', 'if')
        AND m.raw_value IS NULL  -- Only use pattern match if no exact match
    LEFT JOIN dim_app_map f 
        ON f.raw_value = '*'
        AND m.raw_value IS NULL  -- No exact match
        AND r.raw_value IS NULL  -- No pattern match
    WHERE stg.app_clean IS NOT NULL
)

SELECT
    *,
    COALESCE(
        exact_category,
        pattern_category,
        fallback_category,
        'other'
    ) AS app_category
FROM categorized

