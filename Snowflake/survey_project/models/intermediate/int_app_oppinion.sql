{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: App Opinion Categories
-- Categorizes app opinion field with yes/no logic
-- Edge cases treated as 'no'

WITH dim_app_oppinion_map AS (
    SELECT * FROM (VALUES
        ('yes', 'yes'),
        ('no', 'no'),
        ('if', 'no'),
        ('maybe', 'no'),
        ('neutral', 'no'),
        ('not sure', 'no'),
        ('dont know', 'no')
    ) AS t(raw_value, category)
),

stg_with_clean AS (
    SELECT
        *,
        -- Create cleaned version for categorization
        regexp_replace(lower(trim(app_oppinion)), '[^a-z0-9 ]', '') AS app_oppinion_clean
    FROM {{ ref('stg_survey') }}
),

categorized AS (
    SELECT
        stg.*,
        -- Exact match
        m.category AS exact_category,
        -- Pattern match (for multi-word edge cases)
        r.category AS other_category
    FROM stg_with_clean stg
    LEFT JOIN dim_app_oppinion_map m 
        ON stg.app_oppinion_clean = m.raw_value
    LEFT JOIN dim_app_oppinion_map r 
        ON stg.app_oppinion_clean ILIKE '%' || r.raw_value || '%'
        AND r.raw_value IN ('not sure', 'dont know')
        AND m.raw_value IS NULL  -- Only use pattern match if no exact match
    WHERE stg.app_oppinion_clean IS NOT NULL
)

SELECT
    *,
    COALESCE(
        exact_category,
        other_category,
        'no'
    ) AS app_oppinion_category
FROM categorized

