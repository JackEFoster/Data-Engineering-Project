{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Attendance Categories
-- Categorizes attendance field with yes/no/conditional logic

WITH dim_attendance_map AS (
    SELECT * FROM (VALUES
        ('yes', 'yes'),
        ('no', 'no'),
        ('maybe', 'no'),
        ('neutral', 'no'),
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
        lower(regexp_replace(trim(attendance), '[^a-z0-9 ]', '')) AS attendance_clean
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
    LEFT JOIN dim_attendance_map m 
        ON stg.attendance_clean = m.raw_value
        AND m.raw_value IN ('yes', 'no', 'neutral', 'maybe')
    LEFT JOIN dim_attendance_map r 
        ON stg.attendance_clean ILIKE '%' || r.raw_value || '%'
        AND r.raw_value IN ('depends', 'option', 'if')
        AND m.raw_value IS NULL  -- Only use pattern match if no exact match
    LEFT JOIN dim_attendance_map f 
        ON f.raw_value = '*'
        AND m.raw_value IS NULL  -- No exact match
        AND r.raw_value IS NULL  -- No pattern match
    WHERE stg.attendance_clean IS NOT NULL
)

SELECT
    *,
    COALESCE(
        exact_category,
        pattern_category,
        fallback_category,
        'other'
    ) AS attendance_category
FROM categorized

