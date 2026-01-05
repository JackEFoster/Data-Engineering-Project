{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

-- Intermediate model: Leaderboard Categories
-- Categorizes leaderboard field using dimension table logic
-- Matches the MERGE logic from Loading_Survey_Data (2).sql

WITH dim_leader_map AS (
    SELECT * FROM (VALUES
        ('yes', 'yes'),
        ('no', 'no'),
        ('neutral', 'no'),
        ('dont care', 'no'),
        ('middle', 'no'),
        ('not', 'no'),
        ('depends', 'conditional'),
        ('option', 'conditional'),
        ('compete', 'conditional'),
        ('leaderboard', 'conditional'),
        ('*', 'other')
    ) AS t(raw_value, category)
),

categorized AS (
    SELECT
        stg.survey_date,
        stg.leaderboard,
        -- Exact match
        m.category AS exact_category,
        -- Pattern match
        r.category AS pattern_category,
        -- Fallback
        f.category AS fallback_category
    FROM {{ ref('stg_survey') }} stg
    LEFT JOIN dim_leader_map m 
        ON stg.leaderboard = m.raw_value
        AND m.raw_value IN ('yes', 'no', 'neutral')
    LEFT JOIN dim_leader_map r 
        ON stg.leaderboard ILIKE '%' || r.raw_value || '%'
        AND r.raw_value IN (
            'dont care',
            'middle',
            'not',
            'depends',
            'option',
            'compete',
            'leaderboard'
        )
    LEFT JOIN dim_leader_map f 
        ON f.raw_value = '*'
    WHERE stg.leaderboard IS NOT NULL
)

SELECT
    survey_date,
    leaderboard,
    COALESCE(
        exact_category,
        pattern_category,
        fallback_category
    ) AS leaderboard_category
FROM categorized

