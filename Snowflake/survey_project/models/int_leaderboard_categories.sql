{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Categorize leaderboard field using dimension table logic
-- Matches the MERGE logic from Loading_Survey_Data_v3.sql

with stg as (
    select * from {{ ref('stg_survey') }}
),

dim_leader_map as (
    select * from (values
        ('yes', 'yes'),
        ('no', 'no'),
        ('neutral', 'no'),
        ('dont care', 'no'),
        ('middle', 'no'),
        ('not', 'no'),
        ('depends', 'conditional'),
        ('option', 'conditional'),
        ('if', 'conditional'),
        ('*', 'other')
    ) as t(raw_value, category)
),

cleaned_text as (
    select
        *,
        lower(regexp_replace(trim(leaderboard), '[^a-z0-9 ]', '')) as leaderboard_clean
    from stg
),

categorized as (
    select
        cleaned_text.*,
        -- Exact match
        m.category as exact_category,
        -- Pattern match
        r.category as pattern_category,
        -- Fallback
        f.category as fallback_category
    from cleaned_text
    left join dim_leader_map m
        on cleaned_text.leaderboard_clean = m.raw_value
        and m.raw_value in ('yes', 'no', 'neutral')
    left join dim_leader_map r
        on cleaned_text.leaderboard_clean ilike '%' || r.raw_value || '%'
        and r.raw_value in (
            'dont care',
            'middle',
            'not',
            'depends',
            'option',
            'if'
        )
    left join dim_leader_map f
        on f.raw_value = '*'
    where cleaned_text.leaderboard_clean is not null
)

select
    *,
    coalesce(
        exact_category,
        pattern_category,
        fallback_category,
        'other'
    ) as leaderboard_category
from categorized

