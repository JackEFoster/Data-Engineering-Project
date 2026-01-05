{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Categorize competitive field using dimension table logic
-- Matches the MERGE logic from Loading_Survey_Data_v3.sql

with stg as (
    select * from {{ ref('stg_survey') }}
),

dim_competitive_map as (
    select * from (values
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
    ) as t(raw_value, category)
),

cleaned_text as (
    select
        *,
        lower(regexp_replace(trim(competetive), '[^a-z0-9 ]', '')) as competitive_clean
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
    left join dim_competitive_map m
        on cleaned_text.competitive_clean = m.raw_value
        and m.raw_value in ('yes', 'no', 'idc', 'maybe', 'neutral')
    left join dim_competitive_map r
        on cleaned_text.competitive_clean ilike '%' || r.raw_value || '%'
        and r.raw_value in ('depends', 'option', 'compete', 'leaderboard')
    left join dim_competitive_map f
        on f.raw_value = '*'
    where cleaned_text.competitive_clean is not null
)

select
    *,
    coalesce(
        exact_category,
        pattern_category,
        fallback_category,
        'other'
    ) as competitive_category
from categorized

