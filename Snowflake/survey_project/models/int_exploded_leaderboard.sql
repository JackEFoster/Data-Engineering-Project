{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Explode leaderboard into individual rows (if semicolon-separated)
-- Note: If leaderboard is just "No" (not semicolon-separated), this won't create multiple rows

with base as (
    select * from {{ ref('stg_survey') }}
),

exploded as (
    select
        base.*,
        trim(f.value::string) as leaderboard_item
    from base,
    lateral flatten(input => split(coalesce(base.leaderboard, ''), ';')) f
    where trim(f.value::string) <> ''
)

select *
from exploded

