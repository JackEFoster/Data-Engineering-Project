{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Explode motivations into individual rows
-- Each motivation becomes a separate row
-- Example: "Motivation;Health;Sports" becomes 3 rows

with base as (
    select * from {{ ref('stg_survey') }}
),

exploded as (
    select
        base.*,
        trim(f.value::string) as motivation_item
    from base,
    lateral flatten(
        input => split(coalesce(nullif(trim(base.motivations), ''), ''), ';')
    ) f
    where trim(f.value::string) <> ''
        and f.value::string is not null
)

select
    survey_date,
    app_oppinion,
    band_oppinion,
    experience,
    current_gym_frequency,
    ideal_gym_frequency,
    motivations,
    stats,
    competetive,
    leaderboard,
    app,
    reason,
    feedback,
    attendance,
    motivation_item  -- Make sure this column is explicitly selected
from exploded
order by survey_date, motivation_item