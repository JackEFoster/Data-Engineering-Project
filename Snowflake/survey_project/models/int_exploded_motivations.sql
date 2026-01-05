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
        input => split(coalesce(base.motivations, ''), ';')
    ) f
    where trim(f.value::string) <> ''
)

select *
from exploded

