{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Explode reasons into individual rows
-- Each reason becomes a separate row

with base as (
    select * from {{ ref('stg_survey') }}
),

exploded as (
    select
        base.*,
        trim(f.value::string) as reason_item
    from base,
    lateral flatten(input => split(base.reason, ';')) f
    where trim(f.value::string) <> ''
)

select *
from exploded

