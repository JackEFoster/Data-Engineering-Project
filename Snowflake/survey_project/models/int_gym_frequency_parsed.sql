{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Parse gym frequency strings into min/max days
-- Parses "X-Y Days" format into separate min and max day columns

with stg as (
    select * from {{ ref('stg_survey') }}
)

select
    *,
    
    -- Parse current gym frequency: "X-Y Days" -> min and max
    try_cast(
        split_part(
            replace(trim(current_gym_frequency), ' Days', ''),
            '-',
            1
        ) as number
    ) as min_days_current,
    
    try_cast(
        split_part(
            replace(trim(current_gym_frequency), ' Days', ''),
            '-',
            2
        ) as number
    ) as max_days_current,
    
    -- Parse ideal gym frequency: "X-Y Days" -> min and max
    try_cast(
        split_part(
            replace(trim(ideal_gym_frequency), ' Days', ''),
            '-',
            1
        ) as number
    ) as min_days_ideal,
    
    try_cast(
        split_part(
            replace(trim(ideal_gym_frequency), ' Days', ''),
            '-',
            2
        ) as number
    ) as max_days_ideal

from stg

