{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Analyze gym frequency gaps
-- Calculates average days and categorizes frequency gaps

with frequency_parsed as (
    select * from {{ ref('int_gym_frequency_parsed') }}
)

select
    *,
    
    -- Calculate average days
    (min_days_current + max_days_current) / 2.0 as avg_days_current,
    (min_days_ideal + max_days_ideal) / 2.0 as avg_days_ideal,
    
    -- Calculate frequency gap
    (min_days_ideal + max_days_ideal) / 2.0 - (min_days_current + max_days_current) / 2.0 as frequency_gap,
    
    -- Categorize gap
    case
        when (min_days_ideal + max_days_ideal) / 2.0 = (min_days_current + max_days_current) / 2.0 then 'Met Goal'
        when (min_days_ideal + max_days_ideal) / 2.0 > (min_days_current + max_days_current) / 2.0 then 'Below Goal'
        when (min_days_ideal + max_days_ideal) / 2.0 < (min_days_current + max_days_current) / 2.0 then 'Above Goal'
        else 'Unknown'
    end as frequency_gap_category

from frequency_parsed

