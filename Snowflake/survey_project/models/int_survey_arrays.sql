{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Create arrays from semicolon-separated fields
-- Converts motivations, stats, and reason into arrays for easier analysis

with stg as (
    select * from {{ ref('stg_survey') }}
)

select
    *,
    
    -- Create array from motivations (semicolon-separated)
    -- Example: "Motivation;Health;Sports" -> ['Motivation', 'Health', 'Sports']
    case
        when motivations is null or trim(motivations) = '' then array_construct()
        else split(trim(motivations), ';')
    end as motivations_array,
    
    -- Create array from stats (semicolon-separated)
    -- Example: "Reps;Power;Muscle Activation;Quality;No" -> ['Reps', 'Power', 'Muscle Activation', 'Quality', 'No']
    case
        when stats is null or trim(stats) = '' then array_construct()
        else split(trim(stats), ';')
    end as stats_array,
    
    -- Create array from reason (semicolon-separated)
    -- Example: "To gain muscle;To stay in shape;To lose weight" -> ['To gain muscle', 'To stay in shape', 'To lose weight']
    case
        when reason is null or trim(reason) = '' then array_construct()
        else split(trim(reason), ';')
    end as reason_array

from stg

