with raw as (

    select
        *
    from {{ source('survey_source', 'PAK_SURVEY_DATA') }}

),

renamed as (

    select
        raw:SURVEY_DATE::varchar as survey_date,
        raw:APP_OPPINION::varchar as app_oppinion,
        raw:BAND_OPPINION::varchar as band_oppinion,
        raw:EXPERIENCE::int as experience,
        raw:MIN_DAYS_CURR::varchar as current_gym_frequency,
        raw:MAX_DAYS::varchar as ideal_gym_frequency,
        raw:MOTIVATIONS::varchar as motivations,
        raw:STATS_ARR::varchar as stats,
        raw:COMPETETIVE::varchar as competetive,
        raw:LEADERBOARD::varchar as leaderboard,
        raw:APP::varchar as app,
        raw:REASON::varchar as reason,
        raw:FEEDBACK::varchar as feedback,
        raw:ATTENDANCE::varchar as attendance
    from raw

)

select *
from renamed;