with raw as (

    select
        *
    from {{ source('survey_source', 'PAK_SURVEY_DATA') }}

),

renamed as (

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
        attendance
    from raw

)

select *
from renamed