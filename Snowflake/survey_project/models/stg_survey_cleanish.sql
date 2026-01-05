with raw as (

    select
        *
    from {{ source('customer_surveys', 'PAK_survey_cleaned') }}

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
        attendance,
        min_days_curr,
        max_days_curr,
        min_days_id,
        max_days_id,
        mot_arr,
        stats_arr,
        response_id
    from raw

)

select *
from renamed