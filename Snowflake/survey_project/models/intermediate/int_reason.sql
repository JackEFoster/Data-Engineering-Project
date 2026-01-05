{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Explode reason into individual rows
-- Each reason becomes a separate row
-- Example: "Reason1;Reason2;Reason3" becomes 3 rows

WITH base AS (
    SELECT * FROM {{ ref('stg_survey') }}
),

exploded AS (
    SELECT
        base.*,
        TRIM(f.value::string) AS reason_item
    FROM base,
    LATERAL FLATTEN(
        INPUT => SPLIT(COALESCE(NULLIF(TRIM(base.reason), ''), ''), ';')
    ) f
    WHERE TRIM(f.value::string) <> ''
        AND f.value::string IS NOT NULL
)

SELECT
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
    reason_item  -- Make sure this column is explicitly selected
FROM exploded
ORDER BY survey_date, reason_item

