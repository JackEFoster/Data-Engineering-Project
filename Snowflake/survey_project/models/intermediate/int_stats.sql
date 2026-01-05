{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Explode stats into individual rows
-- Each stat becomes a separate row
-- Example: "Stat1;Stat2;Stat3" becomes 3 rows

WITH base AS (
    SELECT * FROM {{ ref('stg_survey') }}
),

exploded AS (
    SELECT
        base.*,
        TRIM(f.value::string) AS stat_item
    FROM base,
    LATERAL FLATTEN(
        INPUT => SPLIT(COALESCE(NULLIF(TRIM(base.stats), ''), ''), ';')
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
    stat_item  -- Make sure this column is explicitly selected
FROM exploded
ORDER BY survey_date, stat_item

