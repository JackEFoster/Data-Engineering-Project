{{
  config(
    materialized='table',
    tags=['facts']
  )
}}

-- Fact table: Survey Responses
-- Grain: One row per survey response
-- This is the central fact table that combines all intermediate models
-- All marts should reference this fact table

SELECT
    -- ============================================
    -- PRIMARY KEY
    -- ============================================
    COALESCE(clean.response_id, ROW_NUMBER() OVER (ORDER BY stg.survey_date)) AS survey_response_id,
    
    -- ============================================
    -- DIMENSION KEYS (Foreign Keys)
    -- ============================================
    stg.survey_date AS survey_date_key,
    
    -- Experience dimension key
    exp.experience_level_numeric AS experience_level_key,
    exp.experience_level AS experience_level_category,
    
    -- ============================================
    -- MEASURES (Facts - Numeric values to aggregate)
    -- ============================================
    
    -- Experience measure
    stg.experience AS experience_value,
    
    -- Frequency measures (from int_gym_frequency_analysis)
    freq.min_days_curr AS min_days_current,
    freq.max_days_curr AS max_days_current,
    freq.min_days_id AS min_days_ideal,
    freq.max_days_id AS max_days_ideal,
    freq.avg_days_current,
    freq.avg_days_ideal,
    freq.frequency_gap,
    
    -- ============================================
    -- CATEGORIZED DIMENSIONS (Degenerate dimensions)
    -- ============================================
    
    -- Opinion categorizations
    app_opinion.app_oppinion_category,
    band_opinion.band_oppinion_category,
    app_cat.app_category,
    attendance_cat.attendance_category,
    
    -- Competitive categorization
    COALESCE(comp.competitive_category, 'other') AS competitive_category,
    
    -- Leaderboard categorization
    COALESCE(lead.leaderboard_category, 'other') AS leaderboard_category,
    
    -- Frequency gap category
    freq.frequency_gap_category,
    
    -- ============================================
    -- DESCRIPTIVE ATTRIBUTES (for filtering/grouping)
    -- ============================================
    
    -- Opinions (text)
    stg.app_oppinion,
    stg.band_oppinion,
    
    -- Frequency strings
    stg.current_gym_frequency,
    stg.ideal_gym_frequency,
    
    -- Text fields (for reference)
    stg.motivations,
    stg.stats,
    stg.competetive,
    stg.leaderboard,
    stg.app,
    stg.reason,
    stg.feedback,
    stg.attendance,
    
    -- Feedback analysis (from int_feedback)
    feedback_analysis.feedback_sentiment,
    feedback_analysis.feedback_length,
    feedback_analysis.has_feedback,
    feedback_analysis.mentions_app,
    feedback_analysis.mentions_band,
    feedback_analysis.mentions_leaderboard,
    feedback_analysis.mentions_stats,
    
    -- Date/time components (from int_survey_date)
    date_analysis.survey_datetime,
    date_analysis.survey_year,
    date_analysis.survey_month,
    date_analysis.survey_day,
    date_analysis.survey_day_of_week,
    date_analysis.survey_day_name,
    date_analysis.survey_hour,
    date_analysis.survey_time_of_day,
    date_analysis.survey_quarter,
    date_analysis.is_weekend,
    
    -- ============================================
    -- RAW VALUES (preserved for audit)
    -- ============================================
    stg.survey_date AS survey_date_raw,
    stg.app_oppinion AS app_oppinion_raw,
    stg.band_oppinion AS band_oppinion_raw,
    stg.experience AS experience_raw,
    stg.current_gym_frequency AS current_gym_frequency_raw,
    stg.ideal_gym_frequency AS ideal_gym_frequency_raw,
    stg.competetive AS competetive_raw,
    stg.leaderboard AS leaderboard_raw

FROM {{ ref('stg_survey') }} stg

-- Join cleaned survey (for response_id if available)
LEFT JOIN {{ ref('stg_survey_cleanish') }} clean
    ON stg.survey_date = clean.survey_date
    AND stg.experience = clean.experience

-- Join experience categorization
LEFT JOIN {{ ref('int_expirience') }} exp
    ON stg.survey_date = exp.survey_date
    AND stg.experience = exp.experience

-- Join competitive categorization
LEFT JOIN {{ ref('int_competitive_cat') }} comp
    ON stg.survey_date = comp.survey_date
    AND COALESCE(stg.competetive, '') = COALESCE(comp.competetive, '')

-- Join leaderboard categorization
LEFT JOIN {{ ref('int_leaderboard') }} lead
    ON stg.survey_date = lead.survey_date
    AND stg.leaderboard = lead.leaderboard

-- Join frequency analysis (uses stg_survey_cleanish)
LEFT JOIN {{ ref('int_gym_frequency_analysis') }} freq
    ON clean.survey_date = freq.survey_date
    AND clean.experience = freq.experience
    AND clean.current_gym_frequency = freq.current_gym_frequency
    AND clean.ideal_gym_frequency = freq.ideal_gym_frequency

-- Join app opinion categorization
LEFT JOIN {{ ref('int_app_oppinion') }} app_opinion
    ON stg.survey_date = app_opinion.survey_date
    AND stg.app_oppinion = app_opinion.app_oppinion

-- Join band opinion categorization
LEFT JOIN {{ ref('int_band_oppinion') }} band_opinion
    ON stg.survey_date = band_opinion.survey_date
    AND stg.band_oppinion = band_opinion.band_oppinion

-- Join app categorization
LEFT JOIN {{ ref('int_app') }} app_cat
    ON stg.survey_date = app_cat.survey_date
    AND stg.app = app_cat.app

-- Join attendance categorization
LEFT JOIN {{ ref('int_attendance') }} attendance_cat
    ON stg.survey_date = attendance_cat.survey_date
    AND stg.attendance = attendance_cat.attendance

-- Join feedback analysis
LEFT JOIN {{ ref('int_feedback') }} feedback_analysis
    ON stg.survey_date = feedback_analysis.survey_date
    AND stg.feedback = feedback_analysis.feedback

-- Join date analysis
LEFT JOIN {{ ref('int_survey_date') }} date_analysis
    ON stg.survey_date = date_analysis.survey_date_raw

