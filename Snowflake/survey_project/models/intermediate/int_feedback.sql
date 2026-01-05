{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Intermediate model: Feedback Analysis
-- Analyzes feedback text field for sentiment and key themes
-- Creates flags for common feedback patterns

SELECT 
    *,
    
    -- Text length analysis
    LENGTH(COALESCE(feedback, '')) AS feedback_length,
    
    -- Sentiment indicators (simple keyword-based)
    CASE 
        WHEN LOWER(feedback) LIKE '%love%' OR LOWER(feedback) LIKE '%great%' OR LOWER(feedback) LIKE '%awesome%' 
             OR LOWER(feedback) LIKE '%excellent%' OR LOWER(feedback) LIKE '%amazing%' THEN 'Positive'
        WHEN LOWER(feedback) LIKE '%hate%' OR LOWER(feedback) LIKE '%bad%' OR LOWER(feedback) LIKE '%terrible%' 
             OR LOWER(feedback) LIKE '%awful%' OR LOWER(feedback) LIKE '%worst%' THEN 'Negative'
        WHEN LOWER(feedback) LIKE '%ok%' OR LOWER(feedback) LIKE '%fine%' OR LOWER(feedback) LIKE '%alright%' 
             OR LOWER(feedback) LIKE '%neutral%' THEN 'Neutral'
        WHEN feedback IS NULL OR TRIM(feedback) = '' THEN 'No Feedback'
        ELSE 'Mixed/Other'
    END AS feedback_sentiment,
    
    -- Key theme flags
    CASE WHEN LOWER(feedback) LIKE '%app%' THEN TRUE ELSE FALSE END AS mentions_app,
    CASE WHEN LOWER(feedback) LIKE '%band%' THEN TRUE ELSE FALSE END AS mentions_band,
    CASE WHEN LOWER(feedback) LIKE '%leaderboard%' OR LOWER(feedback) LIKE '%leader%' THEN TRUE ELSE FALSE END AS mentions_leaderboard,
    CASE WHEN LOWER(feedback) LIKE '%stats%' OR LOWER(feedback) LIKE '%statistic%' THEN TRUE ELSE FALSE END AS mentions_stats,
    CASE WHEN LOWER(feedback) LIKE '%compet%' THEN TRUE ELSE FALSE END AS mentions_competitive,
    
    -- Has feedback flag
    CASE 
        WHEN feedback IS NOT NULL AND TRIM(feedback) != '' THEN TRUE 
        ELSE FALSE 
    END AS has_feedback

FROM {{ ref('stg_survey') }}

