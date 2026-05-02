{{ config(materialized='table') }}

WITH clean AS (
    SELECT *
    FROM {{ ref('dim_questions_clean') }}
)

SELECT
    exam_name,
    year,
    section,
    topic,
    difficulty,
    COUNT(*) as question_count,
    ROUND(AVG(confidence), 2) as avg_confidence,
    ROUND(AVG(expected_time_seconds), 0) as avg_time_seconds,
    SUM(CASE WHEN has_diagram THEN 1 ELSE 0 END) as diagram_count
FROM clean
GROUP BY exam_name, year, section, topic, difficulty
ORDER BY section, topic, difficulty