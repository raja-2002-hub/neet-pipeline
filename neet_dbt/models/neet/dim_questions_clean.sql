WITH staged AS (
    SELECT *
    FROM {{ ref('stg_questions') }}
)

SELECT *
FROM staged
WHERE quality_flag = 'ok'
AND confidence >= 0.8
AND question_text IS NOT NULL
AND question_text != ''