WITH source AS (
    SELECT *
    FROM `project-3639c8e1-b432-4a18-99f.question_bank.dim_questions`
)

SELECT
    question_id,
    paper_id,
    year,
    exam_name,
    phase,
    section,
    question_number,
    question_text,
    option_1,
    option_2,
    option_3,
    option_4,
    correct_answer,
    solution,
    subject,
    topic,
    difficulty,
    expected_time_seconds,
    has_diagram,
    has_question_diagram,
    has_option_diagram,
    has_solution_diagram,
    confidence,
    is_reviewed,

    -- Diagram URLs (singular - first image)
    question_diagram_url,
    solution_diagram_url,
    option_1_diagram_url,
    option_2_diagram_url,
    option_3_diagram_url,
    option_4_diagram_url,

    -- Diagram URLs (plural - all images JSON array)
    question_diagram_urls,
    solution_diagram_urls,
    option_1_diagram_urls,
    option_2_diagram_urls,
    option_3_diagram_urls,
    option_4_diagram_urls,

    -- Quality flag
    CASE
        WHEN question_text IS NULL OR question_text = '' THEN 'missing_text'
        WHEN correct_answer NOT IN ('1','2','3','4') THEN 'invalid_answer'
        WHEN confidence < 0.8 THEN 'low_confidence'
        ELSE 'ok'
    END AS quality_flag,

    -- Difficulty bucket for easier filtering
    CASE
        WHEN expected_time_seconds <= 30 THEN 'quick'
        WHEN expected_time_seconds <= 60 THEN 'standard'
        ELSE 'long'
    END AS time_bucket

FROM source