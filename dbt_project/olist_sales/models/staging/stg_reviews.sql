WITH source AS (
    SELECT * FROM {{ source('olist', 'olist_order_reviews_dataset') }}
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS TIMESTAMP)  AS review_created_at,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answered_at
FROM source