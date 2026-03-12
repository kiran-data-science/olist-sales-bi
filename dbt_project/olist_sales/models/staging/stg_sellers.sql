WITH source AS (
    SELECT * FROM {{ source('olist', 'olist_sellers_dataset') }}
)

SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    seller_city            AS city,
    UPPER(seller_state)    AS state
FROM source