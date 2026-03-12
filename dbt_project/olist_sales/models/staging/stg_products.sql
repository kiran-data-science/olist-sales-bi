WITH source AS (
    SELECT * FROM {{ source('olist', 'olist_products_dataset') }}
),

translation AS (
    SELECT * FROM {{ source('olist', 'product_category_name_translation') }}
)

SELECT
    p.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category_english,
    p.product_name_lenght        AS product_name_length,
    p.product_description_lenght AS product_description_length,
    p.product_photos_qty         AS photos_qty,
    p.product_weight_g           AS weight_g,
    p.product_length_cm          AS length_cm,
    p.product_height_cm          AS height_cm,
    p.product_width_cm           AS width_cm
FROM source p
LEFT JOIN translation t
    ON p.product_category_name = t.product_category_name