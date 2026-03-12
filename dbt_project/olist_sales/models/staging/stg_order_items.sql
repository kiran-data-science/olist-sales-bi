WITH source AS (
    SELECT * FROM {{ source('olist', 'olist_order_items_dataset') }}
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at,
    CAST(price AS DECIMAL(10,2))           AS price,
    CAST(freight_value AS DECIMAL(10,2))   AS freight_value,
    price + freight_value                  AS total_item_value
FROM source