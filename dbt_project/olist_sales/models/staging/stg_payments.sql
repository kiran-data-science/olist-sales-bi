WITH source AS (
    SELECT * FROM {{ source('olist', 'olist_order_payments_dataset') }}
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS DECIMAL(10,2)) AS payment_value
FROM source