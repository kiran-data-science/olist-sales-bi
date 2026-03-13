WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

items AS (
    SELECT
        order_id,
 	ANY_VALUE (seller_id)       AS seller_id,
	ANY_VALUE (product_id)      AS product_id,
        COUNT(order_item_id)        AS total_items,
        SUM(price)                  AS total_price,
        SUM(freight_value)          AS total_freight,
        SUM(total_item_value)       AS gmv
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value)          AS total_payment,
        COUNT(DISTINCT payment_type) AS payment_methods
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id
),

reviews AS (
    SELECT
        order_id,
        review_score,
        review_comment_message
    FROM {{ ref('stg_reviews') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchased_at,
    o.order_approved_at,
    o.order_shipped_at,
    o.order_delivered_at,
    o.order_estimated_at,

    -- Delivery metrics
    DATEDIFF('day', o.order_purchased_at, o.order_delivered_at)   AS actual_delivery_days,
    DATEDIFF('day', o.order_purchased_at, o.order_estimated_at)   AS estimated_delivery_days,
    DATEDIFF('day', o.order_estimated_at, o.order_delivered_at)   AS delivery_delay_days,

    CASE
        WHEN o.order_delivered_at <= o.order_estimated_at THEN 'On Time'
        WHEN o.order_delivered_at > o.order_estimated_at  THEN 'Late'
        ELSE 'Not Delivered'
    END AS delivery_status,

    -- Financial metrics
    i.total_items,
    i.seller_id,
    i.product_id,
    i.total_price,
    i.total_freight,
    i.gmv,
    p.total_payment,
    p.payment_methods,

    -- Review
    r.review_score,
    r.review_comment_message,

    -- Time dimensions
    DATE_TRUNC('month', o.order_purchased_at) AS order_month,
    DATE_TRUNC('week',  o.order_purchased_at) AS order_week,
    DAYOFWEEK(o.order_purchased_at)           AS order_day_of_week

FROM orders o
LEFT JOIN items    i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
LEFT JOIN reviews  r ON o.order_id = r.order_id