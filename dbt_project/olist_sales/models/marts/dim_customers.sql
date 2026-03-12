WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT
        customer_id,
        COUNT(order_id)             AS total_orders,
        SUM(gmv)                    AS lifetime_value,
        MIN(order_purchased_at)     AS first_order_at,
        MAX(order_purchased_at)     AS last_order_at,
        AVG(review_score)           AS avg_review_score
    FROM {{ ref('fct_orders') }}
    WHERE order_status = 'delivered'
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.zip_code,
    c.city,
    c.state,

    -- Order metrics
    COALESCE(o.total_orders, 0)       AS total_orders,
    COALESCE(o.lifetime_value, 0)     AS lifetime_value,
    o.first_order_at,
    o.last_order_at,
    o.avg_review_score,

    -- RFM Segmentation
    DATEDIFF('day', o.last_order_at, CURRENT_DATE) AS recency_days,

    CASE
        WHEN o.total_orders >= 3                                        THEN 'Champion'
        WHEN o.total_orders = 2                                         THEN 'Loyal'
        WHEN DATEDIFF('day', o.last_order_at, CURRENT_DATE) <= 90      THEN 'Recent'
        WHEN DATEDIFF('day', o.last_order_at, CURRENT_DATE) <= 180     THEN 'At Risk'
        ELSE 'Churned'
    END AS customer_segment

FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id