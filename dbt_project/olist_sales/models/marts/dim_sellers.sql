WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

performance AS (
    SELECT
        i.seller_id,
        COUNT(DISTINCT i.order_id)   AS total_orders,
        SUM(i.price)                 AS total_revenue,
        AVG(r.review_score)          AS avg_review_score,
        SUM(CASE WHEN f.delivery_status = 'Late' THEN 1 ELSE 0 END) AS late_deliveries
    FROM {{ ref('stg_order_items') }} i
    LEFT JOIN {{ ref('fct_orders') }} f ON i.order_id = f.order_id
    LEFT JOIN {{ ref('stg_reviews') }} r ON i.order_id = r.order_id
    GROUP BY i.seller_id
)

SELECT
    s.seller_id,
    s.city,
    s.state,
    s.zip_code,
    COALESCE(p.total_orders, 0)     AS total_orders,
    COALESCE(p.total_revenue, 0)    AS total_revenue,
    p.avg_review_score,
    COALESCE(p.late_deliveries, 0)  AS late_deliveries,

    -- Seller tier
    CASE
        WHEN p.total_revenue >= 50000  THEN 'Platinum'
        WHEN p.total_revenue >= 20000  THEN 'Gold'
        WHEN p.total_revenue >= 5000   THEN 'Silver'
        ELSE 'Bronze'
    END AS seller_tier

FROM sellers s
LEFT JOIN performance p ON s.seller_id = p.seller_id