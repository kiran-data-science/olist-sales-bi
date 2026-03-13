WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

sales AS (
    SELECT
        i.product_id,
        COUNT(DISTINCT i.order_id)  AS total_orders,
        SUM(i.price)                AS total_revenue,
        AVG(r.review_score)         AS avg_review_score
    FROM {{ ref('stg_order_items') }} i
    LEFT JOIN {{ ref('stg_reviews') }} r ON i.order_id = r.order_id
    GROUP BY i.product_id
)

SELECT
    p.product_id,
    p.category_english,
    p.weight_g,
    p.photos_qty,
    COALESCE(s.total_orders, 0)   AS total_orders,
    COALESCE(s.total_revenue, 0)  AS total_revenue,
    s.avg_review_score
FROM products p
LEFT JOIN sales s ON p.product_id = s.product_id