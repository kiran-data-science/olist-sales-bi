SELECT
    order_month,
    order_status,
    delivery_status,
    COUNT(order_id)          AS total_orders,
    SUM(gmv)                 AS total_gmv,
    AVG(gmv)                 AS avg_order_value,
    SUM(total_freight)       AS total_freight,
    AVG(review_score)        AS avg_review_score,
    AVG(actual_delivery_days) AS avg_delivery_days,
    AVG(delivery_delay_days)  AS avg_delay_days,
    SUM(CASE WHEN delivery_status = 'Late' THEN 1 ELSE 0 END) AS late_orders
FROM {{ ref('fct_orders') }}
WHERE order_purchased_at IS NOT NULL
GROUP BY order_month, order_status, delivery_status
ORDER BY order_month