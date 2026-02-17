SELECT 
    DATE_TRUNC('month', o.order_date)::date as month,
    p.payment_method,
    COUNT(DISTINCT o.order_id) as paid_orders,
    SUM(p.amount) as total_revenue,
    ROUND(SUM(p.amount) * 1.0 / NULLIF(COUNT(DISTINCT o.order_id),0), 2) as avg_check
FROM {{ ref('stg_order_count_year') }} as o
JOIN {{ ref('stg_payments') }} as p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date), p.payment_method
ORDER BY month, total_revenue DESC
