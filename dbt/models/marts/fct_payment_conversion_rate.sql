WITH total_orders AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::date as month,
        COUNT(DISTINCT o.order_id) as total_orders
    FROM {{ ref('stg_order_count_year') }} as o
    GROUP BY DATE_TRUNC('month', o.order_date)
),
paid_orders AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::date as month,
        COUNT(DISTINCT o.order_id) as paid_orders
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_payments') }} as p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY DATE_TRUNC('month', o.order_date)
)
SELECT 
    t.month,
    t.total_orders,
    p.paid_orders,
    ROUND(CAST(p.paid_orders AS NUMERIC) / NULLIF(t.total_orders, 0) * 100, 2) as payment_conversion_rate
FROM total_orders as t
LEFT JOIN paid_orders as p ON t.month = p.month
ORDER BY t.month
